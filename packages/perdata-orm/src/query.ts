import { Knex } from 'knex'
import { AnyRecord, ObjectSchema, Schema, TypeOf } from 'pertype'
import { EntryRegistry } from './entry'
import { TableMetadata } from './metadata'

export class Query {
  public constructor(
    protected readonly query: Knex.QueryBuilder,
    protected readonly entries: EntryRegistry,
  ) {}

  public from<P extends AnyRecord<Schema>>(
    schema: ObjectSchema<P>,
    metadata: TableMetadata = new TableMetadata(schema),
  ): QueryCollection<P> {
    return new QueryCollection(this.query, this.entries, schema, metadata)
  }
}

export class QueryCollection<P extends AnyRecord<Schema>> extends Query {
  public constructor(
    query: Knex.QueryBuilder,
    entries: EntryRegistry,
    protected readonly schema: ObjectSchema<P>,
    protected readonly metadata: TableMetadata,
  ) {
    super(query, entries)
  }

  public find<K extends keyof P>(
    condition?:
      | QueryFilter<P, K>
      | QueryFilterMultiple<P, K>
      | QueryFilterGroup<P>,
  ): QueryFind<P> {
    return condition !== undefined
      ? new QueryFind(this.query, this.entries, this.schema, this.metadata)
      : new QueryFind(
          this.query,
          this.entries,
          this.schema,
          this.metadata,
          condition,
        )
  }

  public insert(...values: TypeOf<P>[]): QueryInsert<P> {
    return new QueryInsert(
      this.query,
      this.entries,
      this.schema,
      this.metadata,

      values,
    )
  }

  public save(value: Partial<TypeOf<P>>): QuerySave<P> {
    return new QuerySave(
      this.query,
      this.entries,
      this.schema,
      this.metadata,

      value,
    )
  }
}

export abstract class QueryExecutable<P extends AnyRecord<Schema>>
  extends QueryCollection<P>
  implements PromiseLike<TypeOf<P>[]>
{
  public abstract run(): Promise<TypeOf<P>[]>

  public then<R = TypeOf<P>, RE = never>(
    onfulfilled?: (value: TypeOf<P>[]) => R | PromiseLike<R>,
    onrejected?: (reason: any) => RE | PromiseLike<RE>,
  ): PromiseLike<R | RE> {
    return this.run().then(onfulfilled, onrejected)
  }
}

export class QueryFind<P extends AnyRecord<Schema>> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    metadata: TableMetadata,
    private readonly condition?: QueryFilterGroup<P> | undefined,
    private readonly limitCount?: number,
    private readonly offsetCount?: number,
  ) {
    super(query, entries, schema, metadata)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    let query = this.query
      .clone()
      .from(this.metadata.name)
      .select(...this.metadata.baseColumns.map((column) => column.name))

    if (this.condition !== undefined) {
      query = buildFilter(query, this.condition)
    }

    if (this.limitCount !== undefined) {
      query = query.limit(this.limitCount)
    }

    if (this.offsetCount !== undefined) {
      query = query.offset(this.offsetCount)
    }

    const entries = this.metadata.baseSchema
      .array()
      .decode(await query)
      .flatMap((row) =>
        this.entries
          .findById(this.metadata, row[this.metadata.id.name])
          .map((entry) => {
            entry.value = row
            return entry
          }),
      )

    // resolve relations
    await Promise.all(
      this.metadata.relationColumns.map(async (column) => {
        const lookups = entries.map(
          (entry) => entry.property(column.sourceColumn)!.raw,
        )
        await this.from(
          column.foreignTable.origin as ObjectSchema<AnyRecord<Schema>>,
          column.foreignTable,
        ).find(includes(column.foreignColumn.name, lookups))
      }),
    )

    return this.schema.array().decode(entries.map((entry) => entry.value))
  }

  public limit(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.entries,
      this.schema,
      this.metadata,
      this.condition,
      count,
      this.offsetCount,
    )
  }

  public offset(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.entries,
      this.schema,
      this.metadata,
      this.condition,
      this.limitCount,
      count,
    )
  }

  public filter<K extends keyof P>(
    condition: QueryFilter<P, K> | QueryFilterGroup<P>,
  ): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.entries,
      this.schema,
      this.metadata,
      and(condition),
      this.limitCount,
      this.offsetCount,
    )
  }
}

function buildFilter<P extends AnyRecord<Schema>>(
  query: Knex.QueryBuilder,
  group: QueryFilterGroup<P>,
): Knex.QueryBuilder {
  if (group.operator === 'or') {
    for (const condition of group.conditions) {
      switch (condition.operator) {
        case 'and':
          query = query.orWhere((query) => buildFilter(query, condition))
          break
        case 'or':
          query = query.orWhere((query) => buildFilter(query, condition))
          break
        case 'eq':
          query = query.orWhere(condition.key, '=', condition.value)
          break
        case 'ne':
          query = query.orWhere(condition.key, '<>', condition.value)
          break
        case 'gt':
          query = query.orWhere(condition.key, '>', condition.value)
          break
        case 'gte':
          query = query.orWhere(condition.key, '>=', condition.value)
          break
        case 'lt':
          query = query.orWhere(condition.key, '<', condition.value)
          break
        case 'lte':
          query = query.orWhere(condition.key, '<=', condition.value)
          break
        case 'in':
          query = query.orWhereIn(condition.key, condition.values)
          break
      }
    }
  } else if (group.operator === 'and') {
    for (const condition of group.conditions) {
      switch (condition.operator) {
        case 'and':
          query = query.andWhere((query) => buildFilter(query, condition))
          break
        case 'or':
          query = query.andWhere((query) => buildFilter(query, condition))
          break
        case 'eq':
          query = query.andWhere(condition.key, '=', condition.value)
          break
        case 'ne':
          query = query.andWhere(condition.key, '<>', condition.value)
          break
        case 'gt':
          query = query.andWhere(condition.key, '>', condition.value)
          break
        case 'gte':
          query = query.andWhere(condition.key, '>=', condition.value)
          break
        case 'lt':
          query = query.andWhere(condition.key, '<', condition.value)
          break
        case 'lte':
          query = query.andWhere(condition.key, '<=', condition.value)
          break
        case 'in':
          query = query.whereIn(condition.key, condition.values)
          break
      }
    }
  }

  return query
}

export interface QueryFilter<P extends AnyRecord<Schema>, K extends keyof P> {
  readonly operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte'
  readonly key: K
  readonly value: TypeOf<P[K]>
}

export function eq<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'eq', value }
}

export function ne<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'ne', value }
}

export function gt<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'gt', value }
}

export function gte<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'gte', value }
}

export function lt<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'lt', value }
}

export function lte<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryFilter<P, K> {
  return { key, operator: 'lte', value }
}

export interface QueryFilterMultiple<
  P extends AnyRecord<Schema>,
  K extends keyof P,
> {
  readonly operator: 'in'
  readonly key: K
  readonly values: TypeOf<P[K]>[]
}

export function includes<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  values: TypeOf<P[K]>[],
): QueryFilterMultiple<P, K> {
  return { key, operator: 'in', values }
}

export interface QueryFilterGroup<P extends AnyRecord<Schema>> {
  readonly operator: 'and' | 'or'
  readonly conditions: (
    | QueryFilter<P, keyof P>
    | QueryFilterMultiple<P, keyof P>
    | QueryFilterGroup<P>
  )[]
}

export function and<P extends AnyRecord<Schema>>(
  ...conditions: (QueryFilter<P, keyof P> | QueryFilterGroup<P>)[]
): QueryFilterGroup<P> {
  return { operator: 'and', conditions }
}

export function or<P extends AnyRecord<Schema>>(
  ...conditions: (QueryFilter<P, keyof P> | QueryFilterGroup<P>)[]
): QueryFilterGroup<P> {
  return { operator: 'or', conditions }
}

export class QueryInsert<
  P extends AnyRecord<Schema>,
> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    metadata: TableMetadata,
    private readonly values: TypeOf<P>[],
  ) {
    super(query, entries, schema, metadata)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    const query = this.query
      .clone()
      .from(this.metadata.name)
      .insert(this.schema.array().encode(this.values))
      .returning(this.metadata.id.name)
    const ids = this.metadata.id.origin
      .array()
      .encode(this.metadata.id.origin.array().decode(await query))
    return await this.from(this.schema).find(
      includes(this.metadata.id.name, ids),
    )
  }

  public override insert(...values: TypeOf<P>[]): QueryInsert<P> {
    return new QueryInsert(
      this.query,
      this.entries,
      this.schema,
      this.metadata,
      this.values.concat(...values),
    )
  }
}

export class QuerySave<P extends AnyRecord<Schema>> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    metadata: TableMetadata,
    private readonly value: Partial<TypeOf<P>>,
  ) {
    super(query, entries, schema, metadata)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    if (!Object.hasOwn(this.value, this.metadata.id.name)) {
      throw new Error(`Must specify "${this.metadata.id.name}" id property`)
    }

    const id = this.value[this.metadata.id.name]
    const keys = this.metadata.baseColumns
      .filter((col) => !col.id)
      .filter((col) => Object.hasOwn(this.value, col.name))
      .map((col) => col.name)
    const ids = await this.query
      .clone()
      .from(this.metadata.name)
      .update(this.schema.pick(...keys).encode(this.value as TypeOf<P>))
      .where(this.metadata.id.name, id)
      .returning(this.metadata.id.name)

    return await this.from(this.schema).find(
      includes(this.metadata.id.name, ids),
    )
  }
}
