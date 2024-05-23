import { Knex } from 'knex'
import {
  AnyRecord,
  ObjectSchema,
  OptionalOf,
  Schema,
  TypeOf,
  string,
} from 'pertype'
import { Entry, EntryRegistry } from './entry'
import { MetadataRegistry } from './metadata'

export class Query {
  public constructor(
    protected readonly query: Knex.QueryBuilder,
    protected readonly metadata: MetadataRegistry,
    protected readonly entries: EntryRegistry,
  ) {}

  public from<P extends AnyRecord<Schema>>(
    schema: ObjectSchema<P>,
  ): QueryCollection<P> {
    return new QueryCollection(this.query, this.metadata, this.entries, schema)
  }
}

export class QueryCollection<P extends AnyRecord<Schema>> extends Query {
  public constructor(
    query: Knex.QueryBuilder,
    metadata: MetadataRegistry,
    entries: EntryRegistry,
    protected readonly schema: ObjectSchema<P>,
  ) {
    super(query, metadata, entries)
  }

  public find<K extends keyof P>(
    condition?:
      | QueryFilter<P, K>
      | QueryFilterMultiple<P, K>
      | QueryFilterGroup<P>,
  ): QueryFind<P> {
    return condition === undefined
      ? new QueryFind(this.query, this.metadata, this.entries, this.schema)
      : new QueryFind(
          this.query,
          this.metadata,
          this.entries,
          this.schema,
          and(condition),
        )
  }

  public insert(value: OptionalOf<TypeOf<P>>): QueryInsert<P> {
    return new QueryInsert(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      value,
    )
  }

  public save(value: Partial<OptionalOf<TypeOf<P>>>): QuerySave<P> {
    return new QuerySave(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      value,
    )
  }
}

export abstract class QueryExecutable<P extends AnyRecord<Schema>>
  extends QueryCollection<P>
  implements PromiseLike<OptionalOf<TypeOf<P>>[]>
{
  public abstract run(): Promise<OptionalOf<TypeOf<P>>[]>

  public then<R = OptionalOf<TypeOf<P>>, RE = never>(
    onfulfilled?: (value: OptionalOf<TypeOf<P>>[]) => R | PromiseLike<R>,
    onrejected?: (reason: any) => RE | PromiseLike<RE>,
  ): PromiseLike<R | RE> {
    return this.run().then(onfulfilled, onrejected)
  }
}

export class QueryFind<P extends AnyRecord<Schema>> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    metadata: MetadataRegistry,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    private readonly condition?: QueryFilterGroup<P> | undefined,
    private readonly limitCount?: number,
    private readonly offsetCount?: number,
    private readonly orderOptions?: QueryOrder<P>[] | undefined,
  ) {
    super(query, metadata, entries, schema)
  }

  public override async run(): Promise<OptionalOf<TypeOf<P>>[]> {
    const table = this.metadata.get(this.schema)
    let query = this.query
      .clone()
      .from(table.name)
      .select(...table.baseColumns.map((column) => column.name))

    if (this.condition !== undefined) {
      query = buildFilter(query, this.condition)
    }

    if (this.limitCount !== undefined) {
      query = query.limit(this.limitCount)
    }

    if (this.offsetCount !== undefined) {
      query = query.offset(this.offsetCount)
    }

    if (this.orderOptions !== undefined) {
      query = this.orderOptions.reduce(
        (query, opts) => query.orderBy(string().decode(opts.key), opts.order),
        query,
      )
    }

    const entries = table.baseSchema
      .array()
      .decode(await query)
      .map((row) => {
        const entry = this.entries.findById(table, row[table.id.name])
        entry.value = row
        entry.dirty = false
        entry.initialized = true
        return entry
      })

    // resolve relations
    await Promise.all(
      table.relationColumns.map(async (column) => {
        const lookups = entries.map(
          (entry) => entry.property(column.sourceColumn).raw,
        )
        const foreignValues = await this.from(column.foreignTable.schema).find(
          includes(column.foreignColumn.name, lookups),
        )
        entries.forEach((entry) => {
          const matchedEntries = foreignValues.filter(
            (foreignValue) =>
              foreignValue[column.foreignColumn.name] ===
              entry.property(column.sourceColumn).value,
          )
          entry.property(column).value = column.collection
            ? matchedEntries
            : matchedEntries[0]
        })
      }),
    )

    return this.schema
      .array()
      .decode(entries.map((entry) => entry.value)) as OptionalOf<TypeOf<P>>[]
  }

  public limit(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      this.condition,
      count,
      this.offsetCount,
      this.orderOptions,
    )
  }

  public offset(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      this.condition,
      this.limitCount,
      count,
      this.orderOptions,
    )
  }

  public filter<K extends keyof P>(
    condition: QueryFilter<P, K> | QueryFilterGroup<P>,
  ): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      and(condition),
      this.limitCount,
      this.offsetCount,
      this.orderOptions,
    )
  }

  public orderBy<K extends keyof P>(
    key: K,
    order: 'asc' | 'desc' = 'asc',
  ): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.metadata,
      this.entries,
      this.schema,
      this.condition,
      this.limitCount,
      this.offsetCount,
      (this.orderOptions ?? []).concat({ key, order }),
    )
  }
}

export interface QueryOrder<P extends AnyRecord<Schema>> {
  readonly key: keyof P
  readonly order: 'asc' | 'desc'
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
  ...conditions: (
    | QueryFilter<P, keyof P>
    | QueryFilterMultiple<P, keyof P>
    | QueryFilterGroup<P>
  )[]
): QueryFilterGroup<P> {
  return { operator: 'and', conditions }
}

export function or<P extends AnyRecord<Schema>>(
  ...conditions: (
    | QueryFilter<P, keyof P>
    | QueryFilterMultiple<P, keyof P>
    | QueryFilterGroup<P>
  )[]
): QueryFilterGroup<P> {
  return { operator: 'or', conditions }
}

export class QueryInsert<
  P extends AnyRecord<Schema>,
> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    metadata: MetadataRegistry,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    private readonly value: OptionalOf<TypeOf<P>>,
  ) {
    super(query, metadata, entries, schema)
  }

  public override async run(): Promise<OptionalOf<TypeOf<P>>[]> {
    const table = this.metadata.get(this.schema)
    const entry = this.entries.create(table)
    entry.value = this.value
    entry.id.value = undefined
    await flush(this.query, this.entries, entry)
    return this.schema.array().decode(entry.value)
  }
}

export class QuerySave<P extends AnyRecord<Schema>> extends QueryExecutable<P> {
  public constructor(
    query: Knex.QueryBuilder,
    metadata: MetadataRegistry,
    entries: EntryRegistry,
    schema: ObjectSchema<P>,
    private readonly value: Partial<TypeOf<P>>,
  ) {
    super(query, metadata, entries, schema)
  }

  public override async run(): Promise<OptionalOf<TypeOf<P>>[]> {
    const table = this.metadata.get(this.schema)
    const id = this.value[table.id.name]
    const entry =
      id !== undefined
        ? this.entries.findById(table, id)
        : this.entries.create(table)
    entry.value = this.value
    await flush(this.query, this.entries, entry)
    return this.schema.array().decode(entry.value)
  }
}

async function flush(
  connection: Knex.QueryBuilder,
  entries: EntryRegistry,
  entry: Entry,
): Promise<Entry> {
  await flushBase(connection, entry)

  entry.sync()

  // flush relations
  await Promise.all(
    entry.table.relationColumns
      .flatMap((column) => entries.get(column.foreignTable))
      .map((foreignEntry) => flush(connection, entries, foreignEntry)),
  )

  entry.sync()

  // flush entry once more if anything changes
  await flushBase(connection, entry)
  return entry
}

async function flushBase(
  connection: Knex.QueryBuilder,
  entry: Entry,
): Promise<Entry> {
  if (entry.id.value === undefined) {
    // insert
    const query = connection
      .clone()
      .from(entry.table.name)
      .insert(entry.table.baseSchema.encode(entry.value))
      .returning(entry.table.baseColumns.map((column) => column.name))
    entry.table.baseSchema
      .array()
      .decode(await query)
      .forEach((row) => {
        entry.value = row
        entry.dirty = false
        entry.initialized = true
      })
  } else {
    // update
    const changes = entry.properties
      .filter(
        (prop) =>
          prop.active &&
          prop.dirty &&
          !prop.column.id &&
          !prop.column.generated,
      )
      .map((prop) => [prop.column.name, prop.raw])
    if (changes.length > 0) {
      const query = connection
        .clone()
        .from(entry.table.name)
        .update(Object.fromEntries(changes))
        .where(entry.table.id.name, entry.id.raw as string)
        .returning(entry.table.baseColumns.map((column) => column.name))
      entry.table.baseSchema
        .array()
        .decode(await query)
        .forEach((row) => {
          entry.value = row
          entry.dirty = false
          entry.initialized = true
        })
    }
  }
  return entry
}
