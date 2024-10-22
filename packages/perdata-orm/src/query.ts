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
import { createRaw } from './util/raw'

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

  public async execute(): Promise<Entry[]> {
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

    const result = await query
    const decoded = table.baseSchema.array().decode(result)
    const encoded = table.baseSchema.array().encode(decoded)
    const entries = encoded
      .map((item) => createRaw(table, item))
      .map((raw) => this.entries.instantiate(table, raw))
      .filter((entry) => entry !== undefined)
    entries.forEach((entry) => {
      entry.dirty = false
      entry.initialized = true
    })

    // resolve relations
    await Promise.all(
      table.relationColumns.map(async (column) => {
        const lookups = entries
          .map((entry) => entry.property(column.sourceColumn)?.value)
          .filter((value) => value !== undefined)
        const foreignEntries = await this.from(column.foreignTable.schema)
          .find(includes(column.foreignColumn.name, lookups))
          .execute()
        entries.forEach((entry) => {
          const matchedValues = foreignEntries
            .filter(
              (foreignEntry) =>
                foreignEntry.property(column.foreignColumn)?.value ===
                entry.property(column.sourceColumn)?.value,
            )
            .map((entry) => entry.value)
          entry.property(column)!.value = column.collection
            ? matchedValues
            : matchedValues[0]
        })
      }),
    )

    return entries
  }

  public override async run(): Promise<OptionalOf<TypeOf<P>>[]> {
    const entries = await this.execute()
    return this.schema.array().decode(entries.map((entry) => entry.value))
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
    const entry = this.entries.instantiate(table, this.value)
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

  entry.bind()

  // flush relations
  await Promise.all(
    entry.table.relationColumns
      .flatMap((column) => entries.findAll(column.foreignTable))
      .map((foreignEntry) => flush(connection, entries, foreignEntry)),
  )

  entry.bind()

  // flush entry once more if anything changes
  await flushBase(connection, entry)
  return entry
}

async function flushBase(
  connection: Knex.QueryBuilder,
  entry: Entry,
): Promise<Entry> {
  const table = entry.table
  const changeList = entry.baseProperties
    .filter((prop) => prop.dirty && !prop.column.id && !prop.column.generated)
    .map((prop) => [prop.column.name, prop.value ?? null] as const)
  if (changeList.length > 0) {
    const changes = Object.fromEntries(changeList)
    const columns = table.baseColumns.map((column) => column.name)
    const query =
      entry.id.value !== undefined
        ? connection
            .clone()
            .from(table.name)
            .update(changes)
            .where(table.id.name, entry.id.value)
            .returning(columns)
        : connection
            .clone()
            .from(entry.table.name)
            .insert(changes)
            .returning(columns)
    const rows = await query
    rows
      .map((row) => createRaw(table, row))
      .filter((raw) => raw !== undefined)
      .forEach((raw) => (entry.value = raw))
    entry.dirty = false
    entry.initialized = true
  }
  return entry
}
