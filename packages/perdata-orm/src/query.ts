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
import { MetadataRegistry, TableMetadata } from './metadata'
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

  public save(value: OptionalOf<TypeOf<P>>): QuerySave<P> {
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
    const encoded = this.schema.encode(this.value)
    const raw = createRaw(table, encoded)!
    const entry = this.entries.instantiate(table, raw)
    await loadAll(this.query, this.entries, table)
    entry.value = raw
    await commit(this.query, this.entries, table)
    return this.schema.array().decode(entry.value)
  }
}

export class QuerySave<P extends AnyRecord<Schema>> extends QueryExecutable<P> {
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
    const encoded = this.schema.encode(this.value)
    const raw = createRaw(table, encoded)!
    const entry = this.entries.instantiate(table, raw)
    await loadAll(this.query, this.entries, table)
    entry.value = raw
    await commit(this.query, this.entries, table)
    return this.schema.array().decode(entry.value)
  }
}

async function loadAll(
  connection: Knex.QueryBuilder,
  registry: EntryRegistry,
  table: TableMetadata,
): Promise<void> {
  await Promise.all([
    ...table.relationColumns
      .map((column) => column.foreignTable)
      .map((foreignTable) => load(connection, registry, foreignTable)),
    load(connection, registry, table),
  ])
}

async function load(
  connection: Knex.QueryBuilder,
  registry: EntryRegistry,
  table: TableMetadata,
): Promise<void> {
  const ids = registry
    .findAll(table)
    .filter((entry) => !entry.initialized)
    .map((entry) => entry.id.value)
    .filter((raw) => raw !== undefined)
  if (ids.length > 0) {
    const columnNames = table.baseColumns.map((column) => column.name)
    const query = connection
      .clone()
      .from(table.name)
      .select(columnNames)
      .whereIn(table.id.name, ids)
    const rows: unknown[] = await query
    rows
      .map((row) => createRaw(table, row))
      .map((raw) => registry.instantiate(table, raw))
      .filter((entry) => entry !== undefined)
      .forEach((entry) => {
        entry.dirty = false
        entry.initialized = true
      })
  }
}

async function commit(
  connection: Knex.QueryBuilder,
  registry: EntryRegistry,
  table: TableMetadata,
): Promise<void> {
  registry
    .findAll(table)
    .filter((entry) => entry.remove)
    .forEach((entry) =>
      entry.relationProperties.forEach((prop) => (prop.value = undefined)),
    )
  await commitSave(connection, registry, table)
  await commitRemove(connection, registry, table)
}

async function commitSave(
  connection: Knex.QueryBuilder,
  registry: EntryRegistry,
  table: TableMetadata,
): Promise<void> {
  const entries = registry.findAll(table)

  // commit dependencies
  await Promise.all(
    table.relationColumns
      .filter((column) => column.owner === 'source')
      .map((column) => column.foreignTable)
      .map((foreignTable) => commitSave(connection, registry, foreignTable)),
  )
  entries.forEach((entry) => entry.bind())

  // commit current entry
  await Promise.all(
    entries
      .filter((entry) => entry.dirty && !entry.remove)
      .map((entry) =>
        entry.initialized
          ? commitUpdateOne(connection, entry)
          : commitInsertOne(connection, entry),
      ),
  )
  entries.forEach((entry) => entry.bind())

  // commit dependents
  await Promise.all(
    table.relationColumns
      .filter((column) => column.owner === 'foreign')
      .map((column) => column.foreignTable)
      .map((foreignTable) => commitSave(connection, registry, foreignTable)),
  )
}

async function commitUpdateOne(
  connection: Knex.QueryBuilder,
  entry: Entry,
): Promise<void> {
  const changes = entry.baseProperties
    .filter((prop) => prop.dirty && !prop.column.id && !prop.column.generated)
    .map((prop) => [prop.column.name, prop.changes] as const)
  const updateMap = Object.fromEntries(changes)
  const columnNames = entry.table.baseColumns.map((column) => column.name)
  const query = connection
    .clone()
    .from(entry.table.name)
    .update(updateMap)
    .where(entry.id.column.name, entry.id.value)
    .limit(1)
    .returning(columnNames)
  const rows = await query
  rows
    .map((row) => createRaw(entry.table, row))
    .filter((raw) => raw !== undefined)
    .forEach((raw) => (entry.value = raw))
  entry.dirty = false
  entry.initialized = true
}

async function commitInsertOne(
  connection: Knex.QueryBuilder,
  entry: Entry,
): Promise<void> {
  const changes = entry.baseProperties
    .filter((prop) => prop.dirty && !prop.column.generated)
    .map((prop) => [prop.column.name, prop.changes] as const)
  const insertMap = Object.fromEntries(changes)
  const columnNames = entry.table.baseColumns.map((column) => column.name)
  const query = connection
    .clone()
    .from(entry.table.name)
    .insert(insertMap)
    .returning(columnNames)
  const rows = await query
  rows
    .map((row) => createRaw(entry.table, row))
    .filter((raw) => raw !== undefined)
    .forEach((raw) => (entry.value = raw))
  entry.dirty = false
  entry.initialized = true
}

async function commitRemove(
  connection: Knex.QueryBuilder,
  registry: EntryRegistry,
  table: TableMetadata,
): Promise<void> {
  // commit dependents
  await Promise.all(
    table.relationColumns
      .filter((column) => column.owner === 'foreign')
      .map((column) => column.foreignTable)
      .map((foreignTable) => commitRemove(connection, registry, foreignTable)),
  )

  await Promise.all(
    registry
      .findAll(table)
      .filter((entry) => entry.remove)
      .map((entry) => commitDeleteOne(connection, entry)),
  )

  // commit dependencies
  await Promise.all(
    table.relationColumns
      .filter((column) => column.owner === 'source')
      .map((column) => column.foreignTable)
      .map((foreignTable) => commitRemove(connection, registry, foreignTable)),
  )
}

async function commitDeleteOne(
  connection: Knex.QueryBuilder,
  entry: Entry,
): Promise<void> {
  const columnNames = entry.table.baseColumns.map((column) => column.name)
  const query = connection
    .clone()
    .from(entry.table.name)
    .delete()
    .where(entry.id.column.name, entry.id.value)
    .limit(1)
    .returning(columnNames)
  const rows = await query
  rows
    .map((row) => createRaw(entry.table, row))
    .filter((raw) => raw !== undefined)
    .forEach((raw) => (entry.value = raw))
  entry.remove = false
  entry.dirty = false
  entry.initialized = false
}
