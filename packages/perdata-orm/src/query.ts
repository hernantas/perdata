import { Knex } from 'knex'
import { AnyRecord, ObjectSchema, Schema, TypeOf } from 'pertype'
import { TableMetadata } from './metadata'

export class Query {
  public constructor(protected readonly query: Knex.QueryBuilder) {}

  public from<P extends AnyRecord<Schema>>(
    schema: ObjectSchema<P>,
  ): QueryTable<P> {
    return new QueryTable(this.query, schema)
  }
}

export class QueryTable<P extends AnyRecord<Schema>> extends Query {
  public constructor(
    query: Knex.QueryBuilder,
    protected readonly schema: ObjectSchema<P>,
  ) {
    super(query)
  }

  public find<K extends keyof P>(
    condition?: QueryFilter<P, K> | QueryFilterGroup<P>,
  ): QueryFind<P> {
    return condition !== undefined
      ? new QueryFind(this.query, this.schema)
      : new QueryFind(this.query, this.schema, condition)
  }

  public insert(value: Partial<TypeOf<P>>): QueryInsert<P> {
    return new QueryInsert(this.query, this.schema, value)
  }

  public update(value: Partial<TypeOf<P>>): QueryUpdate<P> {
    return new QueryUpdate(this.query, this.schema, value)
  }
}

export abstract class QueryExecutable<P extends AnyRecord<Schema>>
  extends QueryTable<P>
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
    schema: ObjectSchema<P>,
    private readonly condition?: QueryFilterGroup<P> | undefined,
    private readonly limitCount?: number,
    private readonly offsetCount?: number,
  ) {
    super(query, schema)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    const table = new TableMetadata(this.schema)

    let query = this.query
      .from(table.name)
      .select(...table.columns.map((column) => column.name))

    if (this.condition !== undefined) {
      query = buildFilter(query, this.condition)
    }

    if (this.limitCount !== undefined) {
      query = query.limit(this.limitCount)
    }

    if (this.offsetCount !== undefined) {
      query = query.offset(this.offsetCount)
    }

    const result = await query
    return this.schema.array().decode(result)
  }

  public select<K extends keyof P>(...keys: K[]): QueryFind<Pick<P, K>> {
    return new QueryFind(
      this.query,
      this.schema
        .pick(...keys)
        .set('entity', this.schema.get('entity'))
        .set('table', this.schema.get('table')),
    )
  }

  public limit(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.schema,
      this.condition,
      count,
      this.offsetCount,
    )
  }

  public offset(count: number): QueryFind<P> {
    return new QueryFind(
      this.query,
      this.schema,
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
      this.schema,
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

export interface QueryFilterGroup<P extends AnyRecord<Schema>> {
  readonly operator: 'and' | 'or'
  readonly conditions: (QueryFilter<P, keyof P> | QueryFilterGroup<P>)[]
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
    builder: Knex.QueryBuilder,
    schema: ObjectSchema<P>,
    private readonly value: Partial<TypeOf<P>>,
  ) {
    super(builder, schema)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    const table = new TableMetadata(this.schema)
    const keys = Object.keys(this.value)
    const result = await this.query
      .from(table.name)
      .insert(this.schema.pick(...keys).encode(this.value as TypeOf<P>))
      .returning(table.columns.map((column) => column.name))
    return this.schema.array().decode(result)
  }
}

export class QueryUpdate<
  P extends AnyRecord<Schema>,
> extends QueryExecutable<P> {
  public constructor(
    builder: Knex.QueryBuilder,
    schema: ObjectSchema<P>,
    private readonly value: Partial<TypeOf<P>>,
    private readonly condition?: QueryFilterGroup<P>,
  ) {
    super(builder, schema)
  }

  public override async run(): Promise<TypeOf<P>[]> {
    const table = new TableMetadata(this.schema)
    const keys = Object.keys(this.value)
    let query = this.query
      .from(table.name)
      .update(this.schema.pick(...keys).encode(this.value as TypeOf<P>))
      .returning(table.columns.map((column) => column.name))

    if (this.condition !== undefined) {
      query = buildFilter(query, this.condition)
    }

    const result = await query
    return this.schema.array().decode(result)
  }

  public where<K extends keyof P>(
    condition: QueryFilter<P, K> | QueryFilterGroup<P>,
  ): QueryUpdate<P> {
    return new QueryUpdate(this.query, this.schema, this.value, and(condition))
  }
}
