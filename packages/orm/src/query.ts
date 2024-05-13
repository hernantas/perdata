import { Knex } from 'knex'
import { AnyRecord, ObjectSchema, Schema, TypeOf } from 'pertype'
import { TableMetadata } from './metadata'

export abstract class Query<P extends AnyRecord<Schema>> {
  public constructor(
    protected readonly builder: Knex.QueryBuilder,
    protected readonly schema: ObjectSchema<P>,
  ) {}
}

export class QueryTable<P extends AnyRecord<Schema>> extends Query<P> {
  public select(): QuerySelect<P>
  public select<K extends keyof P>(...keys: K[]): QuerySelect<Pick<P, K>>
  public select<K extends keyof P>(
    ...keys: K[]
  ): QuerySelect<P> | QuerySelect<Pick<P, K>> {
    return select(this.builder, this.schema, ...keys)
  }

  public insert(value: Partial<TypeOf<P>>): QueryInsert<P> {
    return new QueryInsert(this.builder, this.schema, value)
  }
}

export abstract class QueryExecutable<P extends AnyRecord<Schema>>
  extends Query<P>
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

export class QuerySelect<
  P extends AnyRecord<Schema>,
> extends QueryExecutable<P> {
  public constructor(
    builder: Knex.QueryBuilder,
    schema: ObjectSchema<P>,
    private readonly condition?: QueryConditionGroup<P>,
    private readonly limitCount?: number,
  ) {
    super(builder, schema)
  }

  public async run(): Promise<TypeOf<P>[]> {
    const table = new TableMetadata(this.schema)

    let query = this.builder
      .from(table.name)
      .select(...table.columns.map((column) => column.name))

    if (this.condition !== undefined) {
      query = where(query, this.condition)
    }

    if (this.limitCount !== undefined) {
      query = query.limit(this.limitCount)
    }

    const result = await query
    return this.schema.array().decode(result)
  }

  public limit(count: number): QuerySelect<P> {
    return new QuerySelect(this.builder, this.schema, this.condition, count)
  }

  public where<K extends keyof P>(
    condition: QueryCondition<P, K> | QueryConditionGroup<P>,
  ): QuerySelect<P> {
    return new QuerySelect(
      this.builder,
      this.schema,
      and(condition),
      this.limitCount,
    )
  }
}

function select<P extends AnyRecord<Schema>, K extends keyof P>(
  builder: Knex.QueryBuilder,
  schema: ObjectSchema<P>,
  ...keys: K[]
): QuerySelect<P> | QuerySelect<Pick<P, K>> {
  return keys.length === 0
    ? new QuerySelect(builder, schema)
    : new QuerySelect(
        builder,
        schema
          .pick(...keys)
          .set('entity', schema.get('entity'))
          .set('table', schema.get('table')),
      )
}

function where<P extends AnyRecord<Schema>>(
  query: Knex.QueryBuilder,
  group: QueryConditionGroup<P>,
): Knex.QueryBuilder {
  if (group.operator === 'or') {
    for (const condition of group.conditions) {
      switch (condition.operator) {
        case 'and':
          query = query.orWhere((query) => where(query, condition))
          break
        case 'or':
          query = query.orWhere((query) => where(query, condition))
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
          query = query.andWhere((query) => where(query, condition))
          break
        case 'or':
          query = query.andWhere((query) => where(query, condition))
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

export interface QueryCondition<
  P extends AnyRecord<Schema>,
  K extends keyof P,
> {
  readonly operator: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte'
  readonly key: K
  readonly value: TypeOf<P[K]>
}

export function eq<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'eq', value }
}

export function ne<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'ne', value }
}

export function gt<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'gt', value }
}

export function gte<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'gte', value }
}

export function lt<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'lt', value }
}

export function lte<P extends AnyRecord<Schema>, K extends keyof P>(
  key: K,
  value: TypeOf<P[K]>,
): QueryCondition<P, K> {
  return { key, operator: 'lte', value }
}

export interface QueryConditionGroup<P extends AnyRecord<Schema>> {
  readonly operator: 'and' | 'or'
  readonly conditions: (QueryCondition<P, keyof P> | QueryConditionGroup<P>)[]
}

export function and<P extends AnyRecord<Schema>>(
  ...conditions: (QueryCondition<P, keyof P> | QueryConditionGroup<P>)[]
): QueryConditionGroup<P> {
  return { operator: 'and', conditions }
}

export function or<P extends AnyRecord<Schema>>(
  ...conditions: (QueryCondition<P, keyof P> | QueryConditionGroup<P>)[]
): QueryConditionGroup<P> {
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
    const result = await this.builder
      .from(table.name)
      .insert(this.schema.pick(...keys).encode(this.value as TypeOf<P>))
      .returning(table.columns.map((column) => column.name))
    return this.schema.array().decode(result)
  }
}
