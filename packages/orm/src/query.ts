import { Knex } from 'knex'
import { AnyRecord, AnySchema, ObjectSchema, TypeOf } from 'pertype'
import { TableMetadata } from './metadata'

export abstract class Query<P extends AnyRecord<AnySchema>> {
  public constructor(
    protected readonly builder: Knex.QueryBuilder,
    protected readonly schema: ObjectSchema<P>,
  ) {}
}

export class QueryTable<P extends AnyRecord<AnySchema>> extends Query<P> {
  public select(): QuerySelect<P>
  public select<K extends keyof P>(...keys: K[]): QuerySelect<Pick<P, K>>
  public select<K extends keyof P>(
    ...keys: K[]
  ): QuerySelect<P> | QuerySelect<Pick<P, K>> {
    return select(this.builder, this.schema, ...keys)
  }
}

export abstract class QueryExecutable<P extends AnyRecord<AnySchema>>
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
  P extends AnyRecord<AnySchema>,
> extends QueryExecutable<P> {
  public async run(): Promise<TypeOf<P>[]> {
    const table = new TableMetadata(this.schema)
    const rows: AnyRecord[] = await this.builder
      .from(table.name)
      .select(...table.columns.map((column) => column.name))
    return this.schema.array().decode(rows)
  }
}

function select<P extends AnyRecord<AnySchema>, K extends keyof P>(
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
