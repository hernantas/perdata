import { Knex, knex } from 'knex'
import { AnyRecord, AnySchema, ObjectSchema } from 'pertype'
import { Query, QueryTable } from './query'
import { Transaction } from './transaction'

export interface DataSourceConfig {
  client: 'pg'
  host: string
  port: number
  user: string
  password: string
  database: string
}

/** Maintain connection with the data sources. */
export class DataSource {
  private readonly instance: Knex

  public constructor({
    client,
    host,
    port,
    user,
    password,
    database,
  }: DataSourceConfig) {
    this.instance = knex({
      client,
      connection: {
        host,
        port,
        user,
        password,
        database,
      },
    })
  }

  public connection(): Knex {
    return this.instance
  }

  public query(): Query {
    return new Query(this.instance.queryBuilder())
  }

  public from<P extends AnyRecord<AnySchema>>(
    schema: ObjectSchema<P>,
  ): QueryTable<P> {
    return new Query(this.instance.queryBuilder()).from(schema)
  }

  public transaction<T = void>(
    fn: (trx: Transaction) => Promise<T>,
  ): Promise<T> {
    return this.instance.transaction((trx) => fn(new Transaction(trx)))
  }

  public async close(): Promise<void> {
    return this.instance.destroy()
  }
}
