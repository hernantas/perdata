import knex, { Knex } from 'knex'
import { AnyRecord, ObjectSchema, Schema } from 'pertype'
import { EntryRegistry } from './entry'
import { MetadataRegistry } from './metadata'
import { Query, QueryCollection } from './query'
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

  private readonly metadata: MetadataRegistry = new MetadataRegistry()

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
    return new Query(
      this.instance.queryBuilder(),
      this.metadata,
      new EntryRegistry(),
    )
  }

  public from<P extends AnyRecord<Schema>>(
    schema: ObjectSchema<P>,
  ): QueryCollection<P> {
    return new Query(
      this.instance.queryBuilder(),
      this.metadata,
      new EntryRegistry(),
    ).from(schema)
  }

  public transaction<T = void>(
    fn: (trx: Transaction) => Promise<T>,
  ): Promise<T> {
    return this.instance.transaction((trx) =>
      fn(new Transaction(trx, this.metadata, new EntryRegistry())),
    )
  }

  public async close(): Promise<void> {
    return this.instance.destroy()
  }
}
