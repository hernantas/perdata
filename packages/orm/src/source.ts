import { Knex, knex } from 'knex'
import { AnyRecord, AnySchema, ObjectSchema } from 'pertype'
import { QueryTable } from './query'

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

  /**
   * Get raw query builder from current data connection.
   *
   * @returns Raw query builder
   */
  public query(): Knex.QueryBuilder {
    return this.instance.queryBuilder()
  }

  public from<P extends AnyRecord<AnySchema>>(
    schema: ObjectSchema<P>,
  ): QueryTable<P> {
    return new QueryTable(this.query(), schema)
  }

  public async close(): Promise<void> {
    return this.instance.destroy()
  }
}
