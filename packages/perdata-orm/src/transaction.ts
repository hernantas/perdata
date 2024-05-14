import { Knex } from 'knex'
import { Query } from './query'

export class Transaction extends Query {
  public constructor(protected readonly trx: Knex.Transaction) {
    super(trx.queryBuilder())
  }

  public async commit(): Promise<void> {
    await this.trx.commit()
  }

  public async rollback(): Promise<void> {
    await this.trx.rollback()
  }
}
