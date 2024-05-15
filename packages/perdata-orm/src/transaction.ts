import { Knex } from 'knex'
import { EntryRegistry } from './entry'
import { Query } from './query'

export class Transaction extends Query {
  public constructor(
    protected readonly trx: Knex.Transaction,
    registry: EntryRegistry,
  ) {
    super(trx.queryBuilder(), registry)
  }

  public async commit(): Promise<void> {
    await this.trx.commit()
  }

  public async rollback(): Promise<void> {
    await this.trx.rollback()
  }
}
