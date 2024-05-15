import { Knex } from 'knex'
import { EntryRegistry } from './entry'
import { MetadataRegistry } from './metadata'
import { Query } from './query'

export class Transaction extends Query {
  public constructor(
    protected readonly trx: Knex.Transaction,
    metadata: MetadataRegistry,
    registry: EntryRegistry,
  ) {
    super(trx.queryBuilder(), metadata, registry)
  }

  public async commit(): Promise<void> {
    await this.trx.commit()
  }

  public async rollback(): Promise<void> {
    await this.trx.rollback()
  }
}
