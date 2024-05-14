import { object, number, string } from 'pertype'
import { DataSource } from './source'
import { getEnvironment } from './util/environment'

describe('Transaction', () => {
  const { CLIENT, HOST, PORT, USER, PASSWORD, DATABASE } = getEnvironment()
  const db = new DataSource({
    client: CLIENT,
    host: HOST,
    port: PORT,
    user: USER,
    password: PASSWORD,
    database: DATABASE,
  })
  const base = object({
    id: number().optional().set('id', true).set('generated', true),
    key: string(),
    value: string().optional(),
  })

  it('Should perform transaction commit', async () => {
    const tableName = 'transaction_commit'
    const schema = base.set('table', tableName)
    await db.connection().from(tableName).truncate()
    await db.transaction(async (db) => {
      await db.from(schema).insert({ key: 'key-1', value: 'value-1' })
      await db.commit()
    })

    const result = await db.from(schema).find()
    expect(result).toHaveLength(1)
    expect(result[0]).toHaveProperty('id')
    expect(result[0]).toHaveProperty('key', 'key-1')
    expect(result[0]).toHaveProperty('value', 'value-1')
  })

  it('Should perform transaction rollback', async () => {
    const tableName = 'transaction_rollback'
    const schema = base.set('table', tableName)
    await db.connection().from(tableName).truncate()
    await db.transaction(async (db) => {
      await db.from(schema).insert({ key: 'key-1', value: 'value-1' })
      await db.rollback()
    })

    const result = await db.from(schema).find()
    expect(result).toHaveLength(0)
  })

  afterAll(() => db.close())
})
