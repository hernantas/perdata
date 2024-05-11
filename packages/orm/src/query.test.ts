import { number, object, string } from 'pertype'
import { DataSource } from './source'
import { getEnvironment } from './util/environment'

describe('Query', () => {
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
    id: number().set('id', true).set('generated', true),
    key: string(),
    value: string().optional(),
  })

  describe('Select', () => {
    const tableName = 'simple_select'

    beforeAll(async () => {
      await db.query().from(tableName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .query()
            .from(tableName)
            .insert({ key: `key-${number}`, value: `value-${number}` }),
        ),
      )
    })

    it('Should be able to select all from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).select()
      expect(result).toHaveLength(10)
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).toHaveProperty('key')
      expect(result[0]).toHaveProperty('value')
    })

    it('Should be able to select all from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).select('id')
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).not.toHaveProperty('key')
      expect(result[0]).not.toHaveProperty('value')
    })
  })

  afterAll(() => db.close())
})
