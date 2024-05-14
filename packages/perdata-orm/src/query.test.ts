import { number, object, string } from 'pertype'
import { eq, or } from './query'
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
    id: number().optional().set('id', true).set('generated', true),
    key: string(),
    value: string().optional(),
  })

  describe('Select', () => {
    const tableName = 'simple_select'

    beforeAll(async () => {
      await db.connection().from(tableName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .connection()
            .from(tableName)
            .insert({ key: `key-${number}`, value: `value-${number}` }),
        ),
      )
    })

    it('Should be able to select all from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find()
      expect(result).toHaveLength(10)
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).toHaveProperty('key')
      expect(result[0]).toHaveProperty('value')
    })

    it('Should be able to select all from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find().select('id')
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).not.toHaveProperty('key')
      expect(result[0]).not.toHaveProperty('value')
    })

    it('Should be able to select with limit from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find().limit(1)
      expect(result).toHaveLength(1)
    })

    it('Should be able to select with offset from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find().limit(1).offset(2)
      expect(result).toHaveLength(1)
    })

    it('Should be able to select with filter from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find().filter(eq('id', 1))
      expect(result).toHaveLength(1)
    })

    it('Should be able to select with filter using "or" from db table', async () => {
      const schema = base.set('table', tableName)
      const query = db
        .from(schema)
        .find()
        .filter(or(eq('id', 1), eq('id', 2)))
      const result = await query
      expect(result).toHaveLength(2)
    })
  })

  it('Should be able to insert elements', async () => {
    const tableName = 'simple_insert'
    await db.connection().from(tableName).truncate()

    const schema = base.set('table', tableName)
    await db.from(schema).insert({ id: undefined, key: 'key', value: 'value' })

    const result = await db.from(schema).find()
    expect(result).toHaveLength(1)
    expect(result[0]).toHaveProperty('id')
    expect(result[0]).toHaveProperty('key', 'key')
    expect(result[0]).toHaveProperty('value', 'value')
  })

  it('Should be able to update elements', async () => {
    const tableName = 'simple_save'
    await db.connection().from(tableName).truncate()

    const schema = base.set('table', tableName)
    await db.from(schema).insert({ id: undefined, key: 'key', value: 'value' })

    await db.from(schema).save({ id: 1, key: 'key-u', value: 'value-u' })

    const result = await db.from(schema).find()
    expect(result).toHaveLength(1)
    expect(result[0]).toHaveProperty('id')
    expect(result[0]).toHaveProperty('key', 'key-u')
    expect(result[0]).toHaveProperty('value', 'value-u')
  })

  describe('Relation', () => {
    it('Should also resolve one-to-one relations', async () => {
      const foreignName = 'relation_one_to_one_source_2'
      await db.connection().from(foreignName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .connection()
            .from(foreignName)
            .insert({ key: `key-${number}`, value: `value-${number}` }),
        ),
      )

      const sourceName = 'relation_one_to_one_source_1'
      await db.connection().from(sourceName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .connection()
            .from(sourceName)
            .insert({
              key: `key-${number}`,
              value: `value-${number}`,
              relation_one_to_one_source_2_id: number + 1,
            }),
        ),
      )

      const schemaForeign = base.set('table', foreignName)
      const schemaSource = object({
        ...base.properties,
        relation: schemaForeign,
      }).set('table', sourceName)

      const result = await db.from(schemaSource).find()
      expect(result).toHaveLength(10)
    })

    it('Should also resolve one-to-many relations', async () => {
      const sourceName = 'relation_one_to_many_1'
      await db.connection().from(sourceName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .connection()
            .from(sourceName)
            .insert({ key: `key-${number}`, value: `value-${number}` }),
        ),
      )

      const foreignName = 'relation_one_to_many_2'
      await db.connection().from(foreignName).truncate()
      await Promise.all(
        [...Array(10).keys()].map((number) =>
          db
            .connection()
            .from(foreignName)
            .insert({
              key: `key-${number}`,
              value: `value-${number}`,
              relation_one_to_many_1_id: number + 1,
            }),
        ),
      )

      const schemaForeign = base.set('table', foreignName)
      const schemaSource = object({
        ...base.properties,
        relation: schemaForeign.array(),
      }).set('table', sourceName)

      const result = await db.from(schemaSource).find()
      expect(result).toHaveLength(10)
    })
  })

  afterAll(() => db.close())
})
