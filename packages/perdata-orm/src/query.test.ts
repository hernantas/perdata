import { Knex } from 'knex'
import { AnyRecord, number, object, string } from 'pertype'
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

  async function setup(
    connection: Knex,
    tableName: string,
    fn: (index: number) => AnyRecord,
  ) {
    await connection.from(tableName).truncate()
    await Promise.all(
      [...Array(10).keys()].map((index) =>
        connection.from(tableName).insert(fn(index)),
      ),
    )
  }

  describe('Select', () => {
    const tableName = 'simple_select'

    beforeAll(async () =>
      setup(db.connection(), tableName, (number) => ({
        key: `key-${number}`,
        value: `value-${number}`,
      })),
    )

    it('Should be able to select all from db table', async () => {
      const schema = base.set('table', tableName)
      const result = await db.from(schema).find()
      expect(result).toHaveLength(10)
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).toHaveProperty('key')
      expect(result[0]).toHaveProperty('value')
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
    describe('One-to-One (Owner: Source) Relationship', () => {
      it('Should be able to resolve relations on "find"', async () => {
        const foreignName = 'one_to_one_source_find_2'
        await setup(db.connection(), foreignName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))
        const foreignSchema = base.set('table', foreignName)

        const sourceName = 'one_to_one_source_find_1'
        await setup(db.connection(), sourceName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
          one_to_one_source_find_2_id: number + 1,
        }))
        const schema = object({
          ...base.props,
          relation: foreignSchema,
        }).set('table', sourceName)

        const result = await db.from(schema).find()
        expect(result).toHaveLength(10)
      })

      it('Should be able to insert (and insert relation)', async () => {
        const foreignName = 'one_to_one_source_insert_insert_2'
        await db.connection().from(foreignName).truncate()
        const foreignSchema = base.set('table', foreignName)

        const sourceName = 'one_to_one_source_insert_insert_1'
        await db.connection().from(sourceName).truncate()
        const schema = object({
          ...base.props,
          relation: foreignSchema,
        }).set('table', sourceName)

        const inserted = await db.from(schema).insert({
          key: 'key',
          value: 'value',
          relation: {
            key: 'key',
            value: 'value',
          },
        })

        expect(inserted).toHaveLength(1)
        expect(inserted[0]).toHaveProperty('id')
        expect(inserted[0]).toHaveProperty('key', 'key')
        expect(inserted[0]).toHaveProperty('value', 'value')
        expect(inserted[0]).toHaveProperty('relation')
        expect(inserted[0]!.relation).toHaveProperty('id')
        expect(inserted[0]!.relation).toHaveProperty('key', 'key')
        expect(inserted[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find()
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })

      it('Should be able to insert (and save relation)', async () => {
        const foreignName = 'one_to_one_source_insert_save_2'
        await setup(db.connection(), foreignName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))
        const foreignSchema = base.set('table', foreignName)

        const sourceName = 'one_to_one_source_insert_save_1'
        await db.connection().from(sourceName).truncate()
        const schema = object({
          ...base.props,
          relation: foreignSchema.set('reference', 'strong'),
        }).set('table', sourceName)

        const inserted = await db.from(schema).insert({
          key: 'key',
          value: 'value',
          relation: {
            id: 1,
            key: 'key',
            value: 'value',
          },
        })

        expect(inserted).toHaveLength(1)
        expect(inserted[0]).toHaveProperty('id')
        expect(inserted[0]).toHaveProperty('key', 'key')
        expect(inserted[0]).toHaveProperty('value', 'value')
        expect(inserted[0]).toHaveProperty('relation')
        expect(inserted[0]!.relation).toHaveProperty('id')
        expect(inserted[0]!.relation).toHaveProperty('key', 'key')
        expect(inserted[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find()
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })

      it('Should be able to save (and insert relation)', async () => {
        const foreignName = 'one_to_one_source_save_insert_2'
        await db.connection().from(foreignName).truncate()
        const foreignSchema = base.set('table', foreignName)

        const sourceName = 'one_to_one_source_save_insert_1'
        await setup(db.connection(), sourceName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))
        const schema = object({
          ...base.props,
          relation: foreignSchema.optional().set('reference', 'strong'),
        }).set('table', sourceName)

        const saved = await db.from(schema).save({
          id: 1,
          key: 'key',
          value: 'value',
          relation: {
            key: 'key',
            value: 'value',
          },
        })

        expect(saved).toHaveLength(1)
        expect(saved[0]).toHaveProperty('id')
        expect(saved[0]).toHaveProperty('key', 'key')
        expect(saved[0]).toHaveProperty('value', 'value')
        expect(saved[0]).toHaveProperty('relation')
        expect(saved[0]!.relation).toHaveProperty('id')
        expect(saved[0]!.relation).toHaveProperty('key', 'key')
        expect(saved[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find(eq('id', 1))
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })
    })

    describe('One-to-One (Owner: Foreign) Relationship', () => {
      it('Should be able to resolve relations on "find"', async () => {
        const sourceName = 'one_to_one_foreign_find_1'
        await setup(db.connection(), sourceName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))

        const foreignName = 'one_to_one_foreign_find_2'
        await setup(db.connection(), foreignName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
          one_to_one_foreign_find_1_id: number + 1,
        }))

        const foreignSchema = base.set('table', foreignName)
        const schema = object({
          ...base.props,
          relation: foreignSchema.set('owner', 'foreign'),
        }).set('table', sourceName)

        const result = await db.from(schema).find()
        expect(result).toHaveLength(10)
      })

      it('Should be able to insert (and insert relation)', async () => {
        const sourceName = 'one_to_one_foreign_insert_insert_1'
        await db.connection().from(sourceName).truncate()

        const foreignName = 'one_to_one_foreign_insert_insert_2'
        await db.connection().from(foreignName).truncate()

        const foreignSchema = base.set('table', foreignName)
        const schema = object({
          ...base.props,
          relation: foreignSchema.set('owner', 'foreign'),
        }).set('table', sourceName)

        const inserted = await db.from(schema).insert({
          key: 'key',
          value: 'value',
          relation: {
            key: 'key',
            value: 'value',
          },
        })

        expect(inserted).toHaveLength(1)
        expect(inserted[0]).toHaveProperty('id')
        expect(inserted[0]).toHaveProperty('key', 'key')
        expect(inserted[0]).toHaveProperty('value', 'value')
        expect(inserted[0]).toHaveProperty('relation')
        expect(inserted[0]!.relation).toHaveProperty('id')
        expect(inserted[0]!.relation).toHaveProperty('key', 'key')
        expect(inserted[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find()
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })

      it('Should be able to insert (and save relation)', async () => {
        const sourceName = 'one_to_one_foreign_insert_save_1'
        await db.connection().from(sourceName).truncate()

        const foreignName = 'one_to_one_foreign_insert_save_2'
        await setup(db.connection(), foreignName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))

        const foreignSchema = base.set('table', foreignName)
        const schema = object({
          ...base.props,
          relation: foreignSchema
            .set('owner', 'foreign')
            .set('reference', 'strong'),
        }).set('table', sourceName)

        const inserted = await db.from(schema).insert({
          key: 'key',
          value: 'value',
          relation: {
            id: 1,
            key: 'key',
            value: 'value',
          },
        })

        expect(inserted).toHaveLength(1)
        expect(inserted[0]).toHaveProperty('id')
        expect(inserted[0]).toHaveProperty('key', 'key')
        expect(inserted[0]).toHaveProperty('value', 'value')
        expect(inserted[0]).toHaveProperty('relation')
        expect(inserted[0]!.relation).toHaveProperty('id')
        expect(inserted[0]!.relation).toHaveProperty('key', 'key')
        expect(inserted[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find()
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })

      it('Should be able to save (and insert relation)', async () => {
        const sourceName = 'one_to_one_foreign_save_insert_1'
        await setup(db.connection(), sourceName, (number) => ({
          key: `key-${number}`,
          value: `value-${number}`,
        }))

        const foreignName = 'one_to_one_foreign_save_insert_2'
        await db.connection().from(foreignName).truncate()

        const foreignSchema = base.set('table', foreignName)
        const schema = object({
          ...base.props,
          relation: foreignSchema
            .set('owner', 'foreign')
            .optional()
            .set('reference', 'strong'),
        }).set('table', sourceName)

        const saved = await db.from(schema).save({
          id: 1,
          key: 'key',
          value: 'value',
          relation: {
            key: 'key',
            value: 'value',
          },
        })

        expect(saved).toHaveLength(1)
        expect(saved[0]).toHaveProperty('id')
        expect(saved[0]).toHaveProperty('key', 'key')
        expect(saved[0]).toHaveProperty('value', 'value')
        expect(saved[0]).toHaveProperty('relation')
        expect(saved[0]!.relation).toHaveProperty('id')
        expect(saved[0]!.relation).toHaveProperty('key', 'key')
        expect(saved[0]!.relation).toHaveProperty('value', 'value')

        const result = await db.from(schema).find(eq('id', 1))
        expect(result).toHaveLength(1)
        expect(result[0]).toHaveProperty('id')
        expect(result[0]).toHaveProperty('key', 'key')
        expect(result[0]).toHaveProperty('value', 'value')
        expect(result[0]).toHaveProperty('relation')
        expect(result[0]!.relation).toHaveProperty('id')
        expect(result[0]!.relation).toHaveProperty('key', 'key')
        expect(result[0]!.relation).toHaveProperty('value', 'value')
      })
    })
  })

  afterAll(() => db.close())
})
