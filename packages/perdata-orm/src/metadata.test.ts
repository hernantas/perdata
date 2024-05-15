import { number, object, string } from 'pertype'
import { MetadataRegistry, TableMetadata } from './metadata'

describe('TableMetadata', () => {
  const base = object({
    normal: string(),
    id: number().set('id', true),
    generate: number().set('generate', true),
    gen: number().set('gen', true),
    generated: number().set('generated', true),
    nullable: string().nullable(),
    optional: string().optional(),
    collection: string().array(),
  })

  it('Should read "entity" metadata correctly', () => {
    const schema = base.set('entity', 'table_name')
    const table = new MetadataRegistry().get(schema)
    expect(table.name).toBe('table_name')
  })

  it('Should read "table" metadata correctly', () => {
    const schema = base.set('table', 'table_name')
    const table = new MetadataRegistry().get(schema)
    expect(table.name).toBe('table_name')
  })

  it('Should have 8 column declared', () => {
    const schema = base.set('table', 'table_name')
    const table = new MetadataRegistry().get(schema)
    expect(table.columns).toHaveLength(8)
  })

  describe('ColumnMetadata', () => {
    const schema = base.set('entity', 'table_name')
    const table = new MetadataRegistry().get(schema)
    testColumn({
      table,
      name: 'normal',
      id: false,
      generated: false,
      nullable: false,
      collection: false,
    })
    testColumn({
      table,
      name: 'id',
      id: true,
      generated: false,
      nullable: false,
      collection: false,
    })
    testColumn({
      table,
      name: 'generate',
      id: false,
      generated: true,
      nullable: false,
      collection: false,
    })
    testColumn({
      table,
      name: 'gen',
      id: false,
      generated: true,
      nullable: false,
      collection: false,
    })
    testColumn({
      table,
      name: 'generated',
      id: false,
      generated: true,
      nullable: false,
      collection: false,
    })
    testColumn({
      table,
      name: 'nullable',
      id: false,
      generated: false,
      nullable: true,
      collection: false,
    })
    testColumn({
      table,
      name: 'collection',
      id: false,
      generated: false,
      nullable: false,
      collection: true,
    })
  })

  describe('RelationColumnMetadata', () => {
    const relationSchema = base.set('entity', 'relation_name')
    const schema = object({
      id: base.props.id,
      key: string(),
      value: string().optional(),
      rel: relationSchema,
    }).set('entity', 'table_name')
    const table = new MetadataRegistry().get(schema)

    expect(table.columns).toHaveLength(5)
  })
})

function testColumn({
  table,
  name,
  id,
  generated,
  nullable,
  collection,
}: {
  table: TableMetadata
  name: string
  id: boolean
  generated: boolean
  nullable: boolean
  collection: boolean
}): void {
  const column = table.column(name)
  describe(`"${name}" Column`, () => {
    it(`Should contains columns named "${name}"`, () =>
      expect(column).not.toBeUndefined())
    it(`Should reference correct table "${table.name}"`, () =>
      expect(column!.table).toBe(table))
    it(`Should have "name" set as "${name}"`, () =>
      expect(column!.name).toBe(name))
    it(`Should have "id" set as "${id}"`, () => expect(column!.id).toBe(id))
    it(`Should have "generated" set as "${generated}"`, () =>
      expect(column!.generated).toBe(generated))
    it(`Should have "nullable" set as "${nullable}"`, () =>
      expect(column!.nullable).toBe(nullable))
    it(`Should have "collection" set as "${collection}"`, () =>
      expect(column!.collection).toBe(collection))
  })
}
