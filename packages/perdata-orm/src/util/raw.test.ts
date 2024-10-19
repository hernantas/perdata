import { number } from 'pertype'
import { MetadataRegistry } from '../metadata'
import { entity, generate, id } from '../schema'
import { createRaw } from './raw'

describe('createRaw', () => {
  const foreignOne = entity('foreign_one', {
    id: number().decorate(id(), generate()),
  })
  const foreignMulti = entity('foreign_multi', {
    id: number().decorate(id(), generate()),
  })
  const schema = entity('base', {
    id: number().decorate(id(), generate()),
    ids: number().array(),
    foreign: foreignOne,
    foreigns: foreignMulti.array(),
  })
  const table = new MetadataRegistry().get(schema)

  it('Should return undefined when passed undefined object', () => {
    const data = createRaw(table, undefined)
    expect(data).toBe(undefined)
  })

  it('Should return empty object when passed empty object', () => {
    const data = createRaw(table, {})
    expect(data).toStrictEqual({})
  })

  describe('Raw Single Value', () => {
    it('Should return undefined as undefined', () => {
      const data = createRaw(table, {
        id: undefined,
      })
      expect(data).toHaveProperty('id', undefined)
    })

    it('Should return value converted into string', () => {
      const data = createRaw(table, {
        id: 1,
      })
      expect(data).toHaveProperty('id', '1')
    })
  })

  describe('Raw Multi Value', () => {
    it('Should return undefined as undefined', () => {
      const data = createRaw(table, {
        ids: undefined,
      })
      expect(data).toHaveProperty('ids', undefined)
    })

    it('Should return empty array as empty array', () => {
      const data = createRaw(table, {
        ids: [],
      })
      expect(data).toHaveProperty('ids', [])
    })

    it('Should return array with undefined as array with undefined', () => {
      const data = createRaw(table, {
        ids: [undefined, undefined],
      })
      expect(data).toHaveProperty('ids')
      expect(data).toHaveProperty('ids.0', undefined)
      expect(data).toHaveProperty('ids.1', undefined)
    })

    it('Should return array as array', () => {
      const data = createRaw(table, {
        ids: [1, 2, 3],
      })
      expect(data).toHaveProperty('ids')
      expect(data).toHaveProperty('ids.0', '1')
      expect(data).toHaveProperty('ids.1', '2')
      expect(data).toHaveProperty('ids.2', '3')
    })
  })

  describe('Raw Single Object', () => {
    it('Should return undefined as undefined', () => {
      const data = createRaw(table, {
        foreign: undefined,
      })
      expect(data).toHaveProperty('foreign', undefined)
    })

    it('Should return object with its property converted into raw', () => {
      const data = createRaw(table, {
        foreign: { id: 1 },
      })
      expect(data).toHaveProperty('foreign')
      expect(data).toHaveProperty('foreign.id', '1')
    })
  })

  describe('Raw Multi Object', () => {
    it('Should return undefined as undefined', () => {
      const data = createRaw(table, {
        foreigns: undefined,
      })
      expect(data).toHaveProperty('foreigns', undefined)
    })

    it('Should return empty array as empty array', () => {
      const data = createRaw(table, {
        foreigns: [],
      })
      expect(data).toHaveProperty('foreigns', [])
    })

    it('Should return array with undefined as array with undefined', () => {
      const data = createRaw(table, {
        foreigns: [undefined, undefined],
      })
      expect(data).toHaveProperty('foreigns')
      expect(data).toHaveProperty('foreigns.0', undefined)
      expect(data).toHaveProperty('foreigns.1', undefined)
    })

    it('Should return array as array', () => {
      const data = createRaw(table, {
        foreigns: [{ id: 1 }, { id: 2 }, { id: 3 }],
      })
      expect(data).toHaveProperty('foreigns')
      expect(data).toHaveProperty('foreigns.0')
      expect(data).toHaveProperty('foreigns.0.id', '1')
      expect(data).toHaveProperty('foreigns.1')
      expect(data).toHaveProperty('foreigns.1.id', '2')
      expect(data).toHaveProperty('foreigns.2')
      expect(data).toHaveProperty('foreigns.2.id', '3')
    })
  })
})
