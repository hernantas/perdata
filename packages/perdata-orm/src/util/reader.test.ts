import { StringSchema, string } from 'pertype'
import { SchemaReader } from './reader'

describe('SchemaReader', () => {
  it('Should traverse to the inner most', () => {
    const schema = string().nullable().optional()
    expect(
      SchemaReader.traverse<boolean>(
        (schema, innerValue) =>
          schema instanceof StringSchema ? true : innerValue,
        schema,
      ),
    ).toBe(true)
  })

  it('Should read metadata attribute', () => {
    expect(
      SchemaReader.read(
        string().set('message', 'hello'),
        'message',
        string().optional(),
      ),
    ).toBe('hello')
  })
})
