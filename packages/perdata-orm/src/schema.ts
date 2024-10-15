import { AnyRecord, Definition, object, ObjectSchema, Schema } from 'pertype'

export function id(value: boolean = true): Partial<Definition> {
  return { id: value }
}

export function generate(value: boolean = true): Partial<Definition> {
  return { generated: value }
}

export function table(name: string): Partial<Definition> {
  return { entity: name }
}

export function document(name: string): Partial<Definition> {
  return { entity: name }
}

export interface JoinOptions {
  readonly name?: string
  readonly owner?: 'source' | 'foreign'
}

export function join(options: JoinOptions): Partial<Definition> {
  return {
    joinName: options.name,
    joinOwner: options.owner ?? 'source',
  }
}

export function entity<S extends AnyRecord<Schema>>(
  name: string,
  schema: S,
): ObjectSchema<S> {
  return object(schema).set('entity', name)
}
