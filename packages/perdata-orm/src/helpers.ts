import { BuilderPlugin, Schema } from 'pertype'

export function entity(name: string): BuilderPlugin<Schema, Schema> {
  return (schema) => schema.set('entity', name)
}

export function table(name: string): BuilderPlugin<Schema, Schema> {
  return (schema) => schema.set('entity', name)
}

export function id(boolean: boolean = true): BuilderPlugin<Schema, Schema> {
  return (schema) => schema.set('id', boolean)
}

export function generated(
  boolean: boolean = true,
): BuilderPlugin<Schema, Schema> {
  return (schema) => schema.set('generated', boolean)
}

export function generate(
  boolean: boolean = true,
): BuilderPlugin<Schema, Schema> {
  return (schema) => schema.set('generated', boolean)
}

export interface RelationOptions {
  readonly owner?: 'source' | 'foreign'
  readonly name?: string
  readonly reference?: string
}

export function relation(
  options: RelationOptions,
): BuilderPlugin<Schema, Schema> {
  return (schema) => {
    if (options.owner !== undefined) {
      schema = schema.set('joinOwner', options.owner)
    }
    if (options.name !== undefined) {
      schema = schema.set('joinName', options.name)
    }
    if (options.reference !== undefined) {
      schema = schema.set('joinReference', options.reference)
    }
    return schema
  }
}
