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
