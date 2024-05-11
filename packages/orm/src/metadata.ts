import {
  AnyRecord,
  ArraySchema,
  NullableSchema,
  ObjectSchema,
  OptionalSchema,
  Schema,
  bool,
  string,
} from 'pertype'
import { SchemaReader } from './util/reader'

export class TableMetadata {
  public readonly name: string

  public readonly columns: ColumnMetadata[]

  public constructor(public readonly schema: Schema) {
    this.name = readEntity(schema)
    const props = readProperties(schema)
    this.columns = Object.entries(props).map(
      ([key, schema]) => new ColumnMetadata(this, key, schema, true),
    )
  }

  public column(name: string): ColumnMetadata | undefined {
    return this.columns.find((column) => column.name === name)
  }
}

export class ColumnMetadata {
  /** Mark if column is an id */
  public readonly id: boolean
  /** Mark if column value is generated */
  public readonly generated: boolean
  /** Mark if column is nullable column */
  public readonly nullable: boolean
  /** Mark if column is collection column */
  public readonly collection: boolean

  public constructor(
    /** {@link TableMetadata} column owner */
    public readonly table: TableMetadata,
    /** Column name */
    public readonly name: string,
    /** {@link Schema} used to declare column */
    public readonly schema: Schema,
    /** Mark if column is declared in schema or not */
    public readonly declared: boolean,
  ) {
    this.id = readId(schema)
    this.generated = readGenerated(schema)
    this.nullable = detectNullable(schema)
    this.collection = detectCollection(schema)
  }
}

function readProperties(schema: Schema): AnyRecord<Schema> {
  return (
    SchemaReader.traverse<AnyRecord<Schema> | undefined>(
      (schema, inner) =>
        schema instanceof ObjectSchema
          ? (schema.props as AnyRecord<Schema>)
          : inner,
      schema,
    ) ?? {}
  )
}

function readEntity(schema: Schema): string {
  const name =
    SchemaReader.read(schema, 'entity', string().optional()) ??
    SchemaReader.read(schema, 'table', string().optional())
  if (name !== undefined) {
    return name
  }
  throw new Error('Cannot read "entity" or "table" name metadata from Schema')
}

function readId(schema: Schema): boolean {
  return SchemaReader.read(schema, 'id', bool().optional()) ?? false
}

function readGenerated(schema: Schema): boolean {
  return (
    SchemaReader.read(schema, 'generate', bool().optional()) ??
    SchemaReader.read(schema, 'gen', bool().optional()) ??
    SchemaReader.read(schema, 'generated', bool().optional()) ??
    false
  )
}

function detectNullable(schema: Schema): boolean {
  return (
    SchemaReader.traverse(
      (schema) =>
        schema instanceof NullableSchema || schema instanceof OptionalSchema,
      schema,
    ) ?? false
  )
}

function detectCollection(schema: Schema): boolean {
  return (
    SchemaReader.traverse(
      (schema, innerValue) => schema instanceof ArraySchema || innerValue,
      schema,
    ) ?? false
  )
}
