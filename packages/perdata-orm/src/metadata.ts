import {
  AnyRecord,
  ArraySchema,
  NullableSchema,
  ObjectSchema,
  OptionalSchema,
  Schema,
  bool,
  literal,
  string,
  union,
} from 'pertype'
import { SchemaReader } from './util/reader'

export class TableMetadata {
  public readonly name: string
  public readonly id: ColumnMetadata
  public readonly baseColumns: ColumnMetadata[] = []
  public readonly relationColumns: RelationColumnMetadata[] = []

  public constructor(public readonly schema: Schema) {
    const name = readEntity(schema)
    if (name === undefined) {
      throw new Error(
        'Cannot read "entity" or "table" name metadata from Schema',
      )
    }
    this.name = name

    const props = readProperties(schema)
    for (const [key, schema] of Object.entries(props)) {
      const relation = readEntity(schema)
      if (relation === undefined) {
        const newColumn = new ColumnMetadata(this, key, schema, true)
        this.baseColumns.push(newColumn)
      } else {
        const newColumn = new RelationColumnMetadata(this, key, schema, true)
        this.relationColumns.push(newColumn)
      }
    }

    const id = this.baseColumns.find((column) => column.id)
    if (id === undefined) {
      throw new Error('Entity must have "id" or "primary" column')
    }
    this.id = id
  }

  public column(name: string): ColumnMetadata | undefined {
    return this.columns.find((column) => column.name === name)
  }

  public get columns(): ColumnMetadata[] {
    return this.baseColumns.concat(...this.relationColumns)
  }

  public get baseSchema(): ObjectSchema<AnyRecord<Schema>> {
    return ObjectSchema.create(
      Object.fromEntries(this.baseColumns.map((col) => [col.name, col.schema])),
    )
  }

  public get relationSchema(): ObjectSchema<AnyRecord<Schema>> {
    return ObjectSchema.create(
      Object.fromEntries(
        this.relationColumns.map((col) => [col.name, col.schema]),
      ),
    )
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

export class RelationColumnMetadata
  extends ColumnMetadata
  implements RelationMetadata
{
  public readonly owner: 'source' | 'foreign'
  public readonly sourceColumn: ColumnMetadata
  public readonly foreignColumn: ColumnMetadata
  public readonly type: 'strong' | 'weak'

  public constructor(
    table: TableMetadata,
    name: string,
    schema: Schema,
    declared: boolean,
  ) {
    super(table, name, schema, declared)

    const sourceTable = this.table
    const foreignTable = new TableMetadata(schema)

    this.owner = this.collection ? 'foreign' : readJoinOwner(schema)

    const targetTable = this.owner === 'source' ? foreignTable : sourceTable
    const targetColumn = targetTable.id

    const joinColumnName =
      readJoinName(schema) ?? `${targetTable.name}_${targetColumn.name}`

    const ownerTable = this.owner === 'source' ? sourceTable : foreignTable
    const ownerColumn =
      ownerTable.column(joinColumnName) ??
      (() => {
        const newColumn = new ColumnMetadata(
          ownerTable,
          joinColumnName,
          schema.set('id', false).set('generated', false),
          false,
        )
        table.baseColumns.push(newColumn)
        return newColumn
      })()

    this.sourceColumn = this.owner === 'source' ? ownerColumn : targetColumn
    this.foreignColumn = this.owner === 'source' ? targetColumn : ownerColumn

    this.type = readReference(schema)
  }
}

export interface RelationMetadata {
  /** Owner of join column */
  readonly owner: 'source' | 'foreign'
  /** Column used as join column in source table */
  readonly sourceColumn: ColumnMetadata
  /** Column used as join column in foreign table */
  readonly foreignColumn: ColumnMetadata
  /** Relation type (strong will also update the referenced value) */
  readonly type: 'weak' | 'strong'
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

function readEntity(schema: Schema): string | undefined {
  return (
    SchemaReader.read(schema, 'entity', string().optional()) ??
    SchemaReader.read(schema, 'table', string().optional())
  )
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

function readJoinOwner(schema: Schema): 'source' | 'foreign' {
  const options = union(literal('source'), literal('foreign')).optional()
  return (
    (SchemaReader.read(schema, 'owner', options) ||
      SchemaReader.read(schema, 'joinOwner', options) ||
      SchemaReader.read(schema, 'join_owner', options)) ??
    'source'
  )
}

function readJoinName(schema: Schema): string | undefined {
  return (
    SchemaReader.read(schema, 'joinName', string().optional()) ||
    SchemaReader.read(schema, 'join_name', string().optional()) ||
    SchemaReader.read(schema, 'join', string().optional())
  )
}

function readReference(schema: Schema): 'strong' | 'weak' {
  const options = union(literal('strong'), literal('weak')).optional()
  return SchemaReader.read(schema, 'reference', options) ?? 'weak'
}
