import { AnyRecord } from 'pertype'
import {
  ColumnMetadata,
  RelationColumnMetadata,
  TableMetadata,
} from './metadata'
import { MapList } from './util/map'

export class EntryRegistry {
  private readonly storage: MapList<TableMetadata, Entry> = new MapList()

  public get tables(): IterableIterator<TableMetadata> {
    return this.storage.keys()
  }

  public get(table: TableMetadata): Entry[] {
    return this.storage.get(table)
  }

  public findById(table: TableMetadata, id: unknown): Entry {
    const entry = this.get(table).find((entry) => entry.id.value === id)
    if (entry !== undefined) {
      return entry
    }

    const newEntry = this.create(table)
    newEntry.id.value = id
    this.get(table).push(newEntry)
    return newEntry
  }

  public create(table: TableMetadata): Entry {
    const newEntry = new Entry(this, table)
    this.storage.get(table).push(newEntry)
    return newEntry
  }
}

export class Entry {
  public readonly properties: EntryProperty[] = []

  public constructor(
    private readonly registry: EntryRegistry,
    public readonly table: TableMetadata,
  ) {
    table.columns.forEach((column) => this.property(column))
  }

  public property(column: ColumnMetadata): EntryProperty {
    if (this.table !== column.table) {
      throw new Error(
        `Column "${column.table.name}"."${column.name}" is not exists within "${this.table.name}" table`,
      )
    }

    const property = this.properties.find((prop) => prop.column === column)
    if (property !== undefined) {
      return property
    }

    const newProperty =
      column instanceof RelationColumnMetadata
        ? column.collection
          ? new EntryPropertyMultiRelation(this.registry, this, column)
          : new EntryPropertyRelation(this.registry, this, column)
        : new EntryPropertyValue(this.registry, this, column)
    this.properties.push(newProperty)
    return newProperty
  }

  public get id(): EntryProperty {
    return this.property(this.table.id)
  }

  public get active(): boolean {
    return this.properties.find((prop) => prop.active) !== undefined
  }

  public get initialized(): boolean {
    return this.properties.find((prop) => prop.initialized) !== undefined
  }

  public set initialized(value: boolean) {
    this.properties
      .filter((prop) => prop.active)
      .forEach((prop) => (prop.initialized = value))
  }

  public get dirty(): boolean {
    return this.properties.find((prop) => prop.dirty) !== undefined
  }

  public set dirty(value: boolean) {
    this.properties
      .filter((prop) => prop.active)
      .forEach((prop) => (prop.dirty = value))
  }

  public get value(): AnyRecord {
    return Object.fromEntries(
      this.properties.map((prop) => [prop.column.name, prop.value]),
    )
  }

  public set value(value: AnyRecord) {
    this.properties
      .filter((prop) => Object.hasOwn(value, prop.column.name))
      .forEach((prop) => (prop.value = value[prop.column.name]))
  }

  public get raw(): AnyRecord {
    return Object.fromEntries(
      this.properties.map((prop) => [prop.column.name, prop.raw]),
    )
  }

  public set raw(value: AnyRecord) {
    this.properties
      .filter((prop) => Object.hasOwn(value, prop.column.name))
      .forEach((prop) => (prop.raw = value[prop.column.name]))
  }

  public sync(): void {
    this.properties.forEach((prop) => prop.sync())
  }
}

export abstract class EntryProperty {
  /**
   * Indicate if property is in use
   */
  private _active: boolean = false
  /**
   * Indicate if property is fetched from database
   */
  private _initialized: boolean = false
  /**
   * Indicate if property is dirty
   */
  private _dirty: boolean = false

  public constructor(
    public readonly registry: EntryRegistry,
    public readonly entry: Entry,
  ) {}

  public get active(): boolean {
    return this._active
  }

  public set active(value: boolean) {
    this._active = value
  }

  public get initialized(): boolean {
    return this._initialized
  }

  public set initialized(value: boolean) {
    this.active = true
    this._initialized = value
  }

  public get dirty(): boolean {
    return this._dirty
  }

  public set dirty(value: boolean) {
    this.active = true
    this._dirty = value
  }

  public sync(): void {}

  public abstract get column(): ColumnMetadata
  public abstract get value(): unknown
  public abstract set value(value: unknown)
  public abstract get raw(): unknown
  public abstract set raw(value: unknown)
}

export class EntryPropertyValue extends EntryProperty {
  private data: unknown

  public constructor(
    registry: EntryRegistry,
    entry: Entry,
    public readonly column: ColumnMetadata,
  ) {
    super(registry, entry)
  }

  public override get value(): unknown {
    return this.data
  }

  public override set value(value: unknown) {
    const newData = this.column.origin.optional().decode(value)
    this.dirty = this.dirty || this.data !== newData
    this.data = newData
  }

  public override get raw(): unknown {
    return this.column.origin.encode(this.data)
  }

  public override set raw(value: unknown) {
    this.value = value
  }
}

export class EntryPropertyRelation extends EntryProperty {
  private data: Entry | undefined

  public constructor(
    registry: EntryRegistry,
    entry: Entry,
    public readonly column: RelationColumnMetadata,
  ) {
    super(registry, entry)
  }

  public override get value(): unknown {
    return this.data?.value
  }

  public override set value(value: unknown) {
    this.unlink()
    this.data = undefined

    if (value !== undefined) {
      const row = this.column.foreignTable.schema.decode(value)
      const id = row[this.column.foreignTable.id.name]
      this.data =
        id !== undefined
          ? this.registry.findById(this.column.foreignTable, id)
          : this.registry.create(this.column.foreignTable)
      this.data.value = row
      this.link()
    }
  }

  public override get raw(): unknown {
    return this.column.origin.encode(this.value)
  }

  public override set raw(value: unknown) {
    this.value = value
  }

  public override sync(): void {
    this.link()
  }

  private link(): void {
    if (this.column.owner === 'source') {
      this.entry.property(this.column.sourceColumn).value = this.data?.property(
        this.column.foreignColumn,
      ).value
    } else if (this.data !== undefined) {
      this.data.property(this.column.foreignColumn).value = this.entry.property(
        this.column.sourceColumn,
      ).value
    }
  }

  private unlink(): void {
    if (this.column.owner === 'source') {
      this.entry.property(this.column.sourceColumn).value = undefined
    } else if (this.data !== undefined) {
      this.data.property(this.column.foreignColumn).value = undefined
    }
  }
}

export class EntryPropertyMultiRelation extends EntryProperty {
  private data: Entry[] | undefined

  public constructor(
    registry: EntryRegistry,
    entry: Entry,
    public readonly column: RelationColumnMetadata,
  ) {
    super(registry, entry)
  }

  public override get value(): unknown {
    return this.data?.map((entry) => entry.value)
  }

  public override set value(value: unknown) {
    this.unlink()
    this.data = undefined

    const entries = this.column.foreignTable.schema
      .array()
      .decode(value)
      .map((row) => {
        const id = row[this.column.foreignTable.id.name]
        const foreignEntry =
          id !== undefined
            ? this.registry.findById(this.column.foreignTable, id)
            : this.registry.create(this.column.foreignTable)
        foreignEntry.value = row
        return foreignEntry
      })
    if (entries.length > 0) {
      this.data = entries
      this.link()
    }
  }

  public override get raw(): unknown {
    return this.column.origin.encode(this.value)
  }

  public override set raw(value: unknown) {
    this.value = value
  }

  public override sync(): void {
    this.link()
  }

  private link(): void {
    if (this.data !== undefined) {
      if (this.column.owner === 'source') {
        this.entry.property(this.column.sourceColumn).value =
          this.data.reduce<unknown>(
            (_, foreignEntry) =>
              foreignEntry.property(this.column.foreignColumn).value,
            undefined,
          )
      } else {
        this.data.forEach((foreignEntry) => {
          foreignEntry.property(this.column.foreignColumn).value =
            this.entry.property(this.column.sourceColumn).value
        })
      }
    }
  }

  private unlink(): void {
    if (this.column.owner === 'source') {
      this.entry.property(this.column.sourceColumn).value = undefined
    } else {
      this.data?.map(
        (foreignEntry) =>
          (foreignEntry.property(this.column.foreignColumn).value = undefined),
      )
    }
  }
}
