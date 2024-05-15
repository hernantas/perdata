import { AnyRecord } from 'pertype'
import {
  ColumnMetadata,
  RelationColumnMetadata,
  TableMetadata,
} from './metadata'
import { MapList } from './util/map'

export class EntryRegistry {
  private readonly storage: MapList<TableMetadata, Entry> = new MapList()

  public get(table: TableMetadata): Entry[] {
    return this.storage.get(table)
  }

  public findById(table: TableMetadata, id: unknown): Entry[] {
    const entries = this.get(table).filter((entry) => entry.id.value === id)
    if (entries.length === 0) {
      const newEntry = this.create(table)
      newEntry.id.value = id
      entries.push(newEntry)
    }
    return entries
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
        ? new EntryPropertyRelation(this.registry, this, column)
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
  public constructor(
    registry: EntryRegistry,
    entry: Entry,
    public readonly column: RelationColumnMetadata,
  ) {
    super(registry, entry)
  }

  private get foreignEntries(): Entry[] {
    const sourceProperty = this.entry.property(this.column.sourceColumn)
    return sourceProperty.value !== undefined
      ? this.registry
          .get(this.column.foreignTable)
          .filter(
            (foreignEntry) =>
              foreignEntry.property(this.column.foreignColumn).value ===
              sourceProperty.value,
          )
      : []
  }

  public override get value(): unknown {
    const entries = this.foreignEntries
    return this.column.collection
      ? entries.map((entry) => entry.value)
      : entries[0]?.value
  }

  public override set value(value: unknown) {
    const sourceProperty = this.entry.property(this.column.sourceColumn)

    // update old foreign entries
    if (sourceProperty.value !== undefined) {
      // unlink the data
      if (this.column.owner === 'source') {
        sourceProperty.value = undefined
      } else {
        this.foreignEntries.forEach(
          (entry) =>
            (entry.property(this.column.foreignColumn).value = undefined),
        )
      }
    }

    if (value !== undefined) {
      const values = Array.isArray(value) ? value : [value]
      values.forEach((value) => {
        const lookupValue = value[this.column.foreignTable.id.name]
        this.registry
          .findById(this.column.table, lookupValue)
          .forEach((foreignEntry) => {
            // apply changes
            if (this.column.type === 'strong') {
              foreignEntry.value = value
            }

            // linking
            if (this.column.owner === 'source') {
              this.entry.property(this.column.sourceColumn).value =
                foreignEntry.property(this.column.foreignColumn).value
            } else {
              foreignEntry.property(this.column.foreignColumn).value =
                this.entry.property(this.column.sourceColumn).value
            }
          })
      })
    }
  }

  public override get raw(): unknown {
    return this.column.origin.encode(this.value)
  }

  public override set raw(value: unknown) {
    this.value = value
  }
}
