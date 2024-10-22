import {
  ColumnMetadata,
  RelationColumnMetadata,
  TableMetadata,
} from './metadata'
import { BiMap, MapSet, SafeMap } from './util/map'
import {
  Raw,
  RawMultiObject,
  RawMultiValue,
  RawRelation,
  RawSingleObject,
  RawSingleValue,
  RawValue,
} from './util/raw'

export class EntryRegistry {
  private readonly storage: MapSet<TableMetadata, Entry> = new MapSet()
  private readonly mapId: SafeMap<
    TableMetadata,
    BiMap<NonNullable<Raw>, Entry>
  > = new SafeMap(() => new BiMap())

  public get tables(): IterableIterator<TableMetadata> {
    return this.storage.keys()
  }

  public create(table: TableMetadata): Entry {
    const entry = new Entry(this, table)
    this.storage.get(table).add(entry)
    return entry
  }

  public findAll(table: TableMetadata): Entry[] {
    return this.storage.get(table).values().toArray()
  }

  public findById(table: TableMetadata, id: Raw): Entry | undefined {
    return id !== undefined ? this.mapId.get(table).getByKey(id) : undefined
  }

  public instantiate(
    table: TableMetadata,
    value: NonNullable<RawSingleObject>,
  ): Entry
  public instantiate(
    table: TableMetadata,
    value: RawSingleObject,
  ): Entry | undefined
  public instantiate(
    table: TableMetadata,
    value: RawSingleObject,
  ): Entry | undefined {
    if (value !== undefined) {
      const id = value?.[table.id.name]
      const entry = this.findById(table, id) ?? this.create(table)
      entry.value = value
      return entry
    }
    return undefined
  }

  public registerId(entry: Entry): void {
    this.ensure(entry)

    const table = entry.table
    const mapId = this.mapId.get(table)
    const id = entry.id.value
    if (id !== undefined) {
      const oldEntry = mapId.getByKey(id)
      if (oldEntry !== undefined && oldEntry !== entry) {
        throw new Error('Cannot register entry, id key already exists')
      }
      mapId.setByKey(id, entry)
    } else {
      mapId.deleteByValue(entry)
    }
  }

  private ensure(entry: Entry): void {
    if (!this.storage.get(entry.table).has(entry)) {
      throw new Error('Entry is not created from this registry')
    }
  }
}

export class Entry {
  public readonly baseProperties: EntryPropertyValue[]
  public readonly relationProperties: EntryPropertyRelation[]
  public readonly id: EntryProperty

  private _initialized: boolean = false
  private _remove: boolean = false

  public constructor(
    registry: EntryRegistry,
    public readonly table: TableMetadata,
  ) {
    this.baseProperties = table.baseColumns.map((column) =>
      column.collection
        ? new EntryPropertyMultiValue(registry, this, column)
        : new EntryPropertySingleValue(registry, this, column),
    )
    this.relationProperties = table.relationColumns.map((column) =>
      column.collection
        ? new EntryPropertyMultiRelation(registry, this, column)
        : new EntryPropertySingleRelation(registry, this, column),
    )
    const id = this.properties.find((prop) => prop.column.id)
    if (id === undefined) {
      throw new Error('Unexpected error, table do not have identifier column')
    }
    this.id = id
  }

  public get properties(): EntryProperty[] {
    return [...this.baseProperties, ...this.relationProperties]
  }

  public property(column: ColumnMetadata | string): EntryProperty | undefined {
    const columnName = typeof column === 'string' ? column : column.name
    return this.properties.find((prop) => prop.column.name === columnName)
  }

  public get initialized(): boolean {
    return this._initialized
  }

  public set initialized(value: boolean) {
    this._initialized = value
  }

  public get remove(): boolean {
    return this._remove
  }

  public set remove(value: boolean) {
    this._remove = value
  }

  public get dirty(): boolean {
    return this.properties.find((prop) => prop.dirty) !== undefined
  }

  public set dirty(value: boolean) {
    this.properties.forEach((prop) => (prop.dirty = value))
  }

  public get value(): NonNullable<RawSingleObject> {
    const entries = this.properties.map(
      (prop) => [prop.column.name, prop.value] as const,
    )
    return Object.fromEntries(entries)
  }

  public set value(value: RawSingleObject) {
    this.properties.forEach((prop) => {
      if (value === undefined) {
        prop.value = undefined
      } else if (Object.hasOwn(value, prop.column.name)) {
        prop.value = value[prop.column.name]
      }
    })
  }

  public bind(): void {
    this.relationProperties.forEach((prop) => prop.bind())
  }

  public unbind(): void {
    this.relationProperties.forEach((prop) => prop.unbind())
  }
}

export abstract class EntryProperty {
  private _dirty: boolean = false

  public constructor(
    protected readonly registry: EntryRegistry,
    protected readonly entry: Entry,
    public readonly column: ColumnMetadata,
  ) {}

  public get dirty(): boolean {
    return this._dirty
  }

  public set dirty(value: boolean) {
    this._dirty = value
  }

  public abstract get value(): Raw

  public abstract set value(value: Raw)
}

export abstract class EntryPropertyValue extends EntryProperty {
  public abstract override get value(): RawValue
  public abstract override set value(value: RawValue)
}

export class EntryPropertySingleValue extends EntryPropertyValue {
  private data: RawSingleValue = undefined

  public get value(): RawSingleValue {
    return this.data
  }

  public set value(value: RawSingleValue) {
    this.dirty = this.dirty || this.data !== value
    this.data = value

    if (this.column.id) {
      this.registry.registerId(this.entry)
    }
  }
}

export class EntryPropertyMultiValue extends EntryPropertyValue {
  private data: RawMultiValue

  public get value(): RawMultiValue {
    return this.data
  }

  public set value(value: RawMultiValue) {
    this.dirty = this.dirty || isMultiValueDirty(this.data, value)
    this.data = value
  }
}

function isMultiValueDirty(
  value1: RawMultiValue,
  value2: RawMultiValue,
): boolean {
  const isUndefined1 = value1 === undefined
  const isUndefined2 = value2 === undefined
  if (isUndefined1 !== isUndefined2) {
    return true
  }

  const array1 = value1 ?? []
  const array2 = value2 ?? []
  if (array1.length !== array2.length) {
    return true
  }

  const length = Math.max(array1.length, array2.length)
  for (let i = 0; i < length; i++) {
    if (array1[i] !== array2[i]) {
      return true
    }
  }

  return false
}

export abstract class EntryPropertyRelation extends EntryProperty {
  public constructor(
    registry: EntryRegistry,
    entry: Entry,
    public override readonly column: RelationColumnMetadata,
  ) {
    super(registry, entry, column)
  }
  public abstract override get value(): RawRelation
  public abstract override set value(value: RawRelation)
  public abstract bind(): void
  public abstract unbind(): void
}

export class EntryPropertySingleRelation extends EntryPropertyRelation {
  private data: Entry | undefined = undefined

  public override get value(): RawSingleObject {
    return this.data?.value
  }

  public override set value(value: RawSingleObject) {
    const newData = this.registry.instantiate(this.column.foreignTable, value)

    if (this.data !== newData) {
      this.dirty = true
      this.unbind()
      this.data = newData
      this.bind()
    }
  }

  public override bind(): void {
    const sourceProperty = this.entry.property(this.column.sourceColumn)
    const foreignProperty = this.data?.property(this.column.foreignColumn)
    if (this.column.owner === 'source' && sourceProperty !== undefined) {
      sourceProperty.value = foreignProperty?.value
    } else if (
      this.column.owner === 'foreign' &&
      foreignProperty !== undefined
    ) {
      foreignProperty.value = sourceProperty?.value
    }
  }

  public override unbind(): void {
    const sourceProperty = this.entry.property(this.column.sourceColumn)
    const foreignProperty = this.data?.property(this.column.foreignColumn)
    if (this.column.owner === 'source' && sourceProperty !== undefined) {
      sourceProperty.value = undefined
    } else if (
      this.column.owner === 'foreign' &&
      foreignProperty !== undefined
    ) {
      foreignProperty.value = undefined
    }
  }
}

export class EntryPropertyMultiRelation extends EntryPropertyRelation {
  private data: (Entry | undefined)[] | undefined

  public override get value(): RawMultiObject {
    return this.data?.map((item) => item?.value)
  }

  public override set value(value: RawMultiObject) {
    this.unbind()
    const entries = value?.map((item) =>
      this.registry.instantiate(this.column.foreignTable, item),
    )
    this.dirty = this.dirty || isEntriesDirty(this.data, entries)
    this.data = entries
    this.bind()
  }

  public override bind(): void {
    const sourceProperty = this.entry.property(this.column.sourceColumn)
    if (this.column.owner === 'source' && sourceProperty !== undefined) {
      this.data
        ?.map((entry) => entry?.property(this.column.foreignColumn))
        .forEach((prop) => (sourceProperty.value = prop?.value))
    } else if (this.column.owner === 'foreign') {
      this.data
        ?.map((entry) => entry?.property(this.column.foreignColumn))
        .filter((prop) => prop !== undefined)
        .forEach((prop) => (prop.value = sourceProperty?.value))
    }
  }

  public override unbind(): void {
    const sourceProperty = this.entry.property(this.column.sourceColumn)
    if (this.column.owner === 'source' && sourceProperty !== undefined) {
      sourceProperty.value = undefined
    } else if (this.column.owner === 'foreign') {
      this.data
        ?.map((entry) => entry?.property(this.column.foreignColumn))
        .filter((prop) => prop !== undefined)
        .forEach((prop) => (prop.value = undefined))
    }
  }
}

function isEntriesDirty(
  value1: (Entry | undefined)[] | undefined,
  value2: (Entry | undefined)[] | undefined,
): boolean {
  const isUndefined1 = value1 === undefined
  const isUndefined2 = value2 === undefined
  if (isUndefined1 !== isUndefined2) {
    return true
  }

  const array1 = value1 ?? []
  const array2 = value2 ?? []
  if (array1.length !== array2.length) {
    return true
  }

  const length = Math.max(array1.length, array2.length)
  for (let index = 0; index < length; index++) {
    const id1 = array1[index]?.id.value
    const id2 = array2[index]?.id.value
    if (id1 !== id2) {
      return true
    }
  }

  return false
}
