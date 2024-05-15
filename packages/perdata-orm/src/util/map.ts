export class SafeMap<K, V> {
  private readonly maps: Map<K, V> = new Map()

  public constructor(private readonly factory: (key: K) => V) {}

  public get size(): number {
    return this.maps.size
  }

  public get [Symbol.toStringTag](): string {
    return 'MapList'
  }

  public has(key: K): boolean {
    return this.maps.has(key)
  }

  public get(key: K): V {
    const values = this.maps.get(key)
    if (values !== undefined) {
      return values
    }

    const newValues: V = this.factory(key)
    this.maps.set(key, newValues)
    return newValues
  }

  public delete(key: K): boolean {
    return this.maps.delete(key)
  }

  public clear(): void {
    this.maps.clear()
  }

  public entries(): IterableIterator<[K, V]> {
    return this.maps.entries()
  }

  public keys(): IterableIterator<K> {
    return this.maps.keys()
  }

  public values(): IterableIterator<V> {
    return this.maps.values()
  }

  public forEach(
    callbackfn: (value: V, key: K, map: Map<K, V>) => void,
    thisArg?: any,
  ): void {
    this.maps.forEach(callbackfn, thisArg)
  }

  public [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.maps.entries()
  }
}

export class MapList<K, V> extends SafeMap<K, V[]> {
  public constructor() {
    super(() => [])
  }
}

export class MapSet<K, V> extends SafeMap<K, Set<V>> {
  public constructor() {
    super(() => new Set())
  }
}

export class BiMap<K, V> {
  private readonly kvMap: Map<K, V> = new Map()
  private readonly vkMap: Map<V, K> = new Map()

  public get keys(): IterableIterator<K> {
    return this.kvMap.keys()
  }

  public get values(): IterableIterator<V> {
    return this.kvMap.values()
  }

  public size(): number {
    return this.kvMap.size
  }

  public clear(): void {
    this.kvMap.clear()
    this.vkMap.clear()
  }

  public hasByKey(key: K): boolean {
    return this.kvMap.has(key)
  }

  public getByKey(key: K): V | undefined {
    return this.kvMap.get(key)
  }

  public setByKey(key: K, value: V): void {
    this.deleteByKey(key)
    this.deleteByValue(value)
    this.kvMap.set(key, value)
    this.vkMap.set(value, key)
  }

  public deleteByKey(key: K): void {
    if (this.kvMap.has(key)) {
      this.vkMap.delete(this.kvMap.get(key)!)
    }
    this.kvMap.delete(key)
  }

  public hasByValue(value: V): boolean {
    return this.vkMap.has(value)
  }

  public getByValue(value: V): K | undefined {
    return this.vkMap.get(value)
  }

  public setByValue(value: V, key: K): void {
    this.deleteByValue(value)
    this.deleteByKey(key)
    this.vkMap.set(value, key)
    this.kvMap.set(key, value)
  }

  public deleteByValue(value: V): void {
    if (this.vkMap.has(value)) {
      this.kvMap.delete(this.vkMap.get(value)!)
    }
    this.vkMap.delete(value)
  }
}
