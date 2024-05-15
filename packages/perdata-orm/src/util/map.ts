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
