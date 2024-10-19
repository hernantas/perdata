import { AnyRecord, Key, string } from 'pertype'
import { TableMetadata } from '../metadata'

export type RawSingleValue = string | undefined
export type RawMultiValue = RawSingleValue[] | undefined
export type RawValue = RawSingleValue | RawMultiValue

export type RawSingleObject = { [key: Key]: Raw } | undefined
export type RawMultiObject = RawSingleObject[] | undefined
export type RawRelation = RawSingleObject | RawMultiObject

export type Raw = RawValue | RawRelation

function getRawSingleValue(value: unknown): RawSingleValue {
  return string().nullable().optional().decode(value) ?? undefined
}

function getRawMultiValue(value: unknown): RawMultiValue {
  const decoded =
    string()
      .nullable()
      .optional()
      .array()
      .nullable()
      .optional()
      .decode(value) ?? undefined
  return decoded?.map((item) => item ?? undefined)
}

function getRawSingleObject(
  table: TableMetadata,
  value: unknown,
): RawSingleObject {
  if (isRecord(value)) {
    if (value === undefined || value === null) {
      return undefined
    }

    const data: NonNullable<RawSingleObject> = {}
    table.baseColumns
      .filter((column) => Object.hasOwn(value, column.name))
      .forEach((column) => {
        data[column.name] = column.collection
          ? getRawMultiValue(value[column.name])
          : getRawSingleValue(value[column.name])
      })
    table.relationColumns
      .filter((column) => Object.hasOwn(value, column.name))
      .forEach(
        (column) =>
          (data[column.name] = column.collection
            ? getRawMultiObject(table, value[column.name])
            : getRawSingleObject(table, value[column.name])),
      )
    return data
  }
  throw new Error('Cannot create raw object from invalid value')
}

function isRecord(value: unknown): value is AnyRecord | null | undefined {
  return value === undefined || typeof value === 'object'
}

function getRawMultiObject(
  table: TableMetadata,
  value: unknown,
): RawMultiObject {
  const values = Array.isArray(value)
    ? value
    : value !== undefined && value !== null
      ? [value]
      : undefined
  if (values === undefined) {
    return undefined
  }
  return values.map((value) => getRawSingleObject(table, value))
}

export function createRaw(
  table: TableMetadata,
  value: unknown,
): RawSingleObject {
  return getRawSingleObject(table, value)
}
