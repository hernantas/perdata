import {
  ArraySchema,
  NullableSchema,
  OptionalSchema,
  Schema,
  TypeOf,
} from 'pertype'

export class SchemaReader {
  /**
   * Traverse {@link Schema} until its deepest part, the read its value using
   * given function and return its value
   *
   * @param fn A function to read given schema and return a value
   * @param schema {@link Schema} to be traversed
   */
  public static traverse<T>(
    fn: (schema: Schema, innerValue: T | undefined) => T | undefined,
    schema: Schema,
  ): T | undefined {
    const innerValue =
      schema instanceof ArraySchema ||
      schema instanceof NullableSchema ||
      schema instanceof OptionalSchema
        ? SchemaReader.traverse(fn, schema.inner)
        : undefined
    return fn(schema, innerValue)
  }

  /**
   * Read given {@link Schema} for given name metadata value
   *
   * @param schema {@link Schema} to be read
   * @param name Metadata attribute name
   * @param type Metadata {@link Schema} used to read attribute metadata
   * @returns A value if metadata exists, undefined otherwise
   */
  public static read<S extends Schema>(
    schema: Schema,
    name: string,
    type: S,
  ): TypeOf<S> {
    const value = SchemaReader.traverse(
      (schema, innerValue) => schema.get(name) ?? innerValue,
      schema,
    )
    if (type.is(value)) {
      return value
    }
    throw new Error(`Cannot read "${name}" metadata value from given schema`)
  }
}
