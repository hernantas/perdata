import { ArraySchema, NullableSchema, OptionalSchema, Schema } from 'pertype'

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
}
