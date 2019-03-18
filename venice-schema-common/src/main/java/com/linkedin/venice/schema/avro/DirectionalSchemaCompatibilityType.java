package com.linkedin.venice.schema.avro;

public enum DirectionalSchemaCompatibilityType {
  /**
   * Both backward and forward compatible.
   *
   * The following changes are fully compatible:
   *
   * - Adding a new field with a default value. If having a default value seems cumbersome, remember
   *   that the new field can be made optional (with a type made up of a union of "null" and another
   *   type, e.g.: "type": ["null", "int"]) and the default value can be set to "null".
   * - Removing an existing field which previously had a default value.
   * - Renaming a field and keeping the old name as an alias.
   * - Re-ordering fields.
   */
  FULL,

  /**
   * Clients using the new schema should be able to read data written with the old schema.
   *
   * The following changes are backward compatible only:
   *
   * - Adding a value to an enum.
   * - Changing the type of a field to another type which the original can be promoted to:
   *   - int is promotable to long, float, or double
   *   - long is promotable to float or double
   *   - float is promotable to double
   *   - string is promotable to bytes
   *   - bytes is promotable to string
   * - Removing a field which did not previously contain a default value.
   */
  BACKWARD,

  /**
   * Clients using the old schema should be able to read data written with the new schema.
   *
   * The following changes are forward compatible only:
   *
   * - Removing a value from an enum.
   * - Changing the type of a field to another type which the original can be promoted from (the inverse of the
   *   type promotion mappings mentioned in {@link #BACKWARD}.
   * - Adding a new mandatory field with no default value.
   */
  FORWARD,

  /**
   * Anything goes.
   *
   * The following changes are NOT compatible:
   *
   * - Changing the type of a field to another which does not support promotion (e.g.: int to record, etc.).
   * - Changing field names without keeping the old name as an alias.
   * - Making "structural changes" to the nested parts of your schema. For example, moving a field from
   *   `value.nestedRecord.fieldX` to `value.fieldX`. TODO: Confirm whether this is really incompatible
   */
  NONE;
}
