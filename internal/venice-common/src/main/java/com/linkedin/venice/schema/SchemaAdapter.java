package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;


/**
 * Try to adapt the {@param datum} to the {@param expectedSchema}.
 * The following steps are followed:
 * 1. If the schema type doesn't allow for adaptation, return the same value that was passed in input.
 * 2. If the schema type allows for adaptation, then
 *     2a. If the value doesn't specify a value for any field, the default value is used
 *     2b. If a field is mandatory, but no default values are specified, then an Exception is thrown
 *
 * @param expectedSchema The type {@link Schema} that the value needs to be adapted to.
 * @param datum The value that needs to be adapted to the specified schema
 * @return
 * @throws VeniceException
 */
public interface SchemaAdapter {
  Object adapt(Schema expectedSchema, Object datum);

  /**
   * Checks if it is possible for some value to be modified to adapt to the provided schema type.
   * @param expectedSchemaType The type {@link Schema.Type} that the value needs to be adapted to.
   * @return {@code true} if a value can be modified to adapt to the provided schema type; {@code false} otherwise.
   */
  static SchemaAdapter getSchemaAdapter(Schema.Type expectedSchemaType) {
    switch (expectedSchemaType) {
      case ENUM:
      case FIXED:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
        return NoOpSchemaAdapter.getInstance();
      case RECORD:
        return RecordSchemaAdapter.getInstance();
      case ARRAY:
        return ListSchemaAdapter.getInstance();
      case MAP:
        return MapSchemaAdapter.getInstance();
      case UNION:
        return UnionSchemaAdapter.getInstance();
      default:
        // Defensive coding
        throw new VeniceException("Unhandled Avro type. This is unexpected.");
    }
  }

  /**
   * Checks if it is possible for some value to be modified to adapt to the provided schema type.
   * @param expectedSchemaType The type {@link Schema.Type} that the value needs to be adapted to.
   * @return {@code true} if a value can be modified to adapt to the provided schema type; {@code false} otherwise.
   */
  static Object adaptToSchema(Schema expectedSchema, Object datum) {
    return SchemaAdapter.getSchemaAdapter(expectedSchema.getType()).adapt(expectedSchema, datum);
  }
}
