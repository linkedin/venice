package com.linkedin.venice.etl;

import java.util.List;
import org.apache.avro.Schema;


/**
 * In ETL jobs, when a "delete" record is seen, "value" is set to "null" and "DELETED_TS" is set to the offset where
 * this delete record was seen. To allow the value field to be set as "null", the schema may need to be modified if it
 * doesn't already allow for a "null" value. For "union" schemas, "null" must be added to the union if it is not already
 * one of the allowed types. For all other avro types, the value schema is set to a union schema of "null" and the
 * original value schema.
 */
public enum ETLValueSchemaTransformation {
  NONE, // When value schema in ETL is the same as the store value schema, e.g. ["int", "string", "null"]
  ADD_NULL_TO_UNION, // When "null" should be added to the union in ETL value schema, e.g. ["int", "string"]
  UNIONIZE_WITH_NULL; // When the store value schema is not a union and it will be changed to a union with null, e.g. a
                      // record schema

  public static ETLValueSchemaTransformation fromSchema(Schema schema) {
    // Since nested unions are not allowed, a null type is added to the union if it doesn't already exist
    if (schema.getType().equals(Schema.Type.UNION)) {
      List<Schema> schemasInUnion = schema.getTypes();

      if (schemasInUnion.stream().map(Schema::getType).noneMatch(type -> type.equals(Schema.Type.NULL))) {
        return ADD_NULL_TO_UNION;
      } else {
        return NONE;
      }
    } else {
      return UNIONIZE_WITH_NULL;
    }
  }
}
