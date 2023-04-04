package com.linkedin.venice.controller.supersetschema;

import com.linkedin.venice.schema.SchemaEntry;
import java.util.Collection;
import org.apache.avro.Schema;


public interface SupersetSchemaGenerator {
  /**
   * Function to generate a superset based on all the input schemas.
   * @param schemas
   * @return {@link SchemaEntry} to contain the generated superset schema and the corresponding schema id.
   */
  SchemaEntry generateSupersetSchemaFromSchemas(Collection<SchemaEntry> schemas);

  /**
   * Compare whether two schemas are equal or not with the consideration of superset schema.
   */
  boolean compareSchema(Schema s1, Schema s2);

  /**
   * Generate the superset schema based on two schemas.
   * @param existingSchema : existing value schema or superset schema
   * @param newSchema : the new value schema.
   * @return
   */
  Schema generateSupersetSchema(Schema existingSchema, Schema newSchema);
}
