package com.linkedin.venice.controller.supersetschema;

import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.Collection;
import org.apache.avro.Schema;


public class DefaultSupersetSchemaGenerator implements SupersetSchemaGenerator {
  @Override
  public SchemaEntry generateSupersetSchemaFromSchemas(Collection<SchemaEntry> schemas) {
    return AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(schemas);
  }

  @Override
  public boolean compareSchema(Schema s1, Schema s2) {
    return AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2);
  }

  @Override
  public Schema generateSupersetSchema(Schema existingSchema, Schema newSchema) {
    return AvroSupersetSchemaUtils.generateSupersetSchema(existingSchema, newSchema);
  }
}
