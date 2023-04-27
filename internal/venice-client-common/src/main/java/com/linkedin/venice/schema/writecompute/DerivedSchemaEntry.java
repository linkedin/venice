package com.linkedin.venice.schema.writecompute;

import com.linkedin.venice.schema.GeneratedSchemaEntry;
import org.apache.avro.Schema;


/**
 * Derived schemas are designed for write compute operations. The schema contains
 * available operations that SN could apply on top of the record. Derived
 * schemas can be generated automatically from value schemas and each value
 * schema can have multiple derived schemas.Check out
 * {@link WriteComputeSchemaConverter} for all
 * available operations and how it gets generated.
 */
public class DerivedSchemaEntry extends GeneratedSchemaEntry {
  public DerivedSchemaEntry(int valueSchemaId, int protocolVersion, String schemaStr) {
    super(valueSchemaId, protocolVersion, schemaStr);
  }

  public DerivedSchemaEntry(int valueSchemaId, int protocolVersion, Schema schema) {
    super(valueSchemaId, protocolVersion, schema);
  }

  public DerivedSchemaEntry(int valueSchemaId, int protocolVersion, byte[] bytes) {
    super(valueSchemaId, protocolVersion, bytes);
  }
}
