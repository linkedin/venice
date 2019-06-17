package com.linkedin.venice.schema;

import org.apache.avro.Schema;


/**
 * schemas are designed for write compute operations. The schema contains
 * available operations that SN could apply on top of the record. Derived
 * schemas can be generated automatically from value schemas and each value
 * schema can have multiple derived schemas.Check out
 * {@link com.linkedin.venice.schema.avro.WriteComputeSchemaAdapter} for all
 * available operations and how it gets generated.
 */
public class DerivedSchemaEntry extends SchemaEntry {
  private final int valueSchemaId;

  public DerivedSchemaEntry(int valueSchemaId, int id, String schemaStr) {
    super(id, schemaStr);
    this.valueSchemaId = valueSchemaId;
  }

  public DerivedSchemaEntry(int valueSchemaId, int id, Schema schema) {
    super(id, schema);
    this.valueSchemaId = valueSchemaId;
  }

  public DerivedSchemaEntry(int id, int valueSchemaId, byte[] bytes) {
    super(id, bytes);
    this.valueSchemaId = valueSchemaId;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return String.format("value schema id: %d\t schema id: %d\t schema: %s",
        valueSchemaId, getId(), getSchema().toString());
  }
}
