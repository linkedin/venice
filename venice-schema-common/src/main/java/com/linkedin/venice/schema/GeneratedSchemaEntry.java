package com.linkedin.venice.schema;

import org.apache.avro.Schema;


/**
 * This is an abstract class which represents a generic class associating a generated schema id with it's
 * corresponding value schema id
 */
public class GeneratedSchemaEntry extends SchemaEntry {
  protected final int valueSchemaId;

  public GeneratedSchemaEntry(int valueSchemaId, int protocolVersion, String schemaStr) {
    super(protocolVersion, schemaStr);
    this.valueSchemaId = valueSchemaId;
  }

  public GeneratedSchemaEntry(int valueSchemaId, int protocolVersion, Schema schema) {
    super(protocolVersion, schema);
    this.valueSchemaId = valueSchemaId;
  }

  public GeneratedSchemaEntry(int valueSchemaId, int protocolVersion, byte[] bytes) {
    super(protocolVersion, bytes);
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
