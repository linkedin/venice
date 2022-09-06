package com.linkedin.venice.schema.rmd;

import com.linkedin.venice.schema.GeneratedSchemaEntry;
import org.apache.avro.Schema;


/**
 * This holds the schema to perform conflict resolution on records when samza produces data in
 * active active replication mode.
 * More information can be found here {@link RmdSchemaGenerator} about the structure of the schema.
 */
public class RmdSchemaEntry extends GeneratedSchemaEntry {
  public RmdSchemaEntry(int valueSchemaId, int protocolVersion, String schemaStr) {
    super(valueSchemaId, protocolVersion, schemaStr);
  }

  public RmdSchemaEntry(int valueSchemaId, int protocolVersion, Schema schema) {
    super(valueSchemaId, protocolVersion, schema);
  }

  public RmdSchemaEntry(int valueSchemaId, int protocolVersion, byte[] bytes) {
    super(valueSchemaId, protocolVersion, bytes);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RmdSchemaEntry other = (RmdSchemaEntry) obj;
    if (this.valueSchemaID != other.valueSchemaID) {
      return false;
    }
    if (this.getId() != other.getId()) {
      return false;
    }
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + valueSchemaID;
    result = result * 31 + getId();
    result = result * 31 + super.hashCode();
    return result;
  }
}
