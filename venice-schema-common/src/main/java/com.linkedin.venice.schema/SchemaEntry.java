package com.linkedin.venice.schema;

import com.linkedin.venice.schema.avro.SchemaCompatibility;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.log4j.Logger;

/**
 * {@link SchemaEntry} is composed of a schema and its corresponding id.
 * Internally, this class will maintain a parsed {@link org.apache.avro.Schema}
 */
public final class SchemaEntry {
  private final int id;
  private Schema schema;
  private Logger logger = Logger.getLogger(getClass());

  /**
   * Primary constructor taking a literal id and schema.
   */
  public SchemaEntry(int id, String schemaStr) {
    if (null == schemaStr) {
      throw new IllegalArgumentException("The schemaStr parameter cannot be null!");
    }

    this.id = id;
    try {
      this.schema = Schema.parse(schemaStr);
    } catch (Exception e) {
      logger.error("Failed to parse schema: " + schemaStr);
      throw new SchemaParseException(e);
    }
  }

  public SchemaEntry(int id, Schema schema) {
    if (null == schema) {
      throw new IllegalArgumentException("The schema parameter cannot be null!");
    }

    this.id = id;
    this.schema = schema;
  }

  public SchemaEntry(int id, byte[] bytes) {
    this(id, new String(bytes));
  }

  /** @return the id */
  public int getId() {
    return id;
  }

  public Schema getSchema() {
    return schema;
  }

  /**
   * Schema.hashCode only generates hash code based on name/value, but not consider doc/order/...,
   * which is not expected.
   * Fow now, we are using the formalized schema serialized string to generate hash code to cover
   * all the fields
   *
   * @return
   */
  @Override
  public int hashCode() {
    return schema.toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SchemaEntry other = (SchemaEntry) obj;
    return schema.toString().equals(other.getSchema().toString());
  }

  public SchemaEntry clone() {
    return new SchemaEntry(id, schema.toString());
  }

  @Override
  public String toString() {
    return id + "\t" + schema.toString();
  }

  public byte[] getSchemaBytes() {
    return schema.toString().getBytes();
  }

  public boolean isCompatible(SchemaEntry otherSchemaEntry) {
    return isFullyCompatible(otherSchemaEntry);
  }

  /**
   * This function will check whether two schema are both backward and forward compatible.
   *
   * Right now, this function is using the util function provided by avro-1.7+ to check compatibility.
   * We need to remove the util class manually copied when venice is able to use avro-1.7+
   *
   * @param otherSchemaEntry
   * @return
   */
  private boolean isFullyCompatible(SchemaEntry otherSchemaEntry) {
    SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(schema, otherSchemaEntry.getSchema());
    if (backwardCompatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
      logger.info("Schema: " + this + " is not compatible with Schema: " + otherSchemaEntry +
          ", msg: " + backwardCompatibility.getDescription());
      return false;
    }

    SchemaCompatibility.SchemaPairCompatibility forwardCompatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(otherSchemaEntry.getSchema(), schema);
    if (forwardCompatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
      logger.info("Schema: " + this + " is not compatible with Schema: " + otherSchemaEntry +
          ", msg: " + forwardCompatibility.getDescription());
      return false;
    }

    return true;
  }
}

