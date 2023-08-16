package com.linkedin.venice.client.schema;

import org.apache.avro.Schema;


/**
 * Used to cache the toString of a given schema, since it is expensive to compute.
 */
public class SchemaAndToString {
  private final Schema schema;
  private final String toString;

  public SchemaAndToString(Schema schema) {
    this.schema = schema;
    this.toString = schema.toString();
  }

  public Schema getSchema() {
    return schema;
  }

  public String getToString() {
    return toString;
  }
}
