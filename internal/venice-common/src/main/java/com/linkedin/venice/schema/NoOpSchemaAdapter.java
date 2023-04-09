package com.linkedin.venice.schema;

import org.apache.avro.Schema;


class NoOpSchemaAdapter implements SchemaAdapter {
  private static NoOpSchemaAdapter INSTANCE = new NoOpSchemaAdapter();

  private NoOpSchemaAdapter() {
  }

  static NoOpSchemaAdapter getInstance() {
    return INSTANCE;
  }

  @Override
  public Object adapt(Schema expectedSchema, Object datum) {
    return datum;
  }
}
