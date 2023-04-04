package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


class UnionSchemaAdapter implements SchemaAdapter {
  private static UnionSchemaAdapter INSTANCE = new UnionSchemaAdapter();

  private UnionSchemaAdapter() {
  }

  static UnionSchemaAdapter getInstance() {
    return INSTANCE;
  }

  @Override
  public Object adapt(Schema expectedSchema, Object datum) {
    int index;
    try {
      index = GenericData.get().resolveUnion(expectedSchema, datum);
    } catch (Exception e) {
      throw new VeniceException(e);
    }
    return SchemaAdapter.adaptToSchema(expectedSchema.getTypes().get(index), datum);
  }
}
