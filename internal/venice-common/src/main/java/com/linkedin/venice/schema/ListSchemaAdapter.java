package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


class ListSchemaAdapter implements SchemaAdapter {
  private static ListSchemaAdapter INSTANCE = new ListSchemaAdapter();

  private ListSchemaAdapter() {
  }

  static ListSchemaAdapter getInstance() {
    return INSTANCE;
  }

  @Override
  public Collection<?> adapt(Schema expectedSchema, Object datum) {
    if (!(datum instanceof Collection)) {
      throw new VeniceException("Expected datum to be a Collection");
    }

    Collection<?> datumCollection = (Collection<?>) datum;

    // Adapt List type - adapt all elements
    Schema elementSchema = expectedSchema.getElementType();
    if (SchemaAdapter.getSchemaAdapter(elementSchema.getType()) instanceof NoOpSchemaAdapter) {
      return datumCollection;
    } else {
      return datumCollection.stream()
          .map(element -> SchemaAdapter.adaptToSchema(elementSchema, element))
          .collect(Collectors.toList());
    }
  }
}
