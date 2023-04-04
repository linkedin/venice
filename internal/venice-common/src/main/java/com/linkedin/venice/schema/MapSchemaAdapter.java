package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


class MapSchemaAdapter implements SchemaAdapter {
  private static MapSchemaAdapter INSTANCE = new MapSchemaAdapter();

  private MapSchemaAdapter() {
  }

  static MapSchemaAdapter getInstance() {
    return INSTANCE;
  }

  @Override
  public Map<String, ?> adapt(Schema expectedSchema, Object datum) {
    if (!(datum instanceof Map)) {
      throw new VeniceException("Expected datum to be of Map type");
    }

    // Adapt Map type - adapt all entries
    Map<String, ?> datumMap = (Map<String, ?>) datum;
    Schema elementSchema = expectedSchema.getValueType();
    if (SchemaAdapter.getSchemaAdapter(elementSchema.getType()) instanceof NoOpSchemaAdapter) {
      return datumMap;
    } else {
      return datumMap.entrySet()
          .stream()
          .collect(
              Collectors
                  .toMap(Map.Entry::getKey, entry -> SchemaAdapter.adaptToSchema(elementSchema, entry.getValue())));
    }
  }
}
