package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;


/**
 * an abstraction of backend parser. It reads intermediate Object and parses it to schema.
 * It also contains a VsonReader reference so it could read from string straightly.
 */
@Deprecated
public abstract class AbstractVsonSchemaAdapter<T> {
  private Object schemaObject;

  AbstractVsonSchemaAdapter(String vsonSchemaStr) {
    parse(vsonSchemaStr);
  }

  private void parse(String vsonSchemaStr) {
    if (vsonSchemaStr == null)
      throw new IllegalArgumentException("Vson schema string cannot be null!");
    VsonReader reader = new VsonReader(new StringReader(vsonSchemaStr));
    schemaObject = reader.read();
  }

  T fromVsonObjects() {
    return fromVsonObjects(schemaObject);
  }

  T fromVsonObjects(Object schemaObject) {
    if (schemaObject == null) {
      throw new VsonSerializationException("Schema object can't be null");
    }

    if (schemaObject instanceof Map) {
      return readMap((Map<String, Object>) schemaObject);
    } else if (schemaObject instanceof List) {
      if (((List) schemaObject).size() != 1)
        throw new VsonSerializationException("List type must have a single entry specifying entry type.");
      return readList((List<T>) schemaObject);
    } else if (schemaObject instanceof String) {
      return readPrimitive((String) schemaObject);
    } else {
      throw new VsonSerializationException(
          "SchemaObject is not a string, an array, or an object, " + "so it is not valid in a type definition.");
    }
  }

  abstract T readMap(Map<String, Object> schemaObject);

  abstract T readList(List<T> schemaObject);

  abstract T readPrimitive(String schemaObject);
}
