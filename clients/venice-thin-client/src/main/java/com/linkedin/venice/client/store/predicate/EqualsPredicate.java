package com.linkedin.venice.client.store.predicate;

import java.util.Objects;
import org.apache.avro.Schema;


public class EqualsPredicate<T> implements Predicate<T> {
  private final T expectedValue;

  EqualsPredicate(T expectedValue) {
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(T value) {
    return Objects.equals(this.expectedValue, value);
  }

  @Override
  public boolean isCompatibleWithSchema(Schema schema) {
    if (expectedValue == null) {
      return true; // null values are compatible with any schema
    }

    Schema.Type avroType = schema.getType();

    if (expectedValue instanceof Integer) {
      return avroType == Schema.Type.INT;
    } else if (expectedValue instanceof Long) {
      return avroType == Schema.Type.LONG;
    } else if (expectedValue instanceof Float) {
      return avroType == Schema.Type.FLOAT;
    } else if (expectedValue instanceof Double) {
      return avroType == Schema.Type.DOUBLE;
    } else if (expectedValue instanceof String) {
      return avroType == Schema.Type.STRING;
    }

    return true; // For other types, assume compatibility
  }

  @Override
  public String toString() {
    return "EqualsPredicate{expectedValue=" + expectedValue + "}";
  }
}
