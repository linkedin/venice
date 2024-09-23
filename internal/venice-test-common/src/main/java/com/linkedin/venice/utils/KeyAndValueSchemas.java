package com.linkedin.venice.utils;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;

import org.apache.avro.Schema;


public class KeyAndValueSchemas {
  private final Schema key, value;

  public KeyAndValueSchemas(Schema key, Schema value) {
    this.key = key;
    this.value = value;
  }

  public KeyAndValueSchemas(Schema keyAndValueNestedInsideRecord) {
    this(
        keyAndValueNestedInsideRecord.getField(DEFAULT_KEY_FIELD_PROP).schema(),
        keyAndValueNestedInsideRecord.getField(DEFAULT_VALUE_FIELD_PROP).schema());
  }

  public Schema getKey() {
    return key;
  }

  public Schema getValue() {
    return value;
  }
}
