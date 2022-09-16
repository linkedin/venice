package com.linkedin.venice.controller.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


public enum SchemaType {
  AVRO_1_4(0);

  private final int value;
  private static final Map<Integer, SchemaType> SCHEMA_TYPE_MAP = getSchemaTypeMap();

  SchemaType(int value) {
    this.value = value;
  }

  private static Map<Integer, SchemaType> getSchemaTypeMap() {
    Map<Integer, SchemaType> intToTypeMap = new HashMap<>();
    for (SchemaType type: SchemaType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static SchemaType valueOf(int value) {
    SchemaType type = SCHEMA_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid schema type: " + value);
    }
    return type;
  }

  public int getValue() {
    return value;
  }

}
