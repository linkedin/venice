package com.linkedin.venice.fastclient;

import java.util.HashMap;
import java.util.Map;


public enum KeyEncodingType {
  NONE(1, ""), BASE64(2, "b64");

  private static final Map<String, KeyEncodingType> ENUM_MAP;

  static {
    ENUM_MAP = new HashMap<>(2);
    for (KeyEncodingType value: values()) {
      ENUM_MAP.put(value.getValue(), value);
    }
  }

  private final String value;
  private final int code;

  KeyEncodingType(int code, String value) {
    this.value = value;
    this.code = code;
  }

  public String getValue() {
    return value;
  }
}
