package com.linkedin.venice.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ObjectMapperFactory {
  private static final ObjectMapper INSTANCE =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);

  private ObjectMapperFactory() {
  }

  public static ObjectMapper getInstance() {
    return INSTANCE;
  }
}
