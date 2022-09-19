package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.meta.StoreConfig;


public class StoreConfigJsonSerializer extends VeniceJsonSerializer<StoreConfig> {
  public StoreConfigJsonSerializer() {
    super(StoreConfig.class);
    OBJECT_MAPPER.addMixIn(StoreConfig.class, StoreConfigSerializerMixin.class);
  }

  public static class StoreConfigSerializerMixin {
    @JsonCreator
    public StoreConfigSerializerMixin(@JsonProperty("storeName") String storeName) {
    }
  }
}
