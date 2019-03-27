package com.linkedin.venice.helix;

import com.linkedin.venice.meta.StoreConfig;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


public class StoreConfigJsonSerializer extends VeniceJsonSerializer<StoreConfig> {
  public StoreConfigJsonSerializer() {
    super(StoreConfig.class);
    mapper.getDeserializationConfig().addMixInAnnotations(StoreConfig.class, StoreConfigSerializerMixin.class);
  }

  public static class StoreConfigSerializerMixin {
    @JsonCreator
    public StoreConfigSerializerMixin(@JsonProperty("storeName") String storeName) {
    }
  }
}
