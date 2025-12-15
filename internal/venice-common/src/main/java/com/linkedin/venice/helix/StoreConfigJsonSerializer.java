package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.meta.StoreConfig;


public class StoreConfigJsonSerializer extends VeniceJsonSerializer<StoreConfig> {
  public StoreConfigJsonSerializer() {
    super(StoreConfig.class);
  }

  @Override
  protected ObjectMapper createObjectMapper() {
    ObjectMapper mapper = super.createObjectMapper();
    mapper.addMixIn(StoreConfig.class, StoreConfigSerializerMixin.class);
    return mapper;
  }

  public static class StoreConfigSerializerMixin {
    @JsonCreator
    public StoreConfigSerializerMixin(
        @JsonProperty("storeName") String storeName,
        @JsonProperty("deleting") boolean deleting,
        @JsonProperty("cluster") String cluster,
        @JsonProperty("migrationSrcCluster") String migrationSrcCluster,
        @JsonProperty("migrationDestCluster") String migrationDestCluster) {
    }
  }
}
