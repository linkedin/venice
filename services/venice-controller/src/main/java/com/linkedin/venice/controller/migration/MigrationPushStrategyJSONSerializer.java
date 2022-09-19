package com.linkedin.venice.controller.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import java.io.IOException;
import java.util.Map;


public class MigrationPushStrategyJSONSerializer extends VeniceJsonSerializer<Map> {
  public MigrationPushStrategyJSONSerializer() {
    super(Map.class);
  }

  @Override
  public Map<String, String> deserialize(byte[] bytes, String path) throws IOException {
    return OBJECT_MAPPER.readValue(bytes, new TypeReference<Map<String, String>>() {
    });
  }
}
