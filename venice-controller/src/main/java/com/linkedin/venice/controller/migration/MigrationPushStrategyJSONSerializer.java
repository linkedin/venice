package com.linkedin.venice.controller.migration;

import com.linkedin.venice.helix.VeniceJsonSerializer;
import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.type.TypeReference;


public class MigrationPushStrategyJSONSerializer extends VeniceJsonSerializer<Map> {
  public MigrationPushStrategyJSONSerializer() {
    super(Map.class);
  }

  @Override
  public Map<String, String> deserialize(byte[] bytes, String path) throws IOException {
    return mapper.readValue(bytes, new TypeReference<Map<String, String>>() {});
  }
}
