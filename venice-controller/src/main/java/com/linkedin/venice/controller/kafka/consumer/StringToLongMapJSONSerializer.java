package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.helix.VeniceJsonSerializer;
import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.type.TypeReference;


public class StringToLongMapJSONSerializer extends VeniceJsonSerializer<Map> {

  public StringToLongMapJSONSerializer() {
    super(Map.class);
  }

  @Override
  public Map<String, Long> deserialize(byte[] bytes, String path) throws IOException {
    return mapper.readValue(bytes, new TypeReference<Map<String, Long>>() {});
  }
}
