package com.linkedin.venice.common;

import java.io.IOException;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * This class defines the serialization/deserialization functions for partition-offset map returned by storage node.
 */
public class PartitionOffsetMapUtils {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static String serializedPartitionOffsetMap(Map<Integer, Long> partitionOffsetMap) throws IOException {
    return OBJECT_MAPPER.writeValueAsString(partitionOffsetMap);
  }

  public static Map<Integer, Long> deserializePartitionOffsetMap(String content) throws IOException {
    return OBJECT_MAPPER.readValue(content, new TypeReference<Map<Integer, Long>>(){});
  }
}
