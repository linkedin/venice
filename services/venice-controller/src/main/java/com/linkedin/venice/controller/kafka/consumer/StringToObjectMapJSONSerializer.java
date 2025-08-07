package com.linkedin.venice.controller.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * JSON serializer for Map<String, Object> that can handle mixed types including
 * Long values and Position objects.
 */
public class StringToObjectMapJSONSerializer extends VeniceJsonSerializer<Map> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public StringToObjectMapJSONSerializer() {
    super(Map.class);
  }

  @Override
  public Map<String, Object> deserialize(byte[] bytes, String path) throws IOException {
    // First parse as JsonNode to inspect the structure
    JsonNode rootNode = OBJECT_MAPPER.readTree(bytes);
    Map<String, Object> result = new HashMap<>();

    rootNode.fields().forEachRemaining(entry -> {
      String key = entry.getKey();
      JsonNode value = entry.getValue();

      // Check if this is a Position object by looking at the key name and structure
      if ((key.equals(AdminTopicMetadataAccessor.POSITION_KEY)
          || key.equals(AdminTopicMetadataAccessor.UPSTREAM_POSITION_KEY)) && value.isObject() && value.has("typeId")
          && value.has("positionBytes")) {
        // Deserialize as Position object
        try {
          AdminTopicMetadataAccessor.Position position =
              OBJECT_MAPPER.treeToValue(value, AdminTopicMetadataAccessor.Position.class);
          result.put(key, position);
        } catch (Exception e) {
          throw new VeniceException("Failed to deserialize field: " + key, e);
        }
      } else if (value.isNumber()) {
        // Deserialize as Long
        result.put(key, value.asLong());
      } else {
        throw new VeniceException("Unexpected type of field key:" + key + " value:" + value);
      }
    });

    return result;
  }
}
