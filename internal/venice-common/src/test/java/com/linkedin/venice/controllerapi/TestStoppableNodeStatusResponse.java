package com.linkedin.venice.controllerapi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Arrays;
import java.util.HashMap;
import org.testng.annotations.Test;


public class TestStoppableNodeStatusResponse {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void testJsonSerialization() throws JsonProcessingException {
    StoppableNodeStatusResponse response = new StoppableNodeStatusResponse();
    response.setStoppableInstances(Arrays.asList("instance1", "instance2"));
    response.setNonStoppableInstancesWithReason(new HashMap<String, String>() {
      {
        put("instance3", "reason1");
        put("instance4", "reason2");
      }
    });

    String serializedResponse = OBJECT_MAPPER.writeValueAsString(response);
    JsonNode deserializedResponse = OBJECT_MAPPER.readTree(serializedResponse);

    assertTrue(deserializedResponse.get("stoppableInstances") instanceof ArrayNode);
    assertEquals(deserializedResponse.get("stoppableInstances").get(0).asText(), "instance1");
    assertEquals(deserializedResponse.get("stoppableInstances").get(1).asText(), "instance2");
    assertTrue(deserializedResponse.get("nonStoppableInstancesWithReasons") instanceof ObjectNode);
    assertEquals(deserializedResponse.get("nonStoppableInstancesWithReasons").get("instance3").asText(), "reason1");
    assertEquals(deserializedResponse.get("nonStoppableInstancesWithReasons").get("instance4").asText(), "reason2");
  }

  @Test
  public void testJsonSerializationForEmptyContent() throws JsonProcessingException {
    StoppableNodeStatusResponse response = new StoppableNodeStatusResponse();

    String serializedResponse = OBJECT_MAPPER.writeValueAsString(response);
    JsonNode deserializedResponse = OBJECT_MAPPER.readTree(serializedResponse);

    assertTrue(deserializedResponse.get("stoppableInstances").isEmpty());
    assertTrue(deserializedResponse.get("nonStoppableInstancesWithReasons").isEmpty());
  }
}
