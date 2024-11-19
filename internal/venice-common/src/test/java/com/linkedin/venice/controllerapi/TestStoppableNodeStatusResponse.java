package com.linkedin.venice.controllerapi;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestStoppableNodeStatusResponse {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static class StoppableNodeStatusResponseForTest {
    private List<String> stoppableInstances;
    private Map<String, String> nonStoppableInstancesWithReasons;

    public Map<String, String> getNonStoppableInstancesWithReasons() {
      return nonStoppableInstancesWithReasons;
    }

    public void setNonStoppableInstancesWithReason(Map<String, String> nonStoppableInstancesWithReasons) {
      this.nonStoppableInstancesWithReasons = nonStoppableInstancesWithReasons;
    }

    public List<String> getStoppableInstances() {
      return stoppableInstances;
    }

    public void setStoppableInstances(List<String> removableInstances) {
      this.stoppableInstances = removableInstances;
    }
  }

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
    StoppableNodeStatusResponseForTest deserializedResponse =
        OBJECT_MAPPER.readValue(serializedResponse, StoppableNodeStatusResponseForTest.class);

    assertEquals(deserializedResponse.stoppableInstances, response.getStoppableInstances());
    assertEquals(deserializedResponse.nonStoppableInstancesWithReasons, response.getNonStoppableInstancesWithReasons());
  }

  @Test
  public void testJsonSerializationForEmptyContent() throws JsonProcessingException {
    StoppableNodeStatusResponse response = new StoppableNodeStatusResponse();

    String serializedResponse = OBJECT_MAPPER.writeValueAsString(response);
    StoppableNodeStatusResponseForTest deserializedResponse =
        OBJECT_MAPPER.readValue(serializedResponse, StoppableNodeStatusResponseForTest.class);

    assertEquals(deserializedResponse.stoppableInstances, Collections.emptyList());
    assertEquals(deserializedResponse.nonStoppableInstancesWithReasons, Collections.emptyMap());
  }
}
