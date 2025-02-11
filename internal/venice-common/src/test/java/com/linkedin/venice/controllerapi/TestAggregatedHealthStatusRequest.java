package com.linkedin.venice.controllerapi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAggregatedHealthStatusRequest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  @Test
  public void testDeserializationWithAllFields() throws JsonProcessingException {
    String json =
        "{\"cluster_id\":\"cluster1\",\"instances\":[\"instance1\",\"instance2\"],\"to_be_stopped_instances\":[\"instance3\",\"instance4\"]}";
    AggregatedHealthStatusRequest request = OBJECT_MAPPER.readValue(json, AggregatedHealthStatusRequest.class);

    assertEquals(request.getClusterId(), "cluster1");
    assertEquals(request.getInstances().size(), 2);
    assertEquals(request.getInstances().get(0), "instance1");
    assertEquals(request.getInstances().get(1), "instance2");
    assertEquals(request.getToBeStoppedInstances().size(), 2);
    assertEquals(request.getToBeStoppedInstances().get(0), "instance3");
    assertEquals(request.getToBeStoppedInstances().get(1), "instance4");
  }

  @Test
  public void testDeserializationWithMandatoryFields() throws JsonProcessingException {
    String json = "{\"cluster_id\":\"cluster1\",\"instances\":[\"instance1\",\"instance2\"]}";
    AggregatedHealthStatusRequest request = OBJECT_MAPPER.readValue(json, AggregatedHealthStatusRequest.class);

    assertEquals(request.getClusterId(), "cluster1");
    assertEquals(request.getInstances().size(), 2);
    assertEquals(request.getInstances().get(0), "instance1");
    assertEquals(request.getInstances().get(1), "instance2");
    assertNotNull(request.getToBeStoppedInstances());
    assertTrue(request.getToBeStoppedInstances().isEmpty());
  }

  @Test
  public void testDeserializationWithMissingMandatoryFields() {
    String json = "{\"instances\":[\"instance1\",\"instance2\"]}";
    ValueInstantiationException e = Assert.expectThrows(
        ValueInstantiationException.class,
        () -> OBJECT_MAPPER.readValue(json, AggregatedHealthStatusRequest.class));
    assertTrue(e.getMessage().contains("'cluster_id' is required"), e.getMessage());

    String json2 = "{\"cluster_id\":\"cluster1\"}";
    ValueInstantiationException e2 = Assert.expectThrows(
        ValueInstantiationException.class,
        () -> OBJECT_MAPPER.readValue(json2, AggregatedHealthStatusRequest.class));
    assertTrue(e2.getMessage().contains("'instances' is required"), e2.getMessage());
  }
}
