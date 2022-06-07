package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHybridStoreConfig {
  static ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
  static final String serialized = "{\"rewindTimeInSeconds\":123,\"offsetLagThresholdToGoOnline\":2500}";

  /**
   * This test verifies that we can deserialize existing {@link HybridStoreConfig} objects. If we add a field then this
   * test must still pass even without adding the field to the serialized string (because we might need to deserialize
   * old objects).
   * @throws IOException
   */
  @Test
  public void deserializes() throws IOException {
    HybridStoreConfig fasterXml = objectMapper.readValue(serialized, HybridStoreConfig.class);
    Assert.assertEquals(fasterXml.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(fasterXml.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);
  }
}
