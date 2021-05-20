package com.linkedin.venice.meta;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHybridStoreConfig {
  //TODO, converge on fasterxml or codehouse
  static com.fasterxml.jackson.databind.ObjectMapper fasterXmlMapper = new com.fasterxml.jackson.databind.ObjectMapper();
  static org.codehaus.jackson.map.ObjectMapper codehouseMapper = new org.codehaus.jackson.map.ObjectMapper();
  static final String serialized = "{\"rewindTimeInSeconds\":123,\"offsetLagThresholdToGoOnline\":2500}";

  /**
   * This test verifies that we can deserialize existing {@link HybridStoreConfig} objects. If we add a field then this
   * test must still pass even without adding the field to the serialized string (because we might need to deserialize
   * old objects).
   * @throws IOException
   */
  @Test
  public void deserializes() throws IOException {
    HybridStoreConfig fasterXml = fasterXmlMapper.readValue(serialized, HybridStoreConfig.class);
    Assert.assertEquals(fasterXml.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(fasterXml.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);

    HybridStoreConfig codehouse = codehouseMapper.readValue(serialized, HybridStoreConfig.class);
    Assert.assertEquals(codehouse.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(fasterXml.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);
  }
}
