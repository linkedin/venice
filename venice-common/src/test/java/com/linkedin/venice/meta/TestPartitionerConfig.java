package com.linkedin.venice.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPartitionerConfig {
  static org.codehaus.jackson.map.ObjectMapper codehouseMapper = new org.codehaus.jackson.map.ObjectMapper();
  static final String serialized = "{\"partitionerParams\":{\"majorField\" : \"jobId\"}, \"partitionerClass\": \"com.linkedin.venice.partitioner.DefaultVenicePartitioner\", \"amplificationFactor\":10}";

  /**
   * This test verifies that we can deserialize existing {@link PartitionerConfig} objects. If we add a field then this
   * test must still pass even without adding the field to the serialized string (because we might need to deserialize
   * old objects).
   * @throws IOException
   */
  @Test
  public void deserializes() throws IOException {
    Map<String, String> testPartitionParams = new HashMap<>();
    testPartitionParams.put("majorField", "jobId");

    PartitionerConfig codehouse = codehouseMapper.readValue(serialized, PartitionerConfig.class);
    Assert.assertEquals(codehouse.getPartitionerParams(), testPartitionParams);
    Assert.assertEquals(codehouse.getPartitionerClass(), "com.linkedin.venice.partitioner.DefaultVenicePartitioner");
    Assert.assertEquals(codehouse.getAmplificationFactor(), 10);
  }
}

