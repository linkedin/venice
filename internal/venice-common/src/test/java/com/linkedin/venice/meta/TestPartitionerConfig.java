package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPartitionerConfig {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String SERIALIZED_CONFIG =
      "{\"partitionerParams\":{\"majorField\" : \"jobId\"}, \"partitionerClass\": \"com.linkedin.venice.partitioner.DefaultVenicePartitioner\", \"amplificationFactor\":10}";

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

    PartitionerConfig partitionerConfig = OBJECT_MAPPER.readValue(SERIALIZED_CONFIG, PartitionerConfig.class);
    Assert.assertEquals(partitionerConfig.getPartitionerParams(), testPartitionParams);
    Assert.assertEquals(
        partitionerConfig.getPartitionerClass(),
        "com.linkedin.venice.partitioner.DefaultVenicePartitioner");
    Assert.assertEquals(partitionerConfig.getAmplificationFactor(), 10);
  }
}
