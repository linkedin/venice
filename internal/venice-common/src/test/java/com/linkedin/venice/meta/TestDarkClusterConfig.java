package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDarkClusterConfig {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String SERIALIZED_CONFIG =
      String.format("{\"%s\":[\"store1\",\"store2\"]}", ConfigKeys.DARK_CLUSTER_TARGET_STORES);

  @Test
  public void deserializesAsJson() throws IOException {
    DarkClusterConfig config = OBJECT_MAPPER.readValue(SERIALIZED_CONFIG, DarkClusterConfig.class);
    Set<String> expectedStores = new HashSet<>(Arrays.asList("store1", "store2"));
    Assert.assertEquals(config.getTargetStores(), expectedStores);
  }

  @Test
  public void serializesAsJson() throws IOException {
    DarkClusterConfig config = new DarkClusterConfig();
    config.setTargetStores(Arrays.asList("store1", "store2"));

    String serializedTestObj = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    Assert.assertEquals(OBJECT_MAPPER.readTree(serializedTestObj), OBJECT_MAPPER.readTree(SERIALIZED_CONFIG));
  }
}
