package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.JsonNode;
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
      String.format("{\"%s\":[\"store1\",\"store2\"]}", ConfigKeys.STORES_TO_REPLICATE);

  @Test
  public void deserializesAsJson() throws IOException {
    DarkClusterConfig config = OBJECT_MAPPER.readValue(SERIALIZED_CONFIG, DarkClusterConfig.class);
    Set<String> expectedStores = new HashSet<>(Arrays.asList("store1", "store2"));
    Assert.assertEquals(config.getStoresToReplicate(), expectedStores);
  }

  @Test
  public void serializesAsJson() throws IOException {
    DarkClusterConfig config = new DarkClusterConfig();
    config.setStoresToReplicate(Arrays.asList("store1", "store2"));

    String serializedTestObj = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    JsonNode expected = OBJECT_MAPPER.readTree(SERIALIZED_CONFIG);
    JsonNode actual = OBJECT_MAPPER.readTree(serializedTestObj);

    Assert.assertEquals(actual, expected);
  }
}
