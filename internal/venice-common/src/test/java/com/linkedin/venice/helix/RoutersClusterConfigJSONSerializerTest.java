package com.linkedin.venice.helix;

import com.linkedin.venice.meta.RoutersClusterConfig;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoutersClusterConfigJSONSerializerTest {
  @Test
  public void testSerializeAndDeserialize() throws IOException {
    RoutersClusterConfig config = new RoutersClusterConfig();
    config.setExpectedRouterCount(10);

    RouterClusterConfigJSONSerializer serializer = new RouterClusterConfigJSONSerializer();
    byte[] data = serializer.serialize(config, "");
    RoutersClusterConfig newConfig = serializer.deserialize(data, "");
    Assert.assertEquals(config, newConfig);
  }

  @Test
  public void testDeserializeWithMissingFields() throws IOException {
    String jsonStr = "{\"expectedRouterCount\":10}";
    RouterClusterConfigJSONSerializer serializer = new RouterClusterConfigJSONSerializer();
    RoutersClusterConfig config = serializer.deserialize(jsonStr.getBytes(), "");
    Assert.assertEquals(config.getExpectedRouterCount(), 10);
    Assert.assertTrue(
        config.isMaxCapacityProtectionEnabled() && config.isThrottlingEnabled(),
        "By default all feature should be enabled.");
  }
}
