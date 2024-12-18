package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VenicePropertiesTest {
  @Test
  public void testConvertSizeFromLiteral() {
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("512"), 512l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1KB"), 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1k"), 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1MB"), 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1m"), 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1GB"), 1024 * 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1g"), 1024 * 1024 * 1024l);
  }

  @Test
  public void testGetMapWhenMapIsStringEncoded() {
    VeniceProperties veniceProperties =
        new PropertyBuilder().put("region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev:dev-broker:9876")
            .build();
    Map<String, String> map = veniceProperties.getMap("region.to.pubsub.broker.map");
    assertEquals(map.size(), 2);
    assertEquals(map.get("prod"), "https://prod-broker:1234");
    assertEquals(map.get("dev"), "dev-broker:9876");

    // Map should be immutable
    assertThrows(() -> map.put("foo", "bar"));

    // Invalid encoding
    VeniceProperties invalidVeniceProperties =
        new PropertyBuilder().put("region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev;dev-broker")
            .build();
    assertThrows(VeniceException.class, () -> invalidVeniceProperties.getMap("region.to.pubsub.broker.map"));
  }
}
