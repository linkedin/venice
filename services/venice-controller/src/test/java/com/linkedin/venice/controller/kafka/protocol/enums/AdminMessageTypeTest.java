package com.linkedin.venice.controller.kafka.protocol.enums;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import org.testng.annotations.Test;


public class AdminMessageTypeTest {
  @Test
  public void testVeniceDimensionInterface() {
    for (AdminMessageType type: AdminMessageType.values()) {
      assertEquals(
          type.getDimensionName(),
          VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE,
          "Unexpected dimension name for " + type.name());
      String expectedValue = type.name().toLowerCase();
      assertNotNull(type.getDimensionValue(), "Dimension value should not be null for " + type.name());
      assertEquals(type.getDimensionValue(), expectedValue, "Unexpected dimension value for " + type.name());
    }
  }
}
