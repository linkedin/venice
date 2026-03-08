package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class MessageTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<MessageType, String> expectedValues = CollectionUtils.<MessageType, String>mapBuilder()
        .put(MessageType.REQUEST, "request")
        .put(MessageType.RESPONSE, "response")
        .build();
    new VeniceDimensionTestFixture<>(MessageType.class, VeniceMetricsDimensions.VENICE_MESSAGE_TYPE, expectedValues)
        .assertAll();
  }
}
