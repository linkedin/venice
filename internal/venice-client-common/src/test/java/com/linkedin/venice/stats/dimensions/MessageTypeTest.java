package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class MessageTypeTest extends VeniceDimensionInterfaceTest<MessageType> {
  protected MessageTypeTest() {
    super(MessageType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
  }

  @Override
  protected Map<MessageType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<MessageType, String>mapBuilder()
        .put(MessageType.REQUEST, "request")
        .put(MessageType.RESPONSE, "response")
        .build();
  }
}
