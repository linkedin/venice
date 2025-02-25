package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class MessageTypeTest extends VeniceEnumValueTest<MessageType> {
  public MessageTypeTest() {
    super(MessageType.class);
  }

  @Override
  protected Map<Integer, MessageType> expectedMapping() {
    return CollectionUtils.<Integer, MessageType>mapBuilder()
        .put(0, MessageType.PUT)
        .put(1, MessageType.DELETE)
        .put(2, MessageType.CONTROL_MESSAGE)
        .put(3, MessageType.UPDATE)
        .put(4, MessageType.GLOBAL_RT_DIV)
        .build();
  }
}
