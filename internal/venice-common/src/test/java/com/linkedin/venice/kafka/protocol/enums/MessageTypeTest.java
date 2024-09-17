package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class MessageTypeTest extends VeniceEnumValueTest<MessageType> {
  public MessageTypeTest() {
    super(MessageType.class);
  }

  @Override
  protected Map<Integer, MessageType> expectedMapping() {
    return CollectionUtil.<Integer, MessageType>mapBuilder()
        .put(0, MessageType.PUT)
        .put(1, MessageType.DELETE)
        .put(2, MessageType.CONTROL_MESSAGE)
        .put(3, MessageType.UPDATE)
        .build();
  }
}
