package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class ControlMessageTypeTest extends VeniceEnumValueTest<ControlMessageType> {
  public ControlMessageTypeTest() {
    super(ControlMessageType.class);
  }

  @Override
  protected Map<Integer, ControlMessageType> expectedMapping() {
    return CollectionUtil.<Integer, ControlMessageType>mapBuilder()
        .put(0, ControlMessageType.START_OF_PUSH)
        .put(1, ControlMessageType.END_OF_PUSH)
        .put(2, ControlMessageType.START_OF_SEGMENT)
        .put(3, ControlMessageType.END_OF_SEGMENT)
        .put(4, ControlMessageType.START_OF_BUFFER_REPLAY)
        .put(5, ControlMessageType.START_OF_INCREMENTAL_PUSH)
        .put(6, ControlMessageType.END_OF_INCREMENTAL_PUSH)
        .put(7, ControlMessageType.TOPIC_SWITCH)
        .put(8, ControlMessageType.VERSION_SWAP)
        .build();
  }
}
