package com.linkedin.venice.kafka.protocol.enums;

import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.END_OF_INCREMENTAL_PUSH;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.END_OF_PUSH;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.END_OF_SEGMENT;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_INCREMENTAL_PUSH;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_PUSH;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.TOPIC_SWITCH;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.VERSION_SWAP;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ControlMessageTypeTest {
  @Test
  public void test() {
    String assertionErrorMessage = "The value ID of enums should not be changed, as that is backwards incompatible.";

    Assert.assertEquals(ControlMessageType.valueOf(0), START_OF_PUSH, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(1), END_OF_PUSH, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(2), START_OF_SEGMENT, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(3), END_OF_SEGMENT, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(5), START_OF_INCREMENTAL_PUSH, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(6), END_OF_INCREMENTAL_PUSH, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(7), TOPIC_SWITCH, assertionErrorMessage);
    Assert.assertEquals(ControlMessageType.valueOf(8), VERSION_SWAP, assertionErrorMessage);
  }
}
