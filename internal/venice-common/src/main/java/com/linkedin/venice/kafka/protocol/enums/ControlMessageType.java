package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * A simple enum to map the values of
 * {@link com.linkedin.venice.kafka.protocol.ControlMessage#controlMessageType}
 *
 * N.B.: We maintain this enum manually because Avro's auto-generated enums do
 *       not support evolution (i.e.: adding values) properly.
 */
public enum ControlMessageType implements VeniceEnumValue {
  START_OF_PUSH(0), END_OF_PUSH(1), START_OF_SEGMENT(2), END_OF_SEGMENT(3), @Deprecated
  START_OF_BUFFER_REPLAY(4), START_OF_INCREMENTAL_PUSH(5), END_OF_INCREMENTAL_PUSH(6), TOPIC_SWITCH(7), VERSION_SWAP(8);

  /** The value is the byte used on the wire format */
  private final int value;
  private static final List<ControlMessageType> TYPES = EnumUtils.getEnumValuesList(ControlMessageType.class);

  ControlMessageType(int value) {
    this.value = value;
  }

  @Override
  public int getValue() {
    return value;
  }

  /**
   * Simple utility function to generate the right type of control message, based on message type.
   *
   * @return an empty instance of either:
   *         - {@link StartOfPush}
   *         - {@link EndOfPush}
   *         - {@link StartOfSegment}
   *         - {@link EndOfSegment}
   *         - {@link StartOfIncrementalPush}
   *         - {@link EndOfIncrementalPush}
   *         - {@link TopicSwitch}
   *         - {@link VersionSwap}
   */
  public Object getNewInstance() {
    switch (valueOf(value)) {
      case START_OF_PUSH:
        return new StartOfPush();
      case END_OF_PUSH:
        return new EndOfPush();
      case START_OF_SEGMENT:
        return new StartOfSegment();
      case END_OF_SEGMENT:
        return new EndOfSegment();
      case START_OF_INCREMENTAL_PUSH:
        return new StartOfIncrementalPush();
      case END_OF_INCREMENTAL_PUSH:
        return new EndOfIncrementalPush();
      case TOPIC_SWITCH:
        return new TopicSwitch();
      case VERSION_SWAP:
        return new VersionSwap();

      default:
        throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  public static ControlMessageType valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, ControlMessageType.class, VeniceMessageException::new);
  }

  public static ControlMessageType valueOf(ControlMessage controlMessage) {
    return valueOf(controlMessage.controlMessageType);
  }
}
