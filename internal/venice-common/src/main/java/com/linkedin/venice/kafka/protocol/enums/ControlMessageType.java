package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.StartOfBufferReplay;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;
import java.util.function.Supplier;


/**
 * A simple enum to map the values of
 * {@link com.linkedin.venice.kafka.protocol.ControlMessage#controlMessageType}
 *
 * N.B.: We maintain this enum manually because Avro's auto-generated enums do
 *       not support evolution (i.e.: adding values) properly.
 */
public enum ControlMessageType implements VeniceEnumValue {
  START_OF_PUSH(0, () -> new StartOfPush()),

  END_OF_PUSH(1, () -> new EndOfPush()),

  START_OF_SEGMENT(2, () -> new StartOfSegment()),

  END_OF_SEGMENT(3, () -> new EndOfSegment()),

  @Deprecated
  START_OF_BUFFER_REPLAY(4, () -> new StartOfBufferReplay()),

  START_OF_INCREMENTAL_PUSH(5, () -> new StartOfIncrementalPush()),

  END_OF_INCREMENTAL_PUSH(6, () -> new EndOfIncrementalPush()),

  TOPIC_SWITCH(7, () -> new TopicSwitch()),

  VERSION_SWAP(8, () -> new VersionSwap());

  /** The value is the byte used on the wire format */
  private final int value;
  private final Supplier<Object> constructor;
  private final int shallowClassOverhead;
  private static final List<ControlMessageType> TYPES = EnumUtils.getEnumValuesList(ControlMessageType.class);

  ControlMessageType(int value, Supplier<Object> constructor) {
    this.value = value;
    this.constructor = constructor;
    this.shallowClassOverhead = ClassSizeEstimator.getClassOverhead(constructor.get().getClass());
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
    return this.constructor.get();
  }

  public static ControlMessageType valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, ControlMessageType.class, VeniceMessageException::new);
  }

  public static ControlMessageType valueOf(ControlMessage controlMessage) {
    return valueOf(controlMessage.getControlMessageType());
  }

  public int getShallowClassOverhead() {
    return this.shallowClassOverhead;
  }
}
