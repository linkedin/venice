package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.*;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple enum to map the values of
 * {@link com.linkedin.venice.kafka.protocol.ControlMessage#controlMessageType}
 *
 * N.B.: We maintain this enum manually because Avro's auto-generated enums do
 *       not support evolution (i.e.: adding values) properly.
 */
public enum ControlMessageType {
  START_OF_PUSH(0),
  END_OF_PUSH(1),
  START_OF_SEGMENT(2),
  END_OF_SEGMENT(3),
  START_OF_BUFFER_REPLAY(4);

  /** The value is the byte used on the wire format */
  private final int value;
  private static final Map<Integer, ControlMessageType> MESSAGE_TYPE_MAP = getMessageTypeMap();

  ControlMessageType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  /**
   * Simple utility function to generate the right type of control message, based on message type.
   *
   * @return an empty instance of either:
   *         - {@link com.linkedin.venice.kafka.protocol.StartOfPush}
   *         - {@link com.linkedin.venice.kafka.protocol.EndOfPush}
   *         - {@link com.linkedin.venice.kafka.protocol.StartOfSegment}
   *         - {@link com.linkedin.venice.kafka.protocol.EndOfSegment}
   *         - {@link com.linkedin.venice.kafka.protocol.StartOfBufferReplay}
   */
  public Object getNewInstance() {
    switch (valueOf(value)) {
      case START_OF_PUSH: return new StartOfPush();
      case END_OF_PUSH: return new EndOfPush();
      case START_OF_SEGMENT: return new StartOfSegment();
      case END_OF_SEGMENT: return new EndOfSegment();
      case START_OF_BUFFER_REPLAY: return new StartOfBufferReplay();
      default: throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static Map<Integer, ControlMessageType> getMessageTypeMap() {
    Map<Integer, ControlMessageType> intToTypeMap = new HashMap<>();
    for (ControlMessageType type : ControlMessageType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public static ControlMessageType valueOf(int value) {
    ControlMessageType type = MESSAGE_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid control message type: " + value);
    }
    return type;
  }

  public static ControlMessageType valueOf(ControlMessage controlMessage) {
    return valueOf(controlMessage.controlMessageType);
  }
}