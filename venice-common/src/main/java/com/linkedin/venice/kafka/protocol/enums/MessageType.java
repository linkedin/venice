package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple enum to map the values of
 * {@link com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope#messageType}
 *
 * N.B.: We maintain this enum manually because Avro's auto-generated enums do
 *       not support evolution (i.e.: adding values) properly.
 */
public enum MessageType {
  PUT(0, (byte) 0),
  DELETE(1, (byte) 0),
  CONTROL_MESSAGE(2, (byte) 2);

  private static final Map<Integer, MessageType> MESSAGE_TYPE_MAP = getMessageTypeMap();

  private final int value;
  private final byte keyHeaderByte;

  private MessageType(int value, byte keyHeaderByte) {
    this.value = (byte) value;
    this.keyHeaderByte = keyHeaderByte;
  }

  /**
   * @return This is the value used in {@link com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope#messageType}
   *         to distinguish message types.
   */
  public int getValue() {
    return value;
  }

  /**
   * @return This is the value used at the beginning of our Kafka keys, in order to ensure that
   *         Kafka's Log Compaction has the correct overwriting semantics. For example, DELETE
   *         must overwrite PUT and vice versa, so they share the same {@param keyHeaderByte}.
   */
  public byte getKeyHeaderByte() {
    return keyHeaderByte;
  }

  /**
   * Simple utility function to generate the right type of payload, based on message type.
   *
   * @return an empty instance of either:
   *         - {@value com.linkedin.venice.kafka.protocol.Put}
   *         - {@value com.linkedin.venice.kafka.protocol.Delete}
   *         - {@value com.linkedin.venice.kafka.protocol.ControlMessage}
   */
  public Object getNewInstance() {
    switch (valueOf(value)) {
      case PUT: return new Put();
      case DELETE: return new Delete();
      case CONTROL_MESSAGE: return new ControlMessage();
      default: throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static Map<Integer, MessageType> getMessageTypeMap() {
    Map<Integer, MessageType> intToTypeMap = new HashMap<>();
    for (MessageType type : MessageType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  private static MessageType valueOf(int value) {
    MessageType type = MESSAGE_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid message type: " + value);
    }
    return type;
  }

  public static MessageType valueOf(KafkaMessageEnvelope kafkaMessageEnvelope) {
    return valueOf(kafkaMessageEnvelope.messageType);
  }
}