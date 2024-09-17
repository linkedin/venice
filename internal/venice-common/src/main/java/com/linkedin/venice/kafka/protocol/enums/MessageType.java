package com.linkedin.venice.kafka.protocol.enums;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * A simple enum to map the values of
 * {@link com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope#messageType}
 *
 * N.B.: We maintain this enum manually because Avro's auto-generated enums do
 *       not support evolution (i.e.: adding values) properly.
 */
public enum MessageType implements VeniceEnumValue {
  PUT(0, Constants.PUT_KEY_HEADER_BYTE), DELETE(1, Constants.PUT_KEY_HEADER_BYTE),
  CONTROL_MESSAGE(2, Constants.CONTROL_MESSAGE_KEY_HEADER_BYTE), UPDATE(3, Constants.UPDATE_KEY_HEADER_BYTE);

  private static final List<MessageType> TYPES = EnumUtils.getEnumValuesList(MessageType.class);

  private final int value;
  private final byte keyHeaderByte;

  MessageType(int value, byte keyHeaderByte) {
    this.value = (byte) value;
    this.keyHeaderByte = keyHeaderByte;
  }

  /**
   * @return This is the value used in {@link com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope#messageType}
   *         to distinguish message types.
   */
  @Override
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
   *         - {@link com.linkedin.venice.kafka.protocol.Put}
   *         - {@link com.linkedin.venice.kafka.protocol.Delete}
   *         - {@link com.linkedin.venice.kafka.protocol.ControlMessage}
   */
  public Object getNewInstance() {
    switch (valueOf(value)) {
      case PUT:
        return new Put();
      case DELETE:
        return new Delete();
      case CONTROL_MESSAGE:
        return new ControlMessage();
      case UPDATE:
        return new Update();
      default:
        throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  public static MessageType valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, MessageType.class, VeniceMessageException::new);
  }

  public static MessageType valueOf(KafkaMessageEnvelope kafkaMessageEnvelope) {
    return valueOf(kafkaMessageEnvelope.messageType);
  }

  public static class Constants {
    public static final byte PUT_KEY_HEADER_BYTE = 0;
    public static final byte CONTROL_MESSAGE_KEY_HEADER_BYTE = 2;
    public static final byte UPDATE_KEY_HEADER_BYTE = 4;
  }
}
