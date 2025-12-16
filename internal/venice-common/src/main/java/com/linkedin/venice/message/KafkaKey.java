package com.linkedin.venice.message;

import com.linkedin.venice.guid.DoLStampGuidGenerator;
import com.linkedin.venice.guid.HeartbeatGuidV3Generator;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.memory.InstanceSizeEstimator;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.avro.specific.FixedSize;


/**
 * Class which stores the components of a Kafka Key, and is the format specified in the
 * {@link com.linkedin.venice.serialization.KafkaKeySerializer}.
 */
public class KafkaKey implements Measurable {
  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(KafkaKey.class);
  /**
   * For control messages, the Key part of the {@link KafkaKey} includes the producer GUID, segment and sequence number.
   *
   * N.B.: This could be optimized further by defining an Avro record to hold this data, since Avro would use
   * variable length encoding for the two integers, which would be smaller than their regular size.
   */
  public static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH =
      GUID.class.getAnnotation(FixedSize.class).value() + Integer.BYTES * 2;
  public static final KafkaKey HEART_BEAT = new KafkaKey(
      MessageType.CONTROL_MESSAGE,
      ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
          .put(HeartbeatGuidV3Generator.getInstance().getGuid().bytes())
          .putInt(0)
          .putInt(0)
          .array());

  /**
   * Special key for Declaration of Leadership (DoL) control messages.
   *
   * <p>Used during leader handover to confirm the new leader can write to and consume from the local VT.
   * This is distinct from HEART_BEAT to clearly separate leadership declaration from heartbeat semantics.
   *
   * <p>The DoL mechanism provides deterministic confirmation that a new leader has successfully:
   * <ul>
   *   <li>Produced a message to the local version topic (VT)</li>
   *   <li>Consumed that message back from VT (loopback confirmation)</li>
   * </ul>
   *
   * <p>This eliminates the need for time-based waits during leader handover and provides
   * strong guarantees that the leader is ready to switch to consuming from the leader source topic
   * (remote VT or RT topic).
   */
  public static final KafkaKey DOL_STAMP = new KafkaKey(
      MessageType.CONTROL_MESSAGE,
      ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
          .put(DoLStampGuidGenerator.getInstance().getGuid().bytes())
          .putInt(0) // segment number
          .putInt(0) // sequence number
          .array());

  private final byte keyHeaderByte;
  private final byte[] key; // TODO: Consider whether we may want to use a ByteBuffer here

  public KafkaKey(@Nonnull MessageType messageType, @Nonnull byte[] key) {
    this(messageType.getKeyHeaderByte(), key);
  }

  public KafkaKey(byte keyHeaderByte, @Nonnull byte[] key) {
    this.keyHeaderByte = keyHeaderByte;
    this.key = key;
  }

  /**
   * The key header byte is the first byte in the content of the Kafka key. This is
   * significant because it affects Kafka's Log Compaction. For {@link MessageType#PUT}
   * and {@link MessageType#DELETE}, we want to use the exact same Kafka key, so the
   * header byte needs to be the same. For control messages, however, we want them
   * to be name-spaced on their own, so that they do not collide with the data when
   * Log Compaction runs.
   *
   * @return a single byte: '0' for PUT and DELETE, or '2' for CONTROL_MESSAGE
   */
  public byte getKeyHeaderByte() {
    return keyHeaderByte;
  }

  /**
   * @return true if this key corresponds to a control message, and false otherwise.
   */
  public boolean isControlMessage() {
    return keyHeaderByte == MessageType.CONTROL_MESSAGE.getKeyHeaderByte();
  }

  /**
   * @return true if this key corresponds to a GlobalRtDiv message, and false otherwise.
   */
  public boolean isGlobalRtDiv() {
    return keyHeaderByte == MessageType.GLOBAL_RT_DIV.getKeyHeaderByte();
  }

  /**
   * @return the content of the key (everything beyond the first byte)
   */
  public byte[] getKey() {
    return key;
  }

  public int getKeyLength() {
    return key == null ? 0 : key.length;
  }

  private String messageTypeString() {
    switch (keyHeaderByte) {
      case MessageType.Constants.PUT_KEY_HEADER_BYTE:
        return "PUT or DELETE"; // PUT_KEY_HEADER_BYTE corresponds to both PUT or DELETE
      case MessageType.Constants.CONTROL_MESSAGE_KEY_HEADER_BYTE:
        return "CONTROL_MESSAGE";
      case MessageType.Constants.UPDATE_KEY_HEADER_BYTE:
        return "UPDATE";
      case MessageType.Constants.GLOBAL_RT_DIV_KEY_HEADER_BYTE:
        return "GLOBAL_RT_DIV";
      default:
        return "UNKNOWN";
    }
  }

  public String toString() {
    return getClass().getSimpleName() + "(" + messageTypeString() + ", " + ByteUtils.toHexString(key) + ")";
  }

  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD + InstanceSizeEstimator.getSize(this.key);
  }
}
