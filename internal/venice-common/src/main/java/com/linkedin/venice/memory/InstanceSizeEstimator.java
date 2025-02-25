package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.ClassSizeEstimator.ARRAY_HEADER_SIZE;
import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.function.ToIntFunction;
import javax.annotation.Nonnull;


/**
 * This utility class provides functions to measure the heap size of objects for a limited number of classes. Wherever
 * possible, the logic makes use of knowledge of the Venice code so that shared or static instances are ignored (i.e.,
 * assuming that their amortized cost is negligible). The code here should all be hot path friendly, and make no use of
 * reflection (except in static constants).
 *
 * If more classes require heap size measurement, the preferred approach is NOT to add code into this class! Rather, the
 * preferred approach is to implement {@link Measurable} and write the logic in {@link Measurable#getHeapSize()}. This
 * utility class is for cases where we wish to measure classes that we cannot modify, such as those coming from the JDK,
 * from third-party libraries, or from code-gen.
 */
public class InstanceSizeEstimator {
  private static final ClassValue<ToIntFunction<Object>> CACHE = new ClassValue<ToIntFunction<Object>>() {
    @Override
    protected ToIntFunction<Object> computeValue(Class<?> type) {
      if (Measurable.class.isAssignableFrom(type)) {
        return value -> ((Measurable) value).getHeapSize();
      } else if (Put.class.isAssignableFrom(type)) {
        return value -> getSize((Put) value);
      } else if (Delete.class.isAssignableFrom(type)) {
        return value -> getSize((Delete) value);
      } else if (Update.class.isAssignableFrom(type)) {
        return value -> getSize((Update) value);
      } else if (ControlMessage.class.isAssignableFrom(type)) {
        return value -> getSize((ControlMessage) value);
      } else if (KafkaMessageEnvelope.class.isAssignableFrom(type)) {
        return value -> getSize((KafkaMessageEnvelope) value);
      } else if (ProducerMetadata.class.isAssignableFrom(type)) {
        return value -> getSize((ProducerMetadata) value);
      } else if (ByteBuffer.class.isAssignableFrom(type)) {
        return value -> getSize((ByteBuffer) value);
      } else if (type.isArray() && type.getComponentType().equals(byte.class)) {
        return value -> getSize((byte[]) value);
      }
      return null;
    }
  };
  private static final int GUID_FULL_CLASS_OVERHEAD =
      getClassOverhead(GUID.class) + getByteArraySizeByLength(GUID.getClassSchema().getFixedSize());
  private static final int PRODUCER_METADATA_FULL_CLASS_OVERHEAD =
      getClassOverhead(ProducerMetadata.class) + GUID_FULL_CLASS_OVERHEAD;
  private static final int KME_PARTIAL_CLASS_OVERHEAD =
      getClassOverhead(KafkaMessageEnvelope.class) + PRODUCER_METADATA_FULL_CLASS_OVERHEAD;
  private static final int PUT_SHALLOW_CLASS_OVERHEAD = getClassOverhead(Put.class);
  private static final int UPDATE_SHALLOW_CLASS_OVERHEAD = getClassOverhead(Update.class);
  private static final int DELETE_SHALLOW_CLASS_OVERHEAD = getClassOverhead(Delete.class);
  private static final int CONTROL_MESSAGE_SHALLOW_CLASS_OVERHEAD = getClassOverhead(ControlMessage.class);
  private static final int LEADER_METADATA_SHALLOW_CLASS_OVERHEAD = getClassOverhead(LeaderMetadata.class);
  private static final int BYTE_BUFFER_SHALLOW_CLASS_OVERHEAD = getClassOverhead(ByteBuffer.class);

  private InstanceSizeEstimator() {
    // Static utility
  }

  /**
   * Works for {@link Measurable} objects and a small number of other types.
   *
   * Not intended as a generic utility for any instance type!
   *
   * @throws IllegalArgumentException when an unsupported type is passed.
   */
  public static int getObjectSize(@Nonnull Object o) {
    if (o instanceof Measurable) {
      return ((Measurable) o).getHeapSize();
    }
    ToIntFunction<Object> sizeComputation = CACHE.get(o.getClass());
    if (sizeComputation == null) {
      throw new IllegalArgumentException("Object of type " + o.getClass() + " is not measurable!");
    }
    return sizeComputation.applyAsInt(o);
  }

  public static int getByteArraySizeByLength(int length) {
    return ClassSizeEstimator.roundUpToNearestAlignment(ARRAY_HEADER_SIZE + length);
  }

  public static int getSize(@Nonnull byte[] bytes) {
    return getByteArraySizeByLength(bytes.length);
  }

  public static int getSize(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return 0;
    }
    if (byteBuffer.hasArray()) {
      return BYTE_BUFFER_SHALLOW_CLASS_OVERHEAD + getByteArraySizeByLength(byteBuffer.capacity());
    }

    throw new IllegalArgumentException("Only array-backed ByteBuffers are measurable with this function.");
  }

  public static int getSize(@Nonnull ProducerMetadata producerMetadata) {
    return PRODUCER_METADATA_FULL_CLASS_OVERHEAD;
  }

  public static int getSize(@Nonnull Put put) {
    int size = PUT_SHALLOW_CLASS_OVERHEAD;
    size += getSize(put.putValue);
    if (put.replicationMetadataPayload != null) {
      size += BYTE_BUFFER_SHALLOW_CLASS_OVERHEAD;
      if (put.replicationMetadataPayload.array() != put.putValue.array()) {
        /**
         * N.B.: When using the {@link org.apache.avro.io.OptimizedBinaryDecoder}, the {@link put.putValue} and the
         *       {@link put.replicationMetadataPayload} will be backed by the same underlying array. If that is the
         *       case, then we don't want to account for the capacity twice.
         */
        size += put.replicationMetadataPayload.capacity();
      }
    }
    return size;
  }

  public static int getSize(@Nonnull Delete delete) {
    return DELETE_SHALLOW_CLASS_OVERHEAD + getSize(delete.replicationMetadataPayload);
  }

  /**
   * This function is imprecise in a couple of ways. The {@link ControlMessage#controlMessageUnion} field is treated as
   * shallow, which in some cases is false (e.g. if a compression dictionary were present), and the
   * {@link ControlMessage#debugInfo} is ignored (i.e. treated as null).
   *
   * We can be more precise by looking at the {@link ControlMessage#controlMessageType} and then providing the precise
   * overhead based on each type, but we're skipping this work for now since, in our use case, control messages should
   * be a negligible fraction of all messages, and therefore not that important to get exactly right.
   */
  public static int getSize(@Nonnull ControlMessage cm) {
    return CONTROL_MESSAGE_SHALLOW_CLASS_OVERHEAD + ControlMessageType.valueOf(cm).getShallowClassOverhead();
  }

  public static int getSize(@Nonnull Update update) {
    return UPDATE_SHALLOW_CLASS_OVERHEAD + getSize(update.updateValue);
  }

  /**
   * Measure the heap usage of {@link KafkaMessageEnvelope}.
   */
  public static int getSize(KafkaMessageEnvelope kme) {
    int size = KME_PARTIAL_CLASS_OVERHEAD;
    switch (MessageType.valueOf(kme)) {
      case PUT:
      case GLOBAL_RT_DIV: // GLOBAL_RT_DIV is the same as PUT, but contains a DIV object rather than user data
        size += getSize((Put) kme.payloadUnion);
        break;
      case DELETE:
        size += getSize((Delete) kme.payloadUnion);
        break;
      case CONTROL_MESSAGE:
        size += getSize((ControlMessage) kme.payloadUnion);
        break;
      case UPDATE:
        size += getSize((Update) kme.payloadUnion);
        break;
    }
    if (kme.leaderMetadataFooter != null && !(kme.leaderMetadataFooter instanceof VeniceWriter.DefaultLeaderMetadata)) {
      /** The host name part of this object should always be shared, hence we ignore it. */
      size += LEADER_METADATA_SHALLOW_CLASS_OVERHEAD;
    }
    return size;
  }
}
