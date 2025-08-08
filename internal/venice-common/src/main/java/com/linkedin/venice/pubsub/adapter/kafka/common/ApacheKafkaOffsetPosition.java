package com.linkedin.venice.pubsub.adapter.kafka.common;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.io.ZeroCopyByteArrayOutputStream;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;


/**
 * Offset position for Apache Kafka topics
 */
public class ApacheKafkaOffsetPosition implements PubSubPosition {
  private static final ThreadLocal<BinaryDecoder> DECODER = new ThreadLocal<>();
  private static final ThreadLocal<BinaryEncoder> ENCODER = new ThreadLocal<>();
  private static final int SHALLOW_CLASS_OVERHEAD =
      ClassSizeEstimator.getClassOverhead(ApacheKafkaOffsetPosition.class);

  private final long offset;

  public ApacheKafkaOffsetPosition(long offset) {
    if (offset < OffsetRecord.LOWEST_OFFSET) {
      throw new IllegalArgumentException("Offset must be greater than or equal to " + OffsetRecord.LOWEST_OFFSET);
    }
    this.offset = offset;
  }

  /**
   * @param buffer the buffer to read from. The ByteBuffer expected to contain avro serialized long
   * @throws IOException  if the buffer is not a valid avro serialized long
   */
  public ApacheKafkaOffsetPosition(ByteBuffer buffer) throws IOException {
    this(
        AvroCompatibilityHelper.newBinaryDecoder(buffer.array(), buffer.position(), buffer.remaining(), DECODER.get())
            .readLong());
  }

  public long getInternalOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "ApacheKafkaOffsetPosition{" + offset + '}';
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ApacheKafkaOffsetPosition that = (ApacheKafkaOffsetPosition) other;
    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(offset);
  }

  @Override
  public long getNumericOffset() {
    return offset;
  }

  public static ApacheKafkaOffsetPosition of(long offset) {
    return new ApacheKafkaOffsetPosition(offset);
  }

  public static ApacheKafkaOffsetPosition of(ByteBuffer buffer) throws IOException {
    return new ApacheKafkaOffsetPosition(buffer);
  }

  /**
   * Returns an Avro-encoded wire format representation of this position, including both
   * the position type ID and the raw Avro-encoded bytes representing the position value.
   * This is used to transmit {@link PubSubPosition} instances over the wire.
   *
   * <p>Example (ApacheKafkaOffsetPosition):
   * <pre>
   * +-------------------------+
   * | Avro-encoded int: type  |  (e.g., 0)
   * +-------------------------+
   * | Avro-encoded bytes:     |  (Avro-encoded long offset)
   * |   rawBytes              |
   * +-------------------------+
   * </pre>
   *
   * @return the serialized {@link PubSubPositionWireFormat} containing the type and position bytes
   */
  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    try {
      ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream(10);
      AvroCompatibilityHelper.newBinaryEncoder(outputStream, false, ENCODER.get()).writeLong(offset);

      PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
      wireFormat.type = APACHE_KAFKA_OFFSET_POSITION_TYPE_ID;
      wireFormat.rawBytes = outputStream.toByteBuffer();
      return wireFormat;
    } catch (IOException e) {
      throw new VeniceException("Failed to serialize ApacheKafkaOffsetPosition", e);
    }
  }

  @Override
  public Class<? extends PubSubPositionFactory> getFactoryClass() {
    return ApacheKafkaOffsetPositionFactory.class;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }
}
