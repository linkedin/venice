package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.io.ZeroCopyByteArrayOutputStream;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubPositionType;
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
  private final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(ApacheKafkaOffsetPosition.class);

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

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *         -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  @Override
  public int comparePosition(PubSubPosition other) {
    validatePositionIsComparable(other);
    ApacheKafkaOffsetPosition otherPosition = (ApacheKafkaOffsetPosition) other;
    return Long.compare(offset, otherPosition.offset);
  }

  /**
   * @return the difference between this position and the other position
   *
   * @throws IllegalArgumentException if position is null or positions are not comparable
   */
  @Override
  public long diff(PubSubPosition other) {
    validatePositionIsComparable(other);
    ApacheKafkaOffsetPosition otherPosition = (ApacheKafkaOffsetPosition) other;
    return offset - otherPosition.offset;
  }

  /**
   * Checks if the other position is comparable to this position
   */
  private void validatePositionIsComparable(PubSubPosition other) {
    if (other == null) {
      throw new IllegalArgumentException("Cannot compare ApacheKafkaOffsetPosition with null");
    }

    if (!(other instanceof ApacheKafkaOffsetPosition)) {
      throw new IllegalArgumentException("Cannot compare ApacheKafkaOffsetPosition with " + other.getClass().getName());
    }
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return offset + "";
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

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   *
   * @return the position wrapper
   */
  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = PubSubPositionType.APACHE_KAFKA_OFFSET;
    // write the offset as avro serialized long
    try {
      ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream(10);
      AvroCompatibilityHelper.newBinaryEncoder(outputStream, false, ENCODER.get()).writeLong(offset);
      wireFormat.rawBytes = outputStream.toByteBuffer();
    } catch (IOException e) {
      throw new VeniceException("Failed to serialize ApacheKafkaOffsetPosition", e);
    }
    return wireFormat;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }
}
