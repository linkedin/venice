package com.linkedin.venice.spark.utils;

import static com.linkedin.venice.ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP;

import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Properties;


/**
 * Test-only {@link PubSubPosition} modeled after log-structured pub-sub positions: it has no meaningful numeric
 * offset ({@link #getNumericOffset()} returns -1) but can be ordered offline within the same {@code rangeId}.
 * Positions in different ranges cannot be compared offline.
 */
public class OfflineOrderedTestPosition implements PubSubPosition {
  public static final int TYPE_ID = 77;

  private final int rangeId;
  private final long sequence;

  public OfflineOrderedTestPosition(int rangeId, long sequence) {
    this.rangeId = rangeId;
    this.sequence = sequence;
  }

  /** @return serialized wire-format bytes of a position, as stored in the Spark {@code __position__} column */
  public static byte[] positionBytes(int rangeId, long sequence) {
    return new OfflineOrderedTestPosition(rangeId, sequence).toWireFormatBytes();
  }

  /** @return job properties registering this position type, as a KIF repush job against such a system would have */
  public static Properties registryProperties() {
    Properties props = new Properties();
    props.setProperty(PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP, TYPE_ID + ":" + Factory.class.getName());
    return props;
  }

  public static PubSubPositionRowComparator newRowComparator() {
    return new PubSubPositionRowComparator(new VeniceProperties(registryProperties()));
  }

  @Override
  public long getNumericOffset() {
    return -1;
  }

  @Override
  public int comparePositionOffline(PubSubPosition other) {
    if (!(other instanceof OfflineOrderedTestPosition) || ((OfflineOrderedTestPosition) other).rangeId != rangeId) {
      throw new UnsupportedOperationException("Cannot compare " + this + " with " + other);
    }
    return Long.compare(sequence, ((OfflineOrderedTestPosition) other).sequence);
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    ByteBuffer raw = ByteBuffer.allocate(Integer.BYTES + Long.BYTES).putInt(rangeId).putLong(sequence);
    raw.flip();
    return new PubSubPositionWireFormat(TYPE_ID, raw);
  }

  @Override
  public Class<? extends PubSubPositionFactory> getFactoryClass() {
    return Factory.class;
  }

  @Override
  public int getHeapSize() {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof OfflineOrderedTestPosition)) {
      return false;
    }
    OfflineOrderedTestPosition that = (OfflineOrderedTestPosition) o;
    return rangeId == that.rangeId && sequence == that.sequence;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rangeId, sequence);
  }

  @Override
  public String toString() {
    return "OfflineOrdered{range=" + rangeId + ",seq=" + sequence + "}";
  }

  public static class Factory extends PubSubPositionFactory {
    public Factory(int positionTypeId) {
      super(positionTypeId);
    }

    @Override
    public PubSubPosition fromPositionRawBytes(ByteBuffer raw) {
      ByteBuffer buf = raw.duplicate();
      return new OfflineOrderedTestPosition(buf.getInt(), buf.getLong());
    }

    @Override
    public String getPubSubPositionClassName() {
      return OfflineOrderedTestPosition.class.getName();
    }
  }
}
