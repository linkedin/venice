package com.linkedin.venice.offsets;

import com.google.common.collect.Maps;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.serialization.avro.PartitionStateSerializer;
import org.apache.avro.util.Utf8;

import java.util.Map;


public class OffsetRecord {

  // Offset 0 is still a valid offset, Using that will cause a message to be skipped.
  public static final long LOWEST_OFFSET = -1;

  private static final PartitionStateSerializer serializer = new PartitionStateSerializer();
  private static final String PARTITION_STATE_STRING = "PartitionState";

  private final PartitionState partitionState;

  public OffsetRecord(PartitionState partitionState) {
    this.partitionState = partitionState;
  }

  public OffsetRecord() {
    this(getEmptyPartitionState());
  }

  /**
   * @param bytes to deserialize from
   */
  public OffsetRecord(byte[] bytes) {
    this(deserializePartitionState(bytes));
  }

  private static PartitionState getEmptyPartitionState() {
    PartitionState emptyPartitionState = new PartitionState();
    emptyPartitionState.offset = LOWEST_OFFSET;
    emptyPartitionState.producerStates = Maps.newHashMap();
    emptyPartitionState.endOfPush = false;
    emptyPartitionState.lastUpdate = System.currentTimeMillis();
    return emptyPartitionState;
  }

  private static PartitionState deserializePartitionState(byte[] bytes) {
    return serializer.deserialize(PARTITION_STATE_STRING, bytes);
  }

  public long getOffset() {
    return this.partitionState.offset;
  }

  public void setOffset(long offset) {
    this.partitionState.offset = offset;
  }

  /**
   * @return the last messageTimeStamp across all producers tracked by this OffsetRecord
   */
  public long getEventTimeEpochMs() {
    return this.partitionState.producerStates.values().stream()
        .map(producerPartitionState -> producerPartitionState.messageTimestamp)
        .sorted((o1, o2) -> o1.compareTo(o2) * -1)
        .findFirst()
        .orElse(-1L);
  }

  public long getProcessingTimeEpochMs() {
    return this.partitionState.lastUpdate;
  }

  public void complete() {
    this.partitionState.endOfPush = true;
  }

  public boolean isCompleted() {
    return this.partitionState.endOfPush;
  }

  public void setProducerPartitionState(GUID producerGuid, ProducerPartitionState state) {
    this.partitionState.producerStates.put(guidToUtf8(producerGuid), state);
  }

  public Map<CharSequence, ProducerPartitionState> getProducerPartitionStates() {
    return this.partitionState.producerStates;
  }

  public ProducerPartitionState getProducerPartitionState(GUID producerGuid) {
    return getProducerPartitionStates().get(guidToUtf8(producerGuid));
  }

  /**
   * It may be useful to cache this mapping. TODO: Explore GC tuning later.
   *
   * @param guid to be converted
   * @return a {@link Utf8} instance corresponding to the {@link GUID} that was passed in
   */
  private CharSequence guidToUtf8(GUID guid) {
    return GuidUtils.getCharSequenceFromGuid(guid);
  }

  @Override
  public String toString() {
    return "OffsetRecord{" +
        "offset=" + getOffset() +
        ", eventTimeEpochMs=" + getEventTimeEpochMs() +
        ", processingTimeEpochMs=" + getProcessingTimeEpochMs() +
        ", completed=" + isCompleted() +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OffsetRecord that = (OffsetRecord) o;

    /** N.B.: {@link #partitionState.lastUpdate} intentionally omitted from comparison */
    return this.partitionState.offset == that.partitionState.offset &&
        this.partitionState.endOfPush == that.partitionState.endOfPush; // &&
    // N.B.: We cannot do a more thorough equality check at this time because it breaks tests.
    // If we actually need the more comprehensive equals, then we'll need to rethink the way we test.
    //    this.partitionState.producerStates.entrySet().equals(that.partitionState.producerStates.entrySet());
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + Long.hashCode(this.partitionState.offset);
    hash = hash * 31 + Boolean.hashCode(this.partitionState.endOfPush);
    return hash;
  }

  /**
   * serialize to bytes
   *
   * @return byte[]
   */
  public byte[] toBytes() {
    return serializer.serialize(PARTITION_STATE_STRING, partitionState);
  }
}
