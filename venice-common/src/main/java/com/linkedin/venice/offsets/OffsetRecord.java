package com.linkedin.venice.offsets;

import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class OffsetRecord {

  // Offset 0 is still a valid offset, Using that will cause a message to be skipped.
  public static final long LOWEST_OFFSET = -1;

  private static final InternalAvroSpecificSerializer<PartitionState> serializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private static final String PARTITION_STATE_STRING = "PartitionState";

  private final PartitionState partitionState;

  private Map<GUID, OffsetRecordTransformer> offsetRecordTransformers = new VeniceConcurrentHashMap<>();

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
    emptyPartitionState.producerStates = new HashMap<>();
    emptyPartitionState.endOfPush = false;
    emptyPartitionState.lastUpdate = System.currentTimeMillis();
    emptyPartitionState.startOfBufferReplayDestinationOffset = null;
    emptyPartitionState.databaseInfo = new HashMap<>();
    return emptyPartitionState;
  }

  private static PartitionState deserializePartitionState(byte[] bytes) {
    return serializer.deserialize(PARTITION_STATE_STRING, bytes);
  }

  /**
   * This function will keep a map of reference to all the {@link OffsetRecordTransformer}
   * from different producers.
   */
  public void addOffsetRecordTransformer(GUID producerGUID, OffsetRecordTransformer transformer) {
    offsetRecordTransformers.put(producerGUID, transformer);
  }

  /**
   * {@link OffsetRecordTransformer#transform(OffsetRecord)} is quite expensive,
   * so this function should be only invoked when necessary.
   */
  public void transform() {
    for (OffsetRecordTransformer transformer : offsetRecordTransformers.values()) {
      transformer.transform(this);
    }
    offsetRecordTransformers.clear();
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

  public void setProcessingTimeEpochMs(long updateTimeInMs) {
    this.partitionState.lastUpdate = updateTimeInMs;
  }

  public void endOfPushReceived(long endOfPushOffset) {
    if (endOfPushOffset < 1) {
      // Even an empty push should have a SOP and EOP, so offset 1 is the absolute minimum.
      throw new IllegalArgumentException("endOfPushOffset cannot be < 1.");
    }
//    this.setOffset(endOfPushOffset);
    this.partitionState.endOfPush = true;
  }

  public boolean isEndOfPushReceived() {
    return this.partitionState.endOfPush;
  }

  public void setProducerPartitionState(GUID producerGuid, ProducerPartitionState state) {
    this.partitionState.producerStates.put(guidToUtf8(producerGuid), state);
  }

  public Map<CharSequence, ProducerPartitionState> getProducerPartitionStateMap() {
    return this.partitionState.producerStates;
  }

  public void setProducerPartitionStateMap(Map<CharSequence, ProducerPartitionState> producerStates) {
    this.partitionState.producerStates = producerStates;
  }

  public ProducerPartitionState getProducerPartitionState(GUID producerGuid) {
    return getProducerPartitionStateMap().get(guidToUtf8(producerGuid));
  }

  public void setStartOfBufferReplayDestinationOffset(long startOfBufferReplayDestinationOffset) {
    this.partitionState.startOfBufferReplayDestinationOffset = startOfBufferReplayDestinationOffset;
  }

  public Optional<Long> getStartOfBufferReplayDestinationOffset() {
    return Optional.ofNullable(partitionState.startOfBufferReplayDestinationOffset);
  }

  public void setDatabaseInfo(Map<String, String> databaseInfo) {
    Map<CharSequence, CharSequence> databaseInfoWithRightType = new HashMap<>();
    databaseInfo.forEach( (k, v) -> databaseInfoWithRightType.put(k, v));
    this.partitionState.databaseInfo = databaseInfoWithRightType;
  }

  public Map<String, String> getDatabaseInfo() {
    Map<String, String> databaseInfo = new HashMap<>();
    if (this.partitionState.databaseInfo != null) {
      /**
       * It is necessary since the 'string' type deserialized by Avro is {@link Utf8}.
       */
      this.partitionState.databaseInfo.forEach( (k, v) -> databaseInfo.put(k.toString(), v.toString()));
    }
    return databaseInfo;
  }

  public void setIncrementalPush(IncrementalPush ip) {
    this.partitionState.incrementalPushInfo = ip;
  }

  public IncrementalPush getIncrementalPush() {
    return this.partitionState.incrementalPushInfo;
  }

  public void setLeaderConsumptionState(String topic, long startOffset) {
    this.partitionState.leaderTopic = topic;
    this.partitionState.leaderOffset = startOffset;
  }

  public void setLeaderTopicOffset(long leaderOffset) {
    this.partitionState.leaderOffset = leaderOffset;
  }

  public void setLeaderGUID(GUID guid) {
    this.partitionState.leaderGUID = guid;
  }

  public void setLeaderHostId(String leaderHostId) {
    this.partitionState.leaderHostId = leaderHostId;
  }

  public String getLeaderTopic() {
    return (partitionState.leaderTopic != null) ? partitionState.leaderTopic.toString() : null;
  }

  public long getLeaderOffset() {
    return this.partitionState.leaderOffset;
  }

  public GUID getLeaderGUID() {
    return this.partitionState.leaderGUID;
  }

  public String getLeaderHostId() {
    return this.partitionState.leaderHostId.toString();
  }

  /**
   * It may be useful to cache this mapping. TODO: Explore GC tuning later.
   *
   * @param guid to be converted
   * @return a {@link Utf8} instance corresponding to the {@link GUID} that was passed in
   */
  private CharSequence guidToUtf8(GUID guid) {
    return new Utf8(GuidUtils.getCharSequenceFromGuid(guid));
  }

  @Override
  public String toString() {
    return "OffsetRecord{" +
        "offset=" + getOffset() +
        ", eventTimeEpochMs=" + getEventTimeEpochMs() +
        ", processingTimeEpochMs=" + getProcessingTimeEpochMs() +
        ", isEndOfPushReceived=" + isEndOfPushReceived() +
        ", databaseInfo=" + getDatabaseInfo() +
        '}';
  }

  /**
   * This function will print out detailed offset info, which including info per producerGuid.
   * The reason is not using the default {@link PartitionState#toString} since it won't print GUID properly.
   *
   * This function is mostly used in AdminOffsetManager since the offset record update frequency for
   * admin topic is very low.
   */
  public String toDetailedString() {
    final String producerStatesFieldName = "producerStates";
    StringBuilder sb = new StringBuilder();
    sb.append("OffsetRecord{");
    for (Schema.Field f : partitionState.getSchema().getFields()) {
      if (f.name().equals(producerStatesFieldName)) {
        sb.append("\n" + producerStatesFieldName + ":");
        if (partitionState.producerStates != null) {
          sb.append("{");
          partitionState.producerStates.forEach((charSeq, producerState) -> {
            sb.append("\n{")
                .append(GuidUtils.getGuidFromCharSequence(charSeq))
                .append(":")
                .append(producerState);
          });
          sb.append("\n}");
        } else {
          sb.append("null");
        }
      } else {
        sb.append("\n" + f.name() + ": " + partitionState.get(f.pos()));
      }
    }
    sb.append("\n}");
    return sb.toString();
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
