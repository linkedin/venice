package com.linkedin.venice.offsets;

import static com.linkedin.venice.pubsub.PubSubUtil.fromKafkaOffset;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.ByteBufferToHexFormatJsonEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.Validate;


/**
 * If OffsetRecord is initialized with a serializer that contains SchemaReader, old version of server codes
 * will be able to deserialize OffsetRecord that is serialized with a newer protocol version, which can happen
 * after rolling back a server release with new protocol version to an old server release with old protocol version.
 */
public class OffsetRecord {
  // Offset 0 is still a valid offset, Using that will cause a message to be skipped.
  public static final long LOWEST_OFFSET = -1;
  public static final long LOWEST_OFFSET_LAG = 0;
  public static final long DEFAULT_OFFSET_LAG = -1;
  public static final String NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY = ""; // A place holder key
  private static final String PARTITION_STATE_STRING = "PartitionState";
  private static final String NULL_STRING = "null";
  private final PartitionState partitionState;
  private final InternalAvroSpecificSerializer<PartitionState> serializer;

  private PubSubTopic leaderPubSubTopic;

  public OffsetRecord(PartitionState partitionState, InternalAvroSpecificSerializer<PartitionState> serializer) {
    this.partitionState = partitionState;
    this.serializer = serializer;
  }

  public OffsetRecord(InternalAvroSpecificSerializer<PartitionState> serializer) {
    this(getEmptyPartitionState(), serializer);
  }

  /**
   * @param bytes to deserialize from
   */
  public OffsetRecord(byte[] bytes, InternalAvroSpecificSerializer<PartitionState> serializer) {
    this.serializer = serializer;
    this.partitionState = deserializePartitionState(bytes);
  }

  private static PartitionState getEmptyPartitionState() {
    PartitionState emptyPartitionState = new PartitionState();
    emptyPartitionState.offset = LOWEST_OFFSET;
    emptyPartitionState.offsetLag = LOWEST_OFFSET_LAG;
    emptyPartitionState.producerStates = new VeniceConcurrentHashMap<>();
    emptyPartitionState.endOfPush = false;
    emptyPartitionState.lastUpdate = 0;
    emptyPartitionState.databaseInfo = new VeniceConcurrentHashMap<>();
    emptyPartitionState.previousStatuses = new VeniceConcurrentHashMap<>();
    emptyPartitionState.leaderOffset = PubSubSymbolicPosition.EARLIEST.getNumericOffset();
    emptyPartitionState.upstreamOffsetMap = new VeniceConcurrentHashMap<>();
    emptyPartitionState.upstreamVersionTopicOffset = PubSubSymbolicPosition.EARLIEST.getNumericOffset();
    emptyPartitionState.pendingReportIncrementalPushVersions = new ArrayList<>();
    emptyPartitionState.setRealtimeTopicProducerStates(new VeniceConcurrentHashMap<>());
    emptyPartitionState.upstreamRealTimeTopicPubSubPositionMap = new VeniceConcurrentHashMap<>();
    emptyPartitionState.currentTermStartPubSubPosition = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    emptyPartitionState.lastProcessedVersionTopicPubSubPosition = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    emptyPartitionState.lastConsumedVersionTopicPubSubPosition = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    emptyPartitionState.upstreamVersionTopicPubSubPosition = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    return emptyPartitionState;
  }

  private PartitionState deserializePartitionState(byte[] bytes) {
    return serializer.deserialize(PARTITION_STATE_STRING, bytes);
  }

  public void setPreviousStatusesEntry(String key, String value) {
    partitionState.getPreviousStatuses().put(key, value);
  }

  public String getPreviousStatusesEntry(String key) {
    return partitionState.getPreviousStatuses().getOrDefault(key, NULL_STRING).toString();
  }

  public PubSubPosition getCheckpointedLocalVtPosition() {
    // TODO: Replace with lastProcessedVersionTopicPubSubPosition
    return fromKafkaOffset(this.partitionState.offset);
  }

  public void checkpointLocalVtPosition(PubSubPosition vtPosition) {
    if (vtPosition == null || vtPosition.getPositionWireFormat() == null) {
      String msg = (vtPosition == null) ? "Position" : "Position wire format";
      throw new IllegalArgumentException(msg + " cannot be null");
    }
    this.partitionState.lastProcessedVersionTopicPubSubPosition = vtPosition.toWireFormatBuffer();
    this.partitionState.offset = vtPosition.getNumericOffset();
  }

  public PubSubPosition getCheckpointedRemoteVtPosition() {
    return fromKafkaOffset(this.partitionState.upstreamVersionTopicOffset);
  }

  public void checkpointRemoteVtPosition(PubSubPosition remoteVtPosition) {
    this.partitionState.upstreamVersionTopicPubSubPosition = remoteVtPosition.toWireFormatBuffer();
    this.partitionState.upstreamVersionTopicOffset = remoteVtPosition.getNumericOffset();
  }

  public long getOffsetLag() {
    return this.partitionState.offsetLag;
  }

  public void setOffsetLag(long offsetLag) {
    this.partitionState.offsetLag = offsetLag;
  }

  public long getHeartbeatTimestamp() {
    return this.partitionState.heartbeatTimestamp;
  }

  public void setHeartbeatTimestamp(long heartbeatTimestamp) {
    this.partitionState.heartbeatTimestamp = heartbeatTimestamp;
  }

  public long getLastCheckpointTimestamp() {
    return this.partitionState.lastCheckpointTimestamp;
  }

  public void setLastCheckpointTimestamp(long timestamp) {
    this.partitionState.lastCheckpointTimestamp = timestamp;
  }

  /**
   * @return the last messageTimeStamp across all producers tracked by this OffsetRecord
   */
  public long getMaxMessageTimeInMs() {
    long maxMessageTimestamp = -1;
    for (ProducerPartitionState state: this.partitionState.producerStates.values()) {
      maxMessageTimestamp = Math.max(maxMessageTimestamp, state.messageTimestamp);
    }
    return maxMessageTimestamp;
  }

  public long getLatestProducerProcessingTimeInMs() {
    return this.partitionState.lastUpdate;
  }

  public void setLatestProducerProcessingTimeInMs(long updateTimeInMs) {
    this.partitionState.lastUpdate = updateTimeInMs;
  }

  public void endOfPushReceived() {
    this.partitionState.endOfPush = true;
  }

  public boolean isEndOfPushReceived() {
    return this.partitionState.endOfPush;
  }

  public synchronized void setProducerPartitionState(GUID producerGuid, ProducerPartitionState state) {
    this.partitionState.producerStates.put(guidToUtf8(producerGuid), state);
  }

  public synchronized void removeProducerPartitionState(GUID producerGuid) {
    this.partitionState.producerStates.remove(guidToUtf8(producerGuid));
  }

  public synchronized Map<CharSequence, ProducerPartitionState> getProducerPartitionStateMap() {
    return this.partitionState.producerStates;
  }

  public synchronized void setRealtimeTopicProducerState(
      String kafkaUrl,
      GUID producerGuid,
      ProducerPartitionState state) {
    partitionState.getRealtimeTopicProducerStates()
        .computeIfAbsent(kafkaUrl, url -> new VeniceConcurrentHashMap<>())
        .put(guidToUtf8(producerGuid), state);
  }

  public synchronized void removeRealTimeTopicProducerState(String kafkaUrl, GUID producerGuid) {
    if (partitionState.getRealtimeTopicProducerStates().get(kafkaUrl) == null) {
      return;
    }
    partitionState.getRealtimeTopicProducerStates().get(kafkaUrl).remove(guidToUtf8(producerGuid));
  }

  public synchronized ProducerPartitionState getRealTimeProducerState(String kafkaUrl, GUID producerGuid) {
    Map<CharSequence, ProducerPartitionState> map = partitionState.getRealtimeTopicProducerStates().get(kafkaUrl);
    if (map == null) {
      return null;
    }
    return map.get(guidToUtf8(producerGuid));
  }

  private Map<String, Map<CharSequence, ProducerPartitionState>> getRealTimeProducerState() {
    return partitionState.getRealtimeTopicProducerStates();
  }

  public synchronized ProducerPartitionState getProducerPartitionState(GUID producerGuid) {
    return getProducerPartitionStateMap().get(guidToUtf8(producerGuid));
  }

  public void setDatabaseInfo(Map<String, String> databaseInfo) {
    Map<CharSequence, CharSequence> databaseInfoWithRightType = new HashMap<>();
    databaseInfo.forEach((k, v) -> databaseInfoWithRightType.put(k, v));
    this.partitionState.databaseInfo = databaseInfoWithRightType;
  }

  public Map<String, String> getDatabaseInfo() {
    Map<String, String> databaseInfo = new HashMap<>();
    if (this.partitionState.databaseInfo != null) {
      /**
       * It is necessary since the 'string' type deserialized by Avro is {@link Utf8}.
       */
      this.partitionState.databaseInfo.forEach((k, v) -> databaseInfo.put(k.toString(), v.toString()));
    }
    return databaseInfo;
  }

  public void setLeaderTopic(PubSubTopic leaderTopic) {
    this.partitionState.leaderTopic = leaderTopic.getName();
    this.leaderPubSubTopic = leaderTopic;
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

  public PubSubTopic getLeaderTopic(PubSubTopicRepository pubSubTopicRepository) {
    if (this.leaderPubSubTopic == null && this.partitionState.leaderTopic != null) {
      this.leaderPubSubTopic = pubSubTopicRepository.getTopic(this.partitionState.leaderTopic.toString());
    }
    return this.leaderPubSubTopic;
  }

  /**
   * The caller of this API should be interested in the largest known upstream offset.
   *
   * For example, during re-balance, a new leader is elected to consume a partition from
   * scratch; the partition in VT looks like this:
   * SOP, data messages from batch..., EOP, TS, some data messages from RT...
   *
   * Leader shouldn't act on the TS message the moment it consumes TS, but instead, it should consume
   * all the messages in the VT including all the existing real-time messages in VT, in order to resume
   * consumption from RT at the largest known upstream offset to avoid duplicate work. In this case,
   * leader is still consuming VT, so it would return VT offset; users should
   * call this API to get the latest upstream offset.
   */
  public PubSubPosition getCheckpointedRtPosition(String pubSubBrokerAddress) {
    Long offset = partitionState.upstreamOffsetMap.get(pubSubBrokerAddress);
    if (offset == null) {
      // If the offset is not set, return EARLIEST symbolic position.
      return PubSubSymbolicPosition.EARLIEST;
    }
    return fromKafkaOffset(offset);
  }

  public void checkpointRtPosition(String pubSubBrokerAddress, PubSubPosition leaderPosition) {
    partitionState.upstreamRealTimeTopicPubSubPositionMap.put(pubSubBrokerAddress, leaderPosition.toWireFormatBuffer());
    partitionState.upstreamOffsetMap.put(pubSubBrokerAddress, leaderPosition.getNumericOffset());
  }

  /**
   * Update the checkpoint upstream positions map with new values from another map provided as the input.
   * @param newRtPositions
   */
  public void checkpointRtPositions(@Nonnull Map<String, PubSubPosition> newRtPositions) {
    Validate.notNull(newRtPositions);
    for (Map.Entry<String, PubSubPosition> offsetEntry: newRtPositions.entrySet()) {
      // leader offset can be the topic offset from any colo
      this.checkpointRtPosition(offsetEntry.getKey(), offsetEntry.getValue());
    }
  }

  /**
   * Clone the checkpoint upstream positions map to another map provided as the input.
   */
  public void cloneRtPositionCheckpoints(@Nonnull Map<String, PubSubPosition> checkpointUpstreamPositionsReceiver) {
    if (partitionState.upstreamOffsetMap != null && !partitionState.upstreamOffsetMap.isEmpty()) {
      Validate.notNull(checkpointUpstreamPositionsReceiver);
      checkpointUpstreamPositionsReceiver.clear();
      for (Map.Entry<String, Long> offsetEntry: partitionState.upstreamOffsetMap.entrySet()) {
        String pubSubBrokerAddress = offsetEntry.getKey();
        checkpointUpstreamPositionsReceiver.put(pubSubBrokerAddress, fromKafkaOffset(offsetEntry.getValue()));
      }
    }
  }

  public GUID getLeaderGUID() {
    return this.partitionState.leaderGUID;
  }

  public String getLeaderHostId() {
    return (partitionState.leaderHostId != null) ? partitionState.leaderHostId.toString() : null;
  }

  public List<String> getPendingReportIncPushVersionList() {
    if (partitionState.pendingReportIncrementalPushVersions == null) {
      return new ArrayList<>();
    }
    return partitionState.pendingReportIncrementalPushVersions.stream()
        .map(CharSequence::toString)
        .collect(Collectors.toList());
  }

  public void setPendingReportIncPushVersionList(List<String> incPushVersionList) {
    partitionState.pendingReportIncrementalPushVersions = new ArrayList<>(incPushVersionList);
  }

  public Integer getRecordTransformerClassHash() {
    Integer classHash = partitionState.getRecordTransformerClassHash();
    return classHash;
  }

  public void setRecordTransformerClassHash(int classHash) {
    this.partitionState.setRecordTransformerClassHash(classHash);
  }

  // Updated from {@link PartitionTracker#updateOffsetRecord}
  public void setLatestConsumedVtPosition(PubSubPosition latestConsumedVtPosition) {
    this.partitionState.setLastConsumedVersionTopicPubSubPosition(latestConsumedVtPosition.toWireFormatBuffer());
    this.partitionState.setLastConsumedVersionTopicOffset(latestConsumedVtPosition.getNumericOffset());
  }

  public PubSubPosition getLatestConsumedVtPosition() {
    return fromKafkaOffset(this.partitionState.getLastConsumedVersionTopicOffset());
  }

  /**
   * It may be useful to cache this mapping. TODO: Explore GC tuning later.
   *
   * @param guid to be converted
   * @return a {@link Utf8} instance corresponding to the {@link GUID} that was passed in
   */
  CharSequence guidToUtf8(GUID guid) {
    /** TODO: Consider replacing with {@link GuidUtils#getUtf8FromGuid(GUID)}, which might be more efficient. */
    return new Utf8(GuidUtils.getCharSequenceFromGuid(guid));
  }

  @Override
  public String toString() {
    return "OffsetRecord{" + "localVtPosition=" + getCheckpointedLocalVtPosition() + ", rtPositions="
        + getPartitionUpstreamOffsetString() + ", leaderTopic=" + getLeaderTopic() + ", offsetLag=" + getOffsetLag()
        + ", eventTimeEpochMs=" + getMaxMessageTimeInMs() + ", latestProducerProcessingTimeInMs="
        + getLatestProducerProcessingTimeInMs() + ", isEndOfPushReceived=" + isEndOfPushReceived() + ", databaseInfo="
        + getDatabaseInfo() + ", realTimeProducerState=" + getRealTimeProducerState() + ", recordTransformerClassHash="
        + getRecordTransformerClassHash() + ", lastProcessedVtPosition="
        + partitionState.getLastProcessedVersionTopicPubSubPosition() + ", lastConsumedVtPosition="
        + partitionState.getLastConsumedVersionTopicPubSubPosition() + ", remoteVtPosition="
        + partitionState.getUpstreamVersionTopicPubSubPosition() + "}";
  }

  /**
   * This function will print only the critical info inside OffsetRecord, like offset, EOP received; producer DIV info
   * will not be printed.
   */
  public String toSimplifiedString() {
    return "OffsetRecord{" + "localVersionTopicOffset=" + getCheckpointedLocalVtPosition()
        + ", latestProducerProcessingTimeInMs=" + getLatestProducerProcessingTimeInMs() + ", isEndOfPushReceived="
        + isEndOfPushReceived() + ", upstreamOffset=" + getPartitionUpstreamOffsetString() + ", leaderTopic="
        + getLeaderTopic() + '}';
  }

  private String getPartitionUpstreamOffsetString() {
    return this.partitionState.upstreamOffsetMap.toString();
  }

  /**
   * PartitionState will be encoded with an in-house JsonEncoder which would transfer all data with "bytes" schema
   * to hexadecimal strings.
   */
  public String toJsonString() {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      GenericDatumWriter<Object> avroDatumWriter = new GenericDatumWriter<>(PartitionState.SCHEMA$);
      Encoder byteToHexJsonEncoder = new ByteBufferToHexFormatJsonEncoder(PartitionState.SCHEMA$, output);
      avroDatumWriter.write(partitionState, byteToHexJsonEncoder);
      byteToHexJsonEncoder.flush();
      output.flush();
      return new String(output.toByteArray());
    } catch (IOException exception) {
      throw new VeniceException(exception);
    }
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
    return this.partitionState.offset == that.partitionState.offset
        && this.partitionState.endOfPush == that.partitionState.endOfPush; // &&
    // N.B.: We cannot do a more thorough equality check at this time because it breaks tests.
    // If we actually need the more comprehensive equals, then we'll need to rethink the way we test.
    // this.partitionState.producerStates.entrySet().equals(that.partitionState.producerStates.entrySet());
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
