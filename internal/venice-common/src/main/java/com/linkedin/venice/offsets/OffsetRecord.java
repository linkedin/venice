package com.linkedin.venice.offsets;

import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_UPSTREAM_OFFSET;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
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
    emptyPartitionState.leaderOffset = DEFAULT_UPSTREAM_OFFSET;
    emptyPartitionState.upstreamOffsetMap = new VeniceConcurrentHashMap<>();
    emptyPartitionState.upstreamVersionTopicOffset = DEFAULT_UPSTREAM_OFFSET;
    emptyPartitionState.pendingReportIncrementalPushVersions = new ArrayList<>();
    emptyPartitionState.setRealtimeTopicProducerStates(new VeniceConcurrentHashMap<>());
    return emptyPartitionState;
  }

  private PartitionState deserializePartitionState(byte[] bytes) {
    return serializer.deserialize(PARTITION_STATE_STRING, bytes);
  }

  public long getLocalVersionTopicOffset() {
    return this.partitionState.offset;
  }

  public void setCheckpointLocalVersionTopicOffset(long offset) {
    this.partitionState.offset = offset;
  }

  public long getCheckpointUpstreamVersionTopicOffset() {
    return this.partitionState.upstreamVersionTopicOffset;
  }

  public void setCheckpointUpstreamVersionTopicOffset(long upstreamVersionTopicOffset) {
    this.partitionState.upstreamVersionTopicOffset = upstreamVersionTopicOffset;
  }

  public long getOffsetLag() {
    return this.partitionState.offsetLag;
  }

  public void setOffsetLag(long offsetLag) {
    this.partitionState.offsetLag = offsetLag;
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

  public void endOfPushReceived(long endOfPushOffset) {
    if (endOfPushOffset < 1) {
      // Even an empty push should have a SOP and EOP, so offset 1 is the absolute minimum.
      throw new IllegalArgumentException("endOfPushOffset cannot be < 1.");
    }
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

  public void setLeaderUpstreamOffset(String upstreamKafkaURL, long leaderOffset) {
    partitionState.upstreamOffsetMap.put(upstreamKafkaURL, leaderOffset);
    // Set this field as well so that we can rollback
    partitionState.leaderOffset = leaderOffset;
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
  public long getUpstreamOffset(String kafkaURL) {
    Long upstreamOffset = getUpstreamOffsetFromPartitionState(this.partitionState, kafkaURL);
    if (upstreamOffset == null) {
      return partitionState.leaderOffset;
    }
    return upstreamOffset;
  }

  public Long getUpstreamOffsetWithNoDefault(String kafkaURL) {
    return getUpstreamOffsetFromPartitionState(this.partitionState, kafkaURL);
  }

  /**
   * Clone the checkpoint upstream offset map to another map provided as the input.
   */
  public void cloneUpstreamOffsetMap(@Nonnull Map<String, Long> checkpointUpstreamOffsetMapReceiver) {
    if (partitionState.upstreamOffsetMap != null && !partitionState.upstreamOffsetMap.isEmpty()) {
      Validate.notNull(checkpointUpstreamOffsetMapReceiver);
      checkpointUpstreamOffsetMapReceiver.clear();
      checkpointUpstreamOffsetMapReceiver.putAll(partitionState.upstreamOffsetMap);
    }
  }

  /**
   * Reset the checkpoint upstream offset map to another map provided as the input.
   * @param checkpointUpstreamOffsetMap
   */
  public void resetUpstreamOffsetMap(@Nonnull Map<String, Long> checkpointUpstreamOffsetMap) {
    Validate.notNull(checkpointUpstreamOffsetMap);
    for (Map.Entry<String, Long> offsetEntry: checkpointUpstreamOffsetMap.entrySet()) {
      // leader offset can be the topic offset from any colo
      this.setLeaderUpstreamOffset(offsetEntry.getKey(), offsetEntry.getValue());
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

  /**
   * It may be useful to cache this mapping. TODO: Explore GC tuning later.
   *
   * @param guid to be converted
   * @return a {@link Utf8} instance corresponding to the {@link GUID} that was passed in
   */
  CharSequence guidToUtf8(GUID guid) {
    return new Utf8(GuidUtils.getCharSequenceFromGuid(guid));
  }

  @Override
  public String toString() {
    return "OffsetRecord{" + "localVersionTopicOffset=" + getLocalVersionTopicOffset() + ", upstreamOffset="
        + getPartitionUpstreamOffsetString() + ", leaderTopic=" + getLeaderTopic() + ", offsetLag=" + getOffsetLag()
        + ", eventTimeEpochMs=" + getMaxMessageTimeInMs() + ", latestProducerProcessingTimeInMs="
        + getLatestProducerProcessingTimeInMs() + ", isEndOfPushReceived=" + isEndOfPushReceived() + ", databaseInfo="
        + getDatabaseInfo() + ", realTimeProducerState=" + getRealTimeProducerState() + '}';
  }

  /**
   * This function will print only the critical info inside OffsetRecord, like offset, EOP received; producer DIV info
   * will not be printed.
   */
  public String toSimplifiedString() {
    return "OffsetRecord{" + "localVersionTopicOffset=" + getLocalVersionTopicOffset()
        + ", latestProducerProcessingTimeInMs=" + getLatestProducerProcessingTimeInMs() + ", isEndOfPushReceived="
        + isEndOfPushReceived() + ", upstreamOffset=" + getPartitionUpstreamOffsetString() + ", leaderTopic="
        + getLeaderTopic() + '}';
  }

  private String getPartitionUpstreamOffsetString() {
    if (this.partitionState.upstreamOffsetMap.isEmpty()) {
      // Fall back to use the "leaderOffset" field
      return Long.toString(this.partitionState.leaderOffset);
    }
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

  /**
   * Return the single entry value from upstreamOffsetMap in native replication.
   */
  private Long getUpstreamOffsetFromPartitionState(PartitionState partitionState, String kafkaURL) {
    return partitionState.upstreamOffsetMap.get(kafkaURL);
  }
}
