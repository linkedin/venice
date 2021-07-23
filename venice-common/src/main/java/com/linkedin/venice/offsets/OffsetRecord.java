package com.linkedin.venice.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.SubPartitionStatus;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.ByteBufferToHexFormatJsonEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

import static com.linkedin.venice.writer.VeniceWriter.*;


/**
 * If OffsetRecord is initialized with a serializer that contains SchemaReader, old version of server codes
 * will be able to deserialize OffsetRecord that is serialized with a newer protocol version, which can happen
 * after rolling back a server release with new protocol version to an old server release with old protocol version.
 */
public class OffsetRecord {
  private static final Logger logger = Logger.getLogger(OffsetRecord.class);
  // Offset 0 is still a valid offset, Using that will cause a message to be skipped.
  public static final long LOWEST_OFFSET = -1;
  public static final long LOWEST_OFFSET_LAG = 0;
  public static final long DEFAULT_OFFSET_LAG = -1;

  private static final String PARTITION_STATE_STRING = "PartitionState";
  private final PartitionState partitionState;
  private final InternalAvroSpecificSerializer<PartitionState> serializer;

  private final Map<GUID, OffsetRecordTransformer> offsetRecordTransformers = new VeniceConcurrentHashMap<>();

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
    emptyPartitionState.producerStates = new HashMap<>();
    emptyPartitionState.endOfPush = false;
    emptyPartitionState.lastUpdate = 0;
    emptyPartitionState.startOfBufferReplayDestinationOffset = null;
    emptyPartitionState.databaseInfo = new HashMap<>();
    emptyPartitionState.previousStatuses = new HashMap<>();
    emptyPartitionState.leaderOffset = DEFAULT_UPSTREAM_OFFSET;
    emptyPartitionState.upstreamOffsetMap = new VeniceConcurrentHashMap<>();
    return emptyPartitionState;
  }

  private PartitionState deserializePartitionState(byte[] bytes) {
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

  public long getOffsetLag() {
    return this.partitionState.offsetLag;
  }

  public void setOffsetLag(long offsetLag) {
    this.partitionState.offsetLag = offsetLag;
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

  public boolean hasSubPartitionStatus(SubPartitionStatus status) {
    for (CharSequence previousStatus : partitionState.previousStatuses.keySet()) {
      if (status.name().equals(previousStatus.toString())) {
        return true;
      }
    }
    return false;
  }

  public void recordSubPartitionStatus(SubPartitionStatus status) {
    partitionState.previousStatuses.put(status.name(), status.name());
  }

  public Map<CharSequence, CharSequence> getSubPartitionStatus() {
    return partitionState.previousStatuses;
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

  /**
   * "leaderOffset" will be used to track the upstream offset only; the "offset" field
   * in {@link PartitionState} will be used to track the consumption offset in version topic.
   *
   * TODO: rename "leaderOffset" field to "upstreamOffset" field.
   */
  public void setLeaderConsumptionState(String kafkaURL, String topic, long startOffset) {
    this.partitionState.leaderTopic = topic;
    if (!Version.isVersionTopic(topic)) {
      populatePartitionStateWithUpstreamOffset(this.partitionState, startOffset, kafkaURL);
    }
  }

  public void setLeaderUpstreamOffset(String kafkaURL, long leaderOffset) {
    populatePartitionStateWithUpstreamOffset(this.partitionState, leaderOffset, kafkaURL);
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

  /**
   * The caller of this API should be interested in which offset currently leader is actually on;
   * if leader is consuming version topic right now, this API will return the latest record consumed
   * offset for VT; if leader is consuming some upstream topics rather than version topic, this API
   * will return the recorded upstream offset.
   */
  public long getLeaderOffset(String kafkaURL) {
    if (getLeaderTopic() == null || Version.isVersionTopic(getLeaderTopic())) {
      return getOffset();
    } else {
      return getUpstreamOffsetFromPartitionState(this.partitionState, kafkaURL);
    }
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
   * leader is still consuming VT, so {@link #getLeaderOffset(String kafkaURL)} would return VT offset; users should
   * call this API to get the latest upstream offset.
   */
  public long getUpstreamOffset(String kafkaURL) {
    return getUpstreamOffsetFromPartitionState(this.partitionState, kafkaURL);
  }

  public GUID getLeaderGUID() {
    return this.partitionState.leaderGUID;
  }

  public String getLeaderHostId() {
    return (partitionState.leaderHostId != null) ? partitionState.leaderHostId.toString() : null;
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
        ", offsetLag=" + getOffsetLag() +
        ", eventTimeEpochMs=" + getEventTimeEpochMs() +
        ", latestProducerProcessingTimeInMs=" + getLatestProducerProcessingTimeInMs() +
        ", isEndOfPushReceived=" + isEndOfPushReceived() +
        ", databaseInfo=" + getDatabaseInfo() +
        '}';
  }

  /**
   * This function will print only the critical info inside OffsetRecord, like offset, EOP received; producer DIV info
   * will not be printed.
   */
  public String toSimplifiedString() {
    return "OffsetRecord{" +
        "offset=" + getOffset() +
        ", latestProducerProcessingTimeInMs=" + getLatestProducerProcessingTimeInMs() +
        ", isEndOfPushReceived=" + isEndOfPushReceived() +
        ", upstreamOffset=" + getPartitionUpstreamOffsetString() +
        ", leaderTopic=" + getLeaderTopic() +
        '}';
  }

  private String getPartitionUpstreamOffsetString() {
    if (this.partitionState.upstreamOffsetMap.isEmpty()) {
      // Fall back to use the "leaderOffset" field
      return Long.toString(this.partitionState.leaderOffset);
    }
    return this.partitionState.upstreamOffsetMap.toString();
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

  private static void populatePartitionStateWithUpstreamOffset(
      PartitionState partitionState,
      final long upstreamOffset,
      String upstreamKafkaClusterId
  ) {
    partitionState.upstreamOffsetMap.put(upstreamKafkaClusterId, upstreamOffset);
    // Set this field as well so that we can rollback
    partitionState.leaderOffset = upstreamOffset;
  }

  private long getUpstreamOffsetFromPartitionState(PartitionState partitionState, String upstreamKafkaClusterId) {
    if (partitionState.upstreamOffsetMap.get(upstreamKafkaClusterId) == null) {
      logger.warn("The PartitionState.upstreamOffsetMap is " + partitionState.upstreamOffsetMap + ". Fall back to use "
          + "PartitionState.leaderOffset. Full partitionState value: " + partitionState);
      return partitionState.leaderOffset;
    }
    return partitionState.upstreamOffsetMap.get(upstreamKafkaClusterId);
  }
}
