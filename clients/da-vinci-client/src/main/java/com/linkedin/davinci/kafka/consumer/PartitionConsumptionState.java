package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is used to maintain internal state for consumption of each partition.
 */
public class PartitionConsumptionState {
  private final int partition;
  private final int amplificationFactor;
  private final int userPartition;
  private final boolean hybrid;
  private final OffsetRecord offsetRecord;

  private GUID leaderGUID;

  private String leaderHostId;

  /** whether the ingestion of current partition is deferred-write. */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  private boolean completionReported;
  private boolean isSubscribed;
  private boolean isDataRecoveryCompleted;
  private LeaderFollowerStateType leaderFollowerState;

  /**
   * Only used in L/F model. Check if the partition has released the latch.
   * In L/F ingestion task, Optionally, the state model holds a latch that
   * is used to determine when it should transit from offline to follower.
   * The latch is only placed if the Helix resource is serving the read traffic.
   *
   * See {@link LeaderFollowerPartitionStateModel} for the
   * details why we need latch for certain resources.
   */
  private boolean isLatchReleased = false;

  /**
   * This future is completed in drainer thread after persisting the associated record and offset to DB.
   */
  private volatile Future<Void> lastLeaderPersistFuture = null;

  /**
   * In-memory cache for the TopicSwitch in {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState};
   * make sure to keep the in-memory state and StoreVersionState in sync.
   */
  private TopicSwitchWrapper topicSwitch = null;

  /**
   * The following priorities are used to store the progress of processed records since it is not efficient to
   * update offset db for every record.
   */
  private long processedRecordSizeSinceLastSync;

  /**
   * An in-memory state to track whether the leader consumer is consuming from remote or not; it will be updated with
   * correct value during ingestion.
   */
  private boolean consumeRemotely;

  private CheckSum expectedSSTFileChecksum;

  private long latestMessageConsumptionTimestampInMs;

  /**
   * An in-memory state to record the ingestion start time of this partition; in O/O state model, the push timeout clock
   * is reset whenever a new state transition from BOOTSTRAP to ONLINE happens; to achieve a parity feature in L/F state
   * model, the push timeout clock will also reset when the partition consumption state is created for a new partition
   * and check timeout in the consumption state check function which runs regularly:
   * {@link LeaderFollowerStoreIngestionTask#checkLongRunningTaskState()}
   */
  private final long consumptionStartTimeInMs;

  /**
   * This hash map will keep a temporary mapping between a key and it's value.
   * get {@link #getTransientRecord(byte[])} and put {@link #setTransientRecord(int, long, byte[], int, GenericRecord)}
   * operation on this map will be invoked from kafka consumer thread.
   * delete {@link #mayRemoveTransientRecord(int, long, byte[])} operation will be invoked from drainer thread after persisting it in DB.
   * because of the properties of the above operations the caller is guaranteed to get the latest value for a key either from
   * this map or from the DB.
   */
  private final ConcurrentMap<ByteArrayKey, TransientRecord> transientRecordMap = new VeniceConcurrentHashMap<>();

  /**
   * In-memory hash set which keeps track of all previous status this sub-partition has reported. It is the in-memory
   * cache of the previousStatuses field in {@link com.linkedin.venice.kafka.protocol.state.PartitionState} inside
   * {@link OffsetRecord}.
   * The reason to have this HashSet is to maintain correctness and efficiency for the sub-partition status report
   * condition checking. The previousStatus is a generated CharSequence to CharSequence map and we need to iterate over
   * all the records in it to in order to compare with incoming status string. Without explicit locking, this iteration
   * might throw {@link java.util.ConcurrentModificationException} in multi-thread environments.
   */
  private final Set<String> previousStatusSet = VeniceConcurrentHashMap.newKeySet();

  /**
   * This field is used to track whether the last queued record has been fully processed or not.
   * For Leader role, it is redundant from {@literal ProducedRecord#persistedToDBFuture} since it is tracking
   * the completeness end to end, since this field won't be set.
   * This field is useful for O/B/O model or for follower role since the server doesn't need to produce to local Kafka.
   */
  private CompletableFuture<Void> lastQueuedRecordPersistedFuture;

  /**
   * An in-memory state to track whether leader should skip processing the Kafka message. Leader will skip only if the
   * flag is set to true. For example, leader in remote fabric will skip SOBR after EOP in remote VT.
   */
  private boolean skipKafkaMessage = false;

  /**
   * This is an in-memory only map which will track the consumed offset from each kafka cluster. Currently used in
   * measuring hybrid offset lag for each prod region during RT consumption.
   *
   * Key: source Kafka url
   * Value: Latest upstream RT offsets of a specific source consumed by leader
   */
  private final ConcurrentMap<String, Long> consumedUpstreamRTOffsetMap;

  // stores the SOP control message's producer timestamp.
  private long startOfPushTimestamp = 0;

  // stores the EOP control message's producer timestamp.
  private long endOfPushTimestamp = 0;

  /**
   * Latest local version topic offset processed by drainer.
   */
  private long latestProcessedLocalVersionTopicOffset;
  /**
   * Latest upstream version topic offset processed by drainer; if batch native replication source is the same as local
   * region, this tracking offset should remain as -1.
   */
  private long latestProcessedUpstreamVersionTopicOffset;

  /**
   * This keeps track of those offsets which have been screened during conflict resolution. This needs to be kept
   * separate from the drainers notion of per colo offsets because the ingestion task will screen offsets at a higher
   * offset then those which have been drained.  This is used for determining the lag of a leader consumer relative
   * to a source topic when the latest messages have been consumed but not applied due to certain writes not getting
   * applied after losing on conflict resolution to a pre-existing/conflicting write.
   *
   * key: source Kafka url
   * value: Latest ignored upstream RT offset
   */
  private Map<String, Long> latestIgnoredUpstreamRTOffsetMap;

  /**
   * This keeps track of the latest RT offsets from a specific broker which have been produced to VT. When a message
   * is consumed out of RT it can be produced to VT but not yet committed to local storage for the leader.  We only
   * commit a message to a drainer queue after we've produced to the local VT.  This map tracks what messages have been
   * produced.  To find what messages have been committed refer to latestProcessedUpstreamRTOffsetMap.  This map is used
   * for determining if a leader replica is ready to serve or not to avoid the edge case that the end of the RT is full
   * of events which we want to ignore or not apply.  NOTE: This is updated 'before' an ACK is received from Kafka,
   * an offset in this map does not guarantee that the message has successfully made it to VT yet.
   *
   * key: source Kafka url
   * Value: Latest upstream RT offset which has been published to VT
   */
  private Map<String, Long> latestRTOffsetTriedToProduceToVTMap;

  /**
   * Key: source Kafka url
   * Value: Latest upstream RT offsets of a specific source processed by drainer
   */
  private Map<String, Long> latestProcessedUpstreamRTOffsetMap;

  public PartitionConsumptionState(int partition, int amplificationFactor, OffsetRecord offsetRecord, boolean hybrid) {
    this.partition = partition;
    this.amplificationFactor = amplificationFactor;
    this.userPartition = PartitionUtils.getUserPartition(partition, amplificationFactor);
    this.hybrid = hybrid;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.isSubscribed = true;
    this.processedRecordSizeSinceLastSync = 0;
    this.leaderFollowerState = LeaderFollowerStateType.STANDBY;
    this.expectedSSTFileChecksum = null;
    /**
     * Initialize the latest consumption time with current time; otherwise, it's 0 by default
     * and leader will be promoted immediately.
     */
    this.latestMessageConsumptionTimestampInMs = System.currentTimeMillis();
    this.consumptionStartTimeInMs = System.currentTimeMillis();

    // Restore previous status from offset record.
    for (CharSequence status: offsetRecord.getSubPartitionStatus().keySet()) {
      previousStatusSet.add(status.toString());
    }

    // Restore in-memory consumption RT upstream offset map and latest processed RT upstream offset map from the
    // checkpoint upstream offset map
    consumedUpstreamRTOffsetMap = new VeniceConcurrentHashMap<>();
    latestProcessedUpstreamRTOffsetMap = new VeniceConcurrentHashMap<>();
    if (offsetRecord.getLeaderTopic() != null && Version.isRealTimeTopic(offsetRecord.getLeaderTopic())) {
      offsetRecord.cloneUpstreamOffsetMap(consumedUpstreamRTOffsetMap);
      offsetRecord.cloneUpstreamOffsetMap(latestProcessedUpstreamRTOffsetMap);
    }
    // Restore in-memory latest consumed version topic offset and leader info from the checkpoint version topic offset
    this.latestProcessedLocalVersionTopicOffset = offsetRecord.getLocalVersionTopicOffset();
    this.latestProcessedUpstreamVersionTopicOffset = offsetRecord.getCheckpointUpstreamVersionTopicOffset();
    this.leaderHostId = offsetRecord.getLeaderHostId();
    this.leaderGUID = offsetRecord.getLeaderGUID();
    // We don't restore ignored offsets from the persisted offset record today. Doing so would only be useful
    // if it was useful to skip ahead through a large number of dropped offsets at the start of consumption.
    this.latestIgnoredUpstreamRTOffsetMap = new HashMap<>();
    // On start we haven't sent anything
    this.latestRTOffsetTriedToProduceToVTMap = new HashMap<>();
  }

  public int getPartition() {
    return this.partition;
  }

  public int getUserPartition() {
    return userPartition;
  }

  public int getAmplificationFactor() {
    return this.amplificationFactor;
  }

  public OffsetRecord getOffsetRecord() {
    return this.offsetRecord;
  }

  public void setDeferredWrite(boolean deferredWrite) {
    this.deferredWrite = deferredWrite;
  }

  public boolean isDeferredWrite() {
    return this.deferredWrite;
  }

  public boolean isStarted() {
    return getLatestProcessedLocalVersionTopicOffset() > 0;
  }

  public final boolean isEndOfPushReceived() {
    return this.offsetRecord.isEndOfPushReceived();
  }

  public boolean isWaitingForReplicationLag() {
    return isEndOfPushReceived() && !lagCaughtUp;
  }

  public void lagHasCaughtUp() {
    this.lagCaughtUp = true;
  }

  public boolean hasLagCaughtUp() {
    return lagCaughtUp;
  }

  public boolean isCompletionReported() {
    return completionReported;
  }

  public void completionReported() {
    this.completionReported = true;
  }

  public boolean isSubscribed() {
    return isSubscribed;
  }

  public void unsubscribe() {
    this.isSubscribed = false;
  }

  public boolean isLatchReleased() {
    return isLatchReleased;
  }

  public void releaseLatch() {
    this.isLatchReleased = true;
  }

  public void errorReported() {
    this.errorReported = true;
  }

  public boolean isErrorReported() {
    return this.errorReported;
  }

  public boolean isComplete() {
    if (!isEndOfPushReceived()) {
      return false;
    }
    // for regular push store, receiving EOP is good to go
    return !hybrid || lagCaughtUp;
  }

  public final boolean isHybrid() {
    return hybrid;
  }

  public boolean isBatchOnly() {
    return !isHybrid();
  }

  @Override
  public String toString() {
    return new StringBuilder().append("PartitionConsumptionState{")
        .append("partition=")
        .append(partition)
        .append(", hybrid=")
        .append(hybrid)
        .append(", latestProcessedLocalVersionTopicOffset=")
        .append(latestProcessedLocalVersionTopicOffset)
        .append(", latestProcessedUpstreamVersionTopicOffset=")
        .append(latestProcessedUpstreamVersionTopicOffset)
        .append(", latestProcessedUpstreamRTOffsetMap=")
        .append(latestProcessedUpstreamRTOffsetMap)
        .append(", latestIgnoredUpstreamRTOffsetMap=")
        .append(latestIgnoredUpstreamRTOffsetMap)
        .append(", latestRTOffsetTriedToProduceToVTMap")
        .append(latestRTOffsetTriedToProduceToVTMap)
        .append(", offsetRecord=")
        .append(offsetRecord)
        .append(", errorReported=")
        .append(errorReported)
        .append(", started=")
        .append(isStarted())
        .append(", lagCaughtUp=")
        .append(lagCaughtUp)
        .append(", processedRecordSizeSinceLastSync=")
        .append(processedRecordSizeSinceLastSync)
        .append(", leaderFollowerState=")
        .append(leaderFollowerState)
        .append("}")
        .toString();
  }

  public long getProcessedRecordSizeSinceLastSync() {
    return this.processedRecordSizeSinceLastSync;
  }

  public void incrementProcessedRecordSizeSinceLastSync(int recordSize) {
    this.processedRecordSizeSinceLastSync += recordSize;
  }

  public void resetProcessedRecordSizeSinceLastSync() {
    this.processedRecordSizeSinceLastSync = 0;
  }

  public void setLeaderFollowerState(LeaderFollowerStateType state) {
    this.leaderFollowerState = state;
  }

  public final LeaderFollowerStateType getLeaderFollowerState() {
    return this.leaderFollowerState;
  }

  public void setLastLeaderPersistFuture(Future<Void> future) {
    this.lastLeaderPersistFuture = future;
  }

  public Future<Void> getLastLeaderPersistFuture() {
    return this.lastLeaderPersistFuture;
  }

  public CompletableFuture<Void> getLastQueuedRecordPersistedFuture() {
    return lastQueuedRecordPersistedFuture;
  }

  public void setLastQueuedRecordPersistedFuture(CompletableFuture<Void> lastQueuedRecordPersistedFuture) {
    this.lastQueuedRecordPersistedFuture = lastQueuedRecordPersistedFuture;
  }

  /**
   * Update the in-memory state for TopicSwitch whenever encounter a new TopicSwitch message or after a restart.
   */
  public void setTopicSwitch(TopicSwitchWrapper topicSwitch) {
    this.topicSwitch = topicSwitch;
  }

  public TopicSwitchWrapper getTopicSwitch() {
    return this.topicSwitch;
  }

  public void setConsumeRemotely(boolean isConsumingRemotely) {
    this.consumeRemotely = isConsumingRemotely;
  }

  public boolean consumeRemotely() {
    return consumeRemotely;
  }

  public void initializeExpectedChecksum() {
    this.expectedSSTFileChecksum = CheckSum.getInstance(CheckSumType.MD5);
  }

  public void finalizeExpectedChecksum() {
    this.expectedSSTFileChecksum = null;
  }

  /**
   * Keep updating the checksum for key/value pair received from kafka PUT message.
   * If the checksum instance is not configured via {@link PartitionConsumptionState#initializeExpectedChecksum} then do nothing.
   * This api will keep the caller's code clean.
   * @param key
   * @param put
   */
  public void maybeUpdateExpectedChecksum(byte[] key, Put put) {
    if (this.expectedSSTFileChecksum == null) {
      return;
    }
    this.expectedSSTFileChecksum.update(key);
    ByteBuffer putValue = put.putValue;
    this.expectedSSTFileChecksum.update(put.schemaId);
    this.expectedSSTFileChecksum.update(putValue.array(), putValue.position(), putValue.remaining());
  }

  public void resetExpectedChecksum() {
    if (this.expectedSSTFileChecksum != null) {
      this.expectedSSTFileChecksum.reset();
    }
  }

  public byte[] getExpectedChecksum() {
    return this.expectedSSTFileChecksum == null ? null : this.expectedSSTFileChecksum.getCheckSum();
  }

  public long getLatestMessageConsumptionTimestampInMs() {
    return latestMessageConsumptionTimestampInMs;
  }

  public void setLatestMessageConsumptionTimestampInMs(long consumptionTimestampInMs) {
    this.latestMessageConsumptionTimestampInMs = consumptionTimestampInMs;
  }

  public long getConsumptionStartTimeInMs() {
    return consumptionStartTimeInMs;
  }

  public void setTransientRecord(
      int kafkaClusterId,
      long kafkaConsumedOffset,
      byte[] key,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
    setTransientRecord(
        kafkaClusterId,
        kafkaConsumedOffset,
        key,
        null,
        -1,
        -1,
        valueSchemaId,
        replicationMetadataRecord);
  }

  public void setTransientRecord(
      int kafkaClusterId,
      long kafkaConsumedOffset,
      byte[] key,
      byte[] value,
      int valueOffset,
      int valueLen,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
    TransientRecord transientRecord =
        new TransientRecord(value, valueOffset, valueLen, valueSchemaId, kafkaClusterId, kafkaConsumedOffset);
    if (replicationMetadataRecord != null) {
      transientRecord.setReplicationMetadataRecord(replicationMetadataRecord);
    }

    transientRecordMap.put(ByteArrayKey.wrap(key), transientRecord);
  }

  public TransientRecord getTransientRecord(byte[] key) {
    return transientRecordMap.get(ByteArrayKey.wrap(key));
  }

  /**
   * This operation is performed atomically to delete the record only when the provided sourceOffset matches.
   *
   * @param kafkaClusterId
   * @param kafkaConsumedOffset
   * @param key
   * @return
   */
  public TransientRecord mayRemoveTransientRecord(int kafkaClusterId, long kafkaConsumedOffset, byte[] key) {
    TransientRecord removed = transientRecordMap.computeIfPresent(ByteArrayKey.wrap(key), (k, v) -> {
      if (v.kafkaClusterId == kafkaClusterId && v.kafkaConsumedOffset == kafkaConsumedOffset) {
        return null;
      } else {
        return v;
      }
    });
    return removed;
  }

  public int getSourceTopicPartitionNumber(PubSubTopic topic) {
    if (topic.isRealTime()) {
      return getUserPartition();
    } else {
      return getPartition();
    }
  }

  public PubSubTopicPartition getSourceTopicPartition(PubSubTopic topic) {
    /**
     * TODO: Consider whether the {@link PubSubTopicPartition} instance might be cacheable.
     * It might not be easily cacheable if we pass different topics as input param (which it seems we do).
     */
    return new PubSubTopicPartitionImpl(topic, getSourceTopicPartitionNumber(topic));
  }

  public int getTransientRecordMapSize() {
    return transientRecordMap.size();
  }

  public void recordSubPartitionStatus(String subPartitionStatus) {
    if (this.getOffsetRecord() != null) {
      this.getOffsetRecord().recordSubPartitionStatus(subPartitionStatus);
    }
    previousStatusSet.add(subPartitionStatus);
  }

  public boolean skipKafkaMessage() {
    return this.skipKafkaMessage;
  }

  public void setSkipKafkaMessage(boolean skipKafkaMessage) {
    this.skipKafkaMessage = skipKafkaMessage;
  }

  /**
   * This immutable class holds a association between a key and value and the source offset of the consumed message.
   * The value could be either as received in kafka ConsumerRecord or it could be a write computed value.
   */
  public static class TransientRecord {
    private final byte[] value;
    private final int valueOffset;
    private final int valueLen;
    private final int valueSchemaId;
    private final int kafkaClusterId;
    private final long kafkaConsumedOffset;
    private GenericRecord replicationMetadataRecord;

    private ChunkedValueManifest valueManifest;
    private ChunkedValueManifest rmdManifest;

    public TransientRecord(
        byte[] value,
        int valueOffset,
        int valueLen,
        int valueSchemaId,
        int kafkaClusterId,
        long kafkaConsumedOffset) {
      this.value = value;
      this.valueOffset = valueOffset;
      this.valueLen = valueLen;
      this.valueSchemaId = valueSchemaId;
      this.kafkaClusterId = kafkaClusterId;
      this.kafkaConsumedOffset = kafkaConsumedOffset;
    }

    public ChunkedValueManifest getRmdManifest() {
      return rmdManifest;
    }

    public void setRmdManifest(ChunkedValueManifest rmdManifest) {
      this.rmdManifest = rmdManifest;
    }

    public ChunkedValueManifest getValueManifest() {
      return valueManifest;
    }

    public void setValueManifest(ChunkedValueManifest valueManifest) {
      this.valueManifest = valueManifest;
    }

    public void setReplicationMetadataRecord(GenericRecord replicationMetadataRecord) {
      this.replicationMetadataRecord = replicationMetadataRecord;
    }

    public GenericRecord getReplicationMetadataRecord() {
      return replicationMetadataRecord;
    }

    public byte[] getValue() {
      return value;
    }

    public int getValueOffset() {
      return valueOffset;
    }

    public int getValueLen() {
      return valueLen;
    }

    public int getValueSchemaId() {
      return valueSchemaId;
    }
  }

  public void updateLeaderConsumedUpstreamRTOffset(String kafkaUrl, long offset) {
    consumedUpstreamRTOffsetMap.put(kafkaUrl, offset);
  }

  public long getLeaderConsumedUpstreamRTOffset(String kafkaUrl) {
    return consumedUpstreamRTOffsetMap.getOrDefault(kafkaUrl, 0L);
  }

  public void updateLatestProcessedUpstreamRTOffset(String kafkaUrl, long offset) {
    latestProcessedUpstreamRTOffsetMap.put(kafkaUrl, offset);
  }

  public void updateLatestRTOffsetTriedToProduceToVTMap(String kafkaUrl, long offset) {
    latestRTOffsetTriedToProduceToVTMap.put(kafkaUrl, offset);
  }

  public long getLatestRTOffsetTriedToProduceToVTMap(String kafkaUrl) {
    return latestRTOffsetTriedToProduceToVTMap.getOrDefault(kafkaUrl, -1L);
  }

  public void updateLatestIgnoredUpstreamRTOffset(String kafkaUrl, long offset) {
    latestIgnoredUpstreamRTOffsetMap.put(kafkaUrl, offset);
  }

  public long getLatestIgnoredUpstreamRTOffset(String kafkaUrl) {
    return latestIgnoredUpstreamRTOffsetMap.getOrDefault(kafkaUrl, -1L);
  }

  public long getLatestProcessedUpstreamRTOffsetWithIgnoredMessages(String kafkaUrl) {
    long lastOffsetFullyProcessed = getLatestProcessedUpstreamRTOffset(kafkaUrl);
    long lastOffsetIgnored = getLatestIgnoredUpstreamRTOffset(kafkaUrl);
    long offsetTriedToProduceToVT = getLatestRTOffsetTriedToProduceToVTMap(kafkaUrl);

    // we've committed messages at a higher offset then the last thing we ignored. Return the processed offset
    if (lastOffsetFullyProcessed >= lastOffsetIgnored) {
      return lastOffsetFullyProcessed;
    }

    // We have messages which have been ignored at a higher offset then what we've processed but we still have committed
    // all messages to local storage that we've produced to upstream VT. In this case, return the ignored offset as the
    // highest offset. Technically speaking, processed should never be 'greater' then produced, it can at most be
    // 'equal',
    // but we'll include the broader comparison as it's still technically correct.
    if (lastOffsetFullyProcessed >= offsetTriedToProduceToVT) {
      return lastOffsetIgnored;
    }

    // We have messages that we're still waiting to commit, though the most recent messages we've ignored (probably
    // after failing a comparison against record in the transient record cache). In this case we'll return the offset
    // of what's been processed
    return lastOffsetFullyProcessed;

  }

  public long getLatestProcessedUpstreamRTOffset(String kafkaUrl) {
    long latestProcessedUpstreamRTOffset = latestProcessedUpstreamRTOffsetMap.getOrDefault(kafkaUrl, -1L);
    if (latestProcessedUpstreamRTOffset < 0) {
      /**
       * When processing {@link TopicSwitch} control message, only the checkpoint upstream offset maps in {@link OffsetRecord}
       * will be updated, since those offset are not processed yet; so when leader try to get the upstream offsets for the very
       * first time, there are no records in {@link #latestProcessedUpstreamRTOffsetMap} yet.
       */
      return getOffsetRecord().getUpstreamOffset(kafkaUrl);
    }
    return latestProcessedUpstreamRTOffset;
  }

  public Long getLatestProcessedUpstreamRTOffsetWithNoDefault(String kafkaUrl) {
    long latestProcessedUpstreamRTOffset = latestProcessedUpstreamRTOffsetMap.getOrDefault(kafkaUrl, -1L);
    if (latestProcessedUpstreamRTOffset < 0) {
      /**
       * When processing {@link TopicSwitch} control message, only the checkpoint upstream offset maps in {@link OffsetRecord}
       * will be updated, since those offset are not processed yet; so when leader try to get the upstream offsets for the very
       * first time, there are no records in {@link #latestProcessedUpstreamRTOffsetMap} yet.
       */
      return getOffsetRecord().getUpstreamOffsetWithNoDefault(kafkaUrl);
    }
    return latestProcessedUpstreamRTOffset;
  }

  /**
   * The caller of this API should be interested in which offset currently leader should consume from now.
   * 1. If currently leader should consume from real-time topic, return upstream RT offset;
   * 2. if currently leader should consume from version topic, return either remote VT offset or local VT offset, depending
   *    on whether the remote consumption flag is on.
   */
  public long getLeaderOffset(String kafkaURL, PubSubTopicRepository pubSubTopicRepository) {
    PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
    if (leaderTopic != null && !leaderTopic.isVersionTopic()) {
      return getLatestProcessedUpstreamRTOffset(kafkaURL);
    } else {
      return consumeRemotely()
          ? getLatestProcessedUpstreamVersionTopicOffset()
          : getLatestProcessedLocalVersionTopicOffset();
    }
  }

  public void setStartOfPushTimestamp(long startOfPushTimestamp) {
    this.startOfPushTimestamp = startOfPushTimestamp;
  }

  public long getStartOfPushTimestamp() {
    return startOfPushTimestamp;
  }

  public void setEndOfPushTimestamp(long endOfPushTimestamp) {
    this.endOfPushTimestamp = endOfPushTimestamp;
  }

  public long getEndOfPushTimestamp() {
    return endOfPushTimestamp;
  }

  public void updateLatestProcessedLocalVersionTopicOffset(long offset) {
    this.latestProcessedLocalVersionTopicOffset = offset;
  }

  public long getLatestProcessedLocalVersionTopicOffset() {
    return this.latestProcessedLocalVersionTopicOffset;
  }

  public void updateLatestProcessedUpstreamVersionTopicOffset(long offset) {
    this.latestProcessedUpstreamVersionTopicOffset = offset;
  }

  public long getLatestProcessedUpstreamVersionTopicOffset() {
    return this.latestProcessedUpstreamVersionTopicOffset;
  }

  public void setDataRecoveryCompleted(boolean dataRecoveryCompleted) {
    isDataRecoveryCompleted = dataRecoveryCompleted;
  }

  public boolean isDataRecoveryCompleted() {
    return isDataRecoveryCompleted;
  }

  public Map<String, Long> getLatestProcessedUpstreamRTOffsetMap() {
    return this.latestProcessedUpstreamRTOffsetMap;
  }

  public GUID getLeaderGUID() {
    return this.leaderGUID;
  }

  public void setLeaderGUID(GUID leaderGUID) {
    this.leaderGUID = leaderGUID;
  }

  public String getLeaderHostId() {
    return this.leaderHostId;
  }

  public void setLeaderHostId(String hostId) {
    this.leaderHostId = hostId;
  }
}
