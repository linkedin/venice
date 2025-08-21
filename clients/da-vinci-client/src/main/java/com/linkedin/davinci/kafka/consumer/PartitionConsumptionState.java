package com.linkedin.davinci.kafka.consumer;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.avro.generic.GenericRecord;


/**
 * In-memory state that represents a replica's view of partition consumption.
 *
 * <p>This class tracks everything the replica cares about: how far it's consumed
 * from the real-time (RT) and version topics (VT), what data has been processed,
 * and what's been committed.
 *
 * <p>This state is not durable — it's periodically checkpointed by updating the
 * {@link OffsetRecord}, which wraps the persisted {@code PartitionState} on disk.
 * Note: OffsetRecord is not persisted to disk until the flush/sync operation is called.
 *
 * <p>When the replica is the leader for the partition, RT and remote VT positions are
 * updated by directly consuming from the corresponding topics. For followers, these
 * positions are derived from the leader footer attached to every version topic message,
 * or from global DIV snapshots — if global DIV is enabled.
 */
public class PartitionConsumptionState {
  private static final int MAX_INCREMENTAL_PUSH_ENTRY_NUM = 50;
  private static final long DEFAULT_HEARTBEAT_LAG_THRESHOLD_MS = MINUTES.toMillis(2); // Default is 2 minutes.
  private static final String PREVIOUSLY_READY_TO_SERVE = "previouslyReadyToServe";
  private static final String TRUE = "true";

  private final String replicaId;
  private final int partition;
  private final boolean hybrid;
  private final OffsetRecord offsetRecord;
  private final PubSubContext pubSubContext;

  private GUID leaderGUID;

  private String leaderHostId;

  /**
   * whether the ingestion of current partition is deferred-write.
   * Refer {@code com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition#deferredWrite}
   */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  /**
   * Save the time when the lag is caught up to record nearline metrics
   * for messages produced after this time to ignore the old messages that are
   * getting caught up after a push.
   */
  private long lagCaughtUpTimeInMs;
  private boolean completionReported;
  private boolean isSubscribed;
  private boolean isDataRecoveryCompleted;
  private LeaderFollowerStateType leaderFollowerState;

  /**
   * The VT produce future should be read/set by the same consumer thread during normal operation. Making it volatile
   * since the SIT thread might want to shortcircuit this future when closing {@link LeaderFollowerStoreIngestionTask}.
   */
  private volatile CompletableFuture<Void> lastVTProduceCallFuture;

  /**
   * State machine that can only transition to LATCH_CREATED if LatchStatus is NONE, and transition to LATCH_RELEASED
   * if LatchStatus is LATCH_CREATED. The latch will only be created in {@link LeaderFollowerPartitionStateModel} if
   * consumption begins on a partition that is the current version. It will not be created if the future version
   * (being consumed) later becomes the current version.
   */
  enum LatchStatus {
    NONE, LATCH_CREATED, LATCH_RELEASED
  }

  /**
   * Only used in L/F model. Check if the partition has released the latch.
   * In L/F ingestion task, Optionally, the state model holds a latch that
   * is used to determine when it should transit from offline to follower.
   * The latch is only placed if the Helix resource is serving the read traffic.
   *
   * See {@link LeaderFollowerPartitionStateModel} for the
   * details why we need latch for certain resources.
   */
  private final AtomicReference<LatchStatus> latchStatus = new AtomicReference<>(LatchStatus.NONE);

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

  /**
   * The timestamp when consumption thread is about to process the "latest" message in topic/partition;
   * Note that when retrieve the timestamp, the new message can be appended to the partition already, because it's
   * only update when this "latest" message is already produced to kafka or queued to drainer.
   * so it's not guaranteed that this timestamp is from the last message.
   */
  private long latestMessageConsumedTimestampInMs;

  /**
   * The timestamp when consumption thread is about to process the "latest" message in topic/partition;
   * The difference between this field and {@link #latestMessageConsumedTimestampInMs} is that this field is updated
   * whenever there's a new record for this topic/partition, while the other field is updated only when the record is
   * fully processed.
   */
  private long latestPolledMessageTimestampInMs;

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
   * get {@link #getTransientRecord(byte[])} and put {@link #setTransientRecord(int, PubSubPosition, byte[], int, GenericRecord)}
   * operation on this map will be invoked from kafka consumer thread.
   * delete {@link #mayRemoveTransientRecord(int, PubSubPosition, byte[])} operation will be invoked from drainer thread after persisting it in DB.
   * because of the properties of the above operations the caller is guaranteed to get the latest value for a key either from
   * this map or from the DB.
   */
  private final ConcurrentMap<ByteArrayKey, TransientRecord> transientRecordMap = new VeniceConcurrentHashMap<>();

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

  // stores the SOP control message's producer timestamp.
  private long startOfPushTimestamp = 0;

  // stores the EOP control message's producer timestamp.
  private long endOfPushTimestamp = 0;

  /**
   * Tracks the latest real-time topic position consumed by the leader from each upstream PubSub source.
   *
   * This is an in-memory-only map used primarily for measuring hybrid offset lag per region during
   * real-time (RT) consumption. The values reflect the most recent positions observed by the consumer,
   * regardless of whether they have been persisted or validated.
   *
   * Key: source PubSub broker address
   * Value: latest consumed real-time topic position from that source
   */
  private final ConcurrentMap<String, PubSubPosition> latestConsumedRtPositions;

  /**
   * Tracks the last real-time topic position consumed from each upstream broker
   * till which Global Data Integrity Validation (DIV) has been successfully generated.
   *
   * This in-memory map reflects the highest position per broker up to which both
   * record consumption and DIV checkpointing were completed. It is restored from
   * disk during startup and used when the client subscribes to real-time topics,
   * if global DIV is enabled.
   *
   * Key: source PubSub broker address
   * Value: last consumed real-time topic position with valid DIV
   */
  private final ConcurrentMap<String, PubSubPosition> divRtCheckpointPositions;

  /**
   * Tracks the position of the last local version topic record processed
   * and committed to the database by the drainer. This value is not durable
   * until explicitly persisted, which happens periodically via offset record
   * updates and checkpointing to disk.
   */
  private PubSubPosition latestProcessedVtPosition;
  /**
   * Tracks the latest upstream  (remote) version topic position processed by the drainer.
   * If the batch native replication source is the same as the local region,
   * this should remain {@link PubSubSymbolicPosition#EARLIEST}. Otherwise, it
   * should be updated to reflect the actual upstream position.
   */
  private PubSubPosition latestProcessedRemoteVtPosition;

  /**
   * Tracks the latest real-time topic position processed by the drainer for each upstream source.
   *
   * Keyed by the source PubSub broker address (see {@link com.linkedin.venice.ConfigKeys#PUBSUB_BROKER_ADDRESS}),
   * this in-memory map reflects the most recent real-time message position received from each upstream region.
   *
   * This state is not durable until it's flushed—typically done by updating the corresponding
   * offset records and checkpointing them to disk. If the drainer has not yet received any messages
   * from a given source, its entry may be absent or initialized to {@link PubSubSymbolicPosition#EARLIEST}.
   */
  private final Map<String, PubSubPosition> latestProcessedRtPositions;

  private LeaderCompleteState leaderCompleteState;
  private long lastLeaderCompleteStateUpdateInMs;

  private List<String> pendingReportIncPushVersionList;

  // veniceWriterLazyRef could be set and get in different threads, mark it volatile.
  private volatile Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterLazyRef;

  private BooleanSupplier isCurrentVersion;

  private long readyToServeTimeLagThresholdInMs = DEFAULT_HEARTBEAT_LAG_THRESHOLD_MS;

  public PartitionConsumptionState(
      String replicaId,
      int partition,
      OffsetRecord offsetRecord,
      PubSubContext pubSubContext,
      boolean hybrid) {
    this.replicaId = replicaId;
    this.partition = partition;
    this.hybrid = hybrid;
    this.offsetRecord = offsetRecord;
    this.pubSubContext = pubSubContext;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.lagCaughtUpTimeInMs = 0;
    this.completionReported = false;
    this.isSubscribed = true;
    this.processedRecordSizeSinceLastSync = 0;
    this.leaderFollowerState = LeaderFollowerStateType.STANDBY;
    this.expectedSSTFileChecksum = null;
    /**
     * Initialize the latest consumed time with current time; otherwise, it's 0 by default
     * and leader will be promoted immediately.
     */
    long currentTimeInMs = System.currentTimeMillis();
    this.latestMessageConsumedTimestampInMs = currentTimeInMs;
    this.latestPolledMessageTimestampInMs = currentTimeInMs;
    this.consumptionStartTimeInMs = currentTimeInMs;

    // Restore in-memory consumption RT positions and latest processed RT
    // positions from the checkpoint upstream positions map
    latestConsumedRtPositions = new VeniceConcurrentHashMap<>(3);
    divRtCheckpointPositions = new VeniceConcurrentHashMap<>(3);
    latestProcessedRtPositions = new VeniceConcurrentHashMap<>(3);
    if (offsetRecord.getLeaderTopic() != null && Version.isRealTimeTopic(offsetRecord.getLeaderTopic())) {
      offsetRecord.cloneRtPositionCheckpoints(latestConsumedRtPositions);
      offsetRecord.cloneRtPositionCheckpoints(latestProcessedRtPositions);
    }
    // Restore in-memory latest consumed version topic position and leader info from the checkpoint version topic
    // position
    this.latestProcessedVtPosition = offsetRecord.getCheckpointedLocalVtPosition();
    this.latestProcessedRemoteVtPosition = offsetRecord.getCheckpointedRemoteVtPosition();
    this.leaderHostId = offsetRecord.getLeaderHostId();
    this.leaderGUID = offsetRecord.getLeaderGUID();
    this.lastVTProduceCallFuture = CompletableFuture.completedFuture(null);
    this.leaderCompleteState = LeaderCompleteState.LEADER_NOT_COMPLETED;
    this.lastLeaderCompleteStateUpdateInMs = 0;
    this.pendingReportIncPushVersionList = offsetRecord.getPendingReportIncPushVersionList();
  }

  public int getPartition() {
    return this.partition;
  }

  public CompletableFuture<Void> getLastVTProduceCallFuture() {
    return this.lastVTProduceCallFuture;
  }

  public void setLastVTProduceCallFuture(CompletableFuture<Void> lastVTProduceCallFuture) {
    this.lastVTProduceCallFuture = lastVTProduceCallFuture;
  }

  public void setCurrentVersionSupplier(BooleanSupplier isCurrentVersion) {
    this.isCurrentVersion = isCurrentVersion;
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
    return !PubSubSymbolicPosition.EARLIEST.equals(getLatestProcessedVtPosition());
  }

  public final boolean isEndOfPushReceived() {
    return this.offsetRecord.isEndOfPushReceived();
  }

  public boolean isWaitingForReplicationLag() {
    return isEndOfPushReceived() && !lagCaughtUp;
  }

  public void lagHasCaughtUp() {
    if (!this.lagCaughtUp) {
      this.lagCaughtUp = true;
      this.lagCaughtUpTimeInMs = System.currentTimeMillis();
    }
  }

  public boolean isCurrentVersion() {
    return isCurrentVersion.getAsBoolean();
  }

  public boolean hasLagCaughtUp() {
    return lagCaughtUp;
  }

  /**
   * check to ignore calculating latency from pubsub broker to ready to serve for
   * messages that are getting caught up from previous pushes.
   * @param producerTimeStampInMs timestamp of the message
   */
  public boolean isNearlineMetricsRecordingValid(long producerTimeStampInMs) {
    return (lagCaughtUp && lagCaughtUpTimeInMs > 0 && producerTimeStampInMs > lagCaughtUpTimeInMs);
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

  public boolean isLatchCreated() {
    return latchStatus.get() != LatchStatus.NONE;
  }

  public void setLatchCreated() {
    latchStatus.compareAndSet(LatchStatus.NONE, LatchStatus.LATCH_CREATED);
  }

  public boolean isLatchReleased() {
    return latchStatus.get() == LatchStatus.LATCH_RELEASED;
  }

  public void releaseLatch() {
    latchStatus.compareAndSet(LatchStatus.LATCH_CREATED, LatchStatus.LATCH_RELEASED);
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
    return new StringBuilder().append("PCS{")
        .append("replicaId=")
        .append(replicaId)
        .append(", hybrid=")
        .append(hybrid)
        .append(", latestProcessedVtPosition=")
        .append(latestProcessedVtPosition)
        .append(", latestProcessedRemoteVtPosition=")
        .append(latestProcessedRemoteVtPosition)
        .append(", latestProcessedRtPositions=")
        .append(latestProcessedRtPositions)
        .append(", offsetRecord=")
        .append(offsetRecord)
        .append(", errorReported=")
        .append(errorReported)
        .append(", started=")
        .append(isStarted())
        .append(", lagCaughtUp=")
        .append(lagCaughtUp)
        .append(", isDeferredWrite=")
        .append(deferredWrite)
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

  public Lazy<VeniceWriter<byte[], byte[], byte[]>> getVeniceWriterLazyRef() {
    return veniceWriterLazyRef;
  }

  public void setVeniceWriterLazyRef(Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterLazyRef) {
    this.veniceWriterLazyRef = veniceWriterLazyRef;
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
    /**
     * 1. For regular value and value chunk, we will take the value payload.
     * 2. When A/A partial update is enabled, RMD chunking is turned on, we should skip the RMD chunk.
     * 3. For chunk manifest, if RMD chunking is enabled, RMD manifest will be in the RMD payload. No matter it is RMD
     * chunking or not, we should only take the value manifest part.
     */
    if (put.schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion() && put.putValue.remaining() == 0) {
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

  public long getLatestMessageConsumedTimestampInMs() {
    return latestMessageConsumedTimestampInMs;
  }

  public void setLatestMessageConsumedTimestampInMs(long consumedTimestampInMs) {
    this.latestMessageConsumedTimestampInMs = consumedTimestampInMs;
  }

  public long getLatestPolledMessageTimestampInMs() {
    return latestPolledMessageTimestampInMs;
  }

  public void setLatestPolledMessageTimestampInMs(long timestampInMs) {
    this.latestPolledMessageTimestampInMs = timestampInMs;
  }

  public long getConsumptionStartTimeInMs() {
    return consumptionStartTimeInMs;
  }

  public void setTransientRecord(
      int kafkaClusterId,
      PubSubPosition consumedPosition,
      byte[] key,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
    setTransientRecord(kafkaClusterId, consumedPosition, key, null, -1, -1, valueSchemaId, replicationMetadataRecord);
  }

  public void setTransientRecord(
      int kafkaClusterId,
      PubSubPosition consumedPosition,
      byte[] key,
      byte[] value,
      int valueOffset,
      int valueLen,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
    TransientRecord transientRecord =
        new TransientRecord(value, valueOffset, valueLen, valueSchemaId, kafkaClusterId, consumedPosition);
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
   * @param recordPosition
   * @param key
   * @return
   */
  public TransientRecord mayRemoveTransientRecord(int kafkaClusterId, PubSubPosition recordPosition, byte[] key) {
    return transientRecordMap.computeIfPresent(ByteArrayKey.wrap(key), (k, v) -> {
      if (v.kafkaClusterId == kafkaClusterId && v.consumedPosition == recordPosition) {
        return null;
      } else {
        return v;
      }
    });
  }

  public PubSubTopicPartition getSourceTopicPartition(PubSubTopic topic) {
    /**
     * TODO: Consider whether the {@link PubSubTopicPartition} instance might be cacheable.
     * It might not be easily cacheable if we pass different topics as input param (which it seems we do).
     */
    return new PubSubTopicPartitionImpl(topic, getPartition());
  }

  public int getTransientRecordMapSize() {
    return transientRecordMap.size();
  }

  public boolean skipKafkaMessage() {
    return this.skipKafkaMessage;
  }

  public void setSkipKafkaMessage(boolean skipKafkaMessage) {
    this.skipKafkaMessage = skipKafkaMessage;
  }

  /**
   * This persists to the offsetRecord associated to this partitionConsumptionState that the ready to serve check has
   * passed.  This will be persisted to disk once the offsetRecord is checkpointed, and subsequent restarts will
   * consult this information when determining if the node should come online or not to serve traffic
   */
  public void recordReadyToServeInOffsetRecord() {
    offsetRecord.setPreviousStatusesEntry(PREVIOUSLY_READY_TO_SERVE, TRUE);
  }

  public boolean getReadyToServeInOffsetRecord() {
    return TRUE.equals(offsetRecord.getPreviousStatusesEntry(PREVIOUSLY_READY_TO_SERVE));
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
    private final PubSubPosition consumedPosition;
    private GenericRecord replicationMetadataRecord;

    private ChunkedValueManifest valueManifest;
    private ChunkedValueManifest rmdManifest;

    public TransientRecord(
        byte[] value,
        int valueOffset,
        int valueLen,
        int valueSchemaId,
        int kafkaClusterId,
        PubSubPosition consumedPosition) {
      this.value = value;
      this.valueOffset = valueOffset;
      this.valueLen = valueLen;
      this.valueSchemaId = valueSchemaId;
      this.kafkaClusterId = kafkaClusterId;
      this.consumedPosition = consumedPosition;
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

  /**
   * Updates the in-memory latest real-time topic position consumed from the given
   * upstream PubSub broker.
   *
   * This position reflects the most recent message observed by the leader during
   * real-time (RT) consumption and is used for tracking ingestion progress.
   *
   * @param pubSubBrokerAddress the source PubSub broker address
   * @param lastConsumedRtPosition the latest consumed real-time topic position
   */
  public void setLatestConsumedRtPosition(String pubSubBrokerAddress, PubSubPosition lastConsumedRtPosition) {
    latestConsumedRtPositions.put(pubSubBrokerAddress, lastConsumedRtPosition);
  }

  /**
   * Retrieves the latest real-time topic position consumed from the given
   * upstream PubSub broker.
   * If no position is recorded, returns {@link PubSubSymbolicPosition#EARLIEST}
   * as the default.
   *
   * @param pubSubBrokerAddress the source PubSub broker address
   * @return the latest consumed real-time topic position, or EARLIEST if not present
   */
  public PubSubPosition getLatestConsumedRtPosition(String pubSubBrokerAddress) {
    // TODO(sushantmane): We changed default value from 0L to EARLIEST; remember this during troubleshooting in
    // case of any issues. We don't have access (or construct) to Zeroth PubSubPosition.
    return latestConsumedRtPositions.getOrDefault(pubSubBrokerAddress, PubSubSymbolicPosition.EARLIEST);
  }

  /**
   * Sets the real-time topic position till which Data Integrity Validation (DIV)
   * has been checkpointed for the given upstream broker.
   *
   * @param pubSubBrokerAddress the source PubSub broker address
   * @param divRtCheckpointPosition the position till which DIV has been generated and persisted
   */
  public void setDivRtCheckpointPosition(String pubSubBrokerAddress, PubSubPosition divRtCheckpointPosition) {
    divRtCheckpointPositions.put(pubSubBrokerAddress, divRtCheckpointPosition);
  }

  /**
   * Returns the real-time topic position till which Data Integrity Validation (DIV)
   * has been checkpointed for the given upstream broker.
   *
   * If no checkpoint exists for the broker, {@link PubSubSymbolicPosition#EARLIEST} is returned.
   *
   * @param pubSubBrokerAddress the source PubSub broker address
   * @return the checkpointed position for DIV, or EARLIEST if not found
   */
  public PubSubPosition getDivRtCheckpointPosition(String pubSubBrokerAddress) {
    return divRtCheckpointPositions.getOrDefault(pubSubBrokerAddress, PubSubSymbolicPosition.EARLIEST);
  }

  /**
   * Sets the latest processed local version topic position.
   *
   * @param vtPosition the version topic position to set
   */
  public void setLatestProcessedVtPosition(PubSubPosition vtPosition) {
    this.latestProcessedVtPosition = vtPosition;
  }

  /**
   * Returns the latest processed local version topic position.
   *
   * @return the current version topic position
   */
  public PubSubPosition getLatestProcessedVtPosition() {
    return this.latestProcessedVtPosition;
  }

  /**
   * Sets the latest processed upstream version topic position.
   *
   * @param upstreamVtPosition the upstream version topic position to set
   */
  public void setLatestProcessedRemoteVtPosition(PubSubPosition upstreamVtPosition) {
    this.latestProcessedRemoteVtPosition = upstreamVtPosition;
  }

  /**
   * Returns the latest processed upstream version topic position.
   *
   * @return the current upstream version topic position
   */
  public PubSubPosition getLatestProcessedRemoteVtPosition() {
    return this.latestProcessedRemoteVtPosition;
  }

  /**
   * Updates the in-memory latest real-time topic position for the given upstream
   * PubSub broker address.
   *
   * @param pubSubBrokerAddress the source PubSub broker address
   * @param rtPosition the latest real-time topic position to set
   */
  public void setLatestProcessedRtPosition(String pubSubBrokerAddress, PubSubPosition rtPosition) {
    latestProcessedRtPositions.put(pubSubBrokerAddress, rtPosition);
  }

  /**
   * Retrieves the latest real-time topic position processed by the drainer for a given
   * upstream PubSub broker address.
   *
   * <p>This method first checks the in-memory state stored in
   * {@code latestProcessedUpstreamRtPositions}, which reflects positions already processed
   * and tracked by the drainer. If no entry is found, or if the position is
   * {@link PubSubSymbolicPosition#EARLIEST}, it falls back to the offset record, which is
   * periodically flushed to disk and may contain checkpointed upstream offsets.
   *
   * <p>This fallback is necessary during initial processing of {@link TopicSwitch} control messages,
   * where upstream offsets are written only to the {@link OffsetRecord} before actual records
   * are processed. In such cases, the in-memory map may still be uninitialized.
   *
   * @param pubSubBrokerAddress the source PubSub broker address whose position is being queried
   * @return the latest real-time topic position for the given broker address
   */
  public PubSubPosition getLatestProcessedRtPosition(String pubSubBrokerAddress) {
    PubSubPosition rtPosition =
        getLatestProcessedRtPositions().getOrDefault(pubSubBrokerAddress, PubSubSymbolicPosition.EARLIEST);
    if (PubSubSymbolicPosition.EARLIEST.equals(rtPosition)) {
      /**
       * When processing {@link TopicSwitch} control message, only the checkpoint upstream offset maps in {@link OffsetRecord}
       * will be updated, since those offset are not processed yet; so when leader try to get the upstream offsets for the very
       * first time, there are no records in {@link #latestProcessedRtPositions} yet.
       */
      return getOffsetRecord().getCheckpointedRtPosition(pubSubBrokerAddress);
    }
    return rtPosition;
  }

  /**
   * Returns the in-memory map of latest real-time topic positions processed by the drainer
   * for each upstream PubSub broker address.
   *
   * @return a map from PubSub broker address to the latest processed real-time topic position
   */
  public Map<String, PubSubPosition> getLatestProcessedRtPositions() {
    return this.latestProcessedRtPositions;
  }

  /**
   * Returns the position the leader should consume from, based on the current leader topic
   * and whether real-time (RT) or version topic (VT) consumption is active.
   *
   * <p>If the leader topic is an RT topic:
   * <ul>
   *   <li>If {@code useCheckpointedDivRtPosition} is true, returns the last checkpointed
   *       RT position from global DIV (LCRO).</li>
   *   <li>Otherwise, returns the latest processed RT position from in-memory state (or OffsetRecord if required)</li>
   * </ul>
   *
   * <p>If the leader topic is a version topic:
   * <ul>
   *   <li>If remote consumption is enabled, returns the latest processed remote VT position.</li>
   *   <li>Otherwise, returns the latest processed local VT position.</li>
   * </ul>
   *
   * @param pubSubBrokerAddress the upstream PubSub broker address
   * @param useCheckpointedDivRtPosition whether to use the global DIV checkpoint (LCRO) as the RT start position
   * @return the position the leader should consume from
   */
  public PubSubPosition getLeaderPosition(String pubSubBrokerAddress, boolean useCheckpointedDivRtPosition) {
    PubSubTopic leaderTopic = getOffsetRecord().getLeaderTopic(getPubSubContext().getPubSubTopicRepository());
    if (leaderTopic != null && !leaderTopic.isVersionTopic()) {
      // consumed corresponds to messages seen by consumer, processed corresponds to messages seen by drainer
      return (useCheckpointedDivRtPosition)
          ? getDivRtCheckpointPosition(pubSubBrokerAddress)
          : getLatestProcessedRtPosition(pubSubBrokerAddress);
    } else {
      return consumeRemotely() ? getLatestProcessedRemoteVtPosition() : getLatestProcessedVtPosition();
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

  public PubSubPosition getLatestConsumedVtPosition() {
    return offsetRecord.getLatestConsumedVtPosition();
  }

  public void setDataRecoveryCompleted(boolean dataRecoveryCompleted) {
    isDataRecoveryCompleted = dataRecoveryCompleted;
  }

  public boolean isDataRecoveryCompleted() {
    return isDataRecoveryCompleted;
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

  public boolean isLeaderCompleted() {
    return getLeaderCompleteState() == LeaderCompleteState.LEADER_COMPLETED;
  }

  public LeaderCompleteState getLeaderCompleteState() {
    return leaderCompleteState;
  }

  public void setLeaderCompleteState(LeaderCompleteState leaderCompleteState) {
    this.leaderCompleteState = leaderCompleteState;
  }

  public long getLastLeaderCompleteStateUpdateInMs() {
    return lastLeaderCompleteStateUpdateInMs;
  }

  public void setLastLeaderCompleteStateUpdateInMs(long lastLeaderCompleteStateUpdateInMs) {
    this.lastLeaderCompleteStateUpdateInMs = lastLeaderCompleteStateUpdateInMs;
  }

  public String getReplicaId() {
    return replicaId;
  }

  public void addIncPushVersionToPendingReportList(String incPushVersion) {
    pendingReportIncPushVersionList.add(incPushVersion);
    /**
     * We will perform filtering on batch inc push to report, as by original design we will only keep latest 50 inc push
     * entries.
     */
    int versionCount = pendingReportIncPushVersionList.size();
    if (versionCount > MAX_INCREMENTAL_PUSH_ENTRY_NUM) {
      pendingReportIncPushVersionList =
          pendingReportIncPushVersionList.subList(versionCount - MAX_INCREMENTAL_PUSH_ENTRY_NUM, versionCount);
    }
    getOffsetRecord().setPendingReportIncPushVersionList(pendingReportIncPushVersionList);
  }

  public List<String> getPendingReportIncPushVersionList() {
    return pendingReportIncPushVersionList;
  }

  public void clearPendingReportIncPushVersionList() {
    pendingReportIncPushVersionList.clear();
    offsetRecord.setPendingReportIncPushVersionList(pendingReportIncPushVersionList);
  }

  public PubSubContext getPubSubContext() {
    return pubSubContext;
  }

  public long getReadyToServeTimeLagThresholdInMs() {
    return readyToServeTimeLagThresholdInMs;
  }

  public void setReadyToServeTimeLagThresholdInMs(long readyToServeTimeLagThresholdInMs) {
    this.readyToServeTimeLagThresholdInMs = readyToServeTimeLagThresholdInMs;
  }
}
