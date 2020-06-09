package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.offsets.OffsetRecord;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * This class is used to maintain internal state for consumption of each partition.
 */
class PartitionConsumptionState {
  private final int partition;
  private final boolean hybrid;
  private final boolean isIncrementalPushEnabled;
  private OffsetRecord offsetRecord;
  /** whether the ingestion of current partition is deferred-write. */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  private boolean completionReported;
  private LeaderFollowerStateType leaderState;

  /**
   * Only used in L/F model. Check if the partition has released the latch.
   * In L/F ingestion task, Optionally, the state model holds a latch that
   * is used to determine when it should transit from offline to follower.
   * The latch is only placed if the Helix resource is serving the read traffic.
   *
   * See {@link com.linkedin.venice.helix.LeaderFollowerParticipantModel} for the
   * details why we need latch for certain resources.
   */
  private boolean isLatchReleased = false;

  private volatile Future<RecordMetadata> lastLeaderProduceCallback = null;

  /**
   * In-memory cache for the TopicSwitch in {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState};
   * make sure to keep the in-memory state and StoreVersionState in sync.
   */
  private TopicSwitch topicSwitch = null;

  /**
   * The following priorities are used to store the progress of processed records since it is not efficient to
   * update offset db for every record.
   */
  private int processedRecordNum;
  private int processedRecordSize;
  private long processedRecordSizeSinceLastSync;

  private long lastTimeOfSourceTopicOffsetLookup;
  private long sourceTopicMaxOffset;

  private KafkaConsumerService consumerService = null;

  public void setSourceTopicMaxOffset(long sourceTopicMaxOffset) {
    this.sourceTopicMaxOffset = sourceTopicMaxOffset;
  }

  public long getSourceTopicMaxOffset() {

    return sourceTopicMaxOffset;
  }

  public PartitionConsumptionState(int partition, OffsetRecord offsetRecord, boolean hybrid, boolean isIncrementalPushEnabled) {
    this.partition = partition;
    this.hybrid = hybrid;
    this.isIncrementalPushEnabled = isIncrementalPushEnabled;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.processedRecordNum = 0;
    this.processedRecordSize = 0;
    this.processedRecordSizeSinceLastSync = 0;
    this.lastTimeOfSourceTopicOffsetLookup = -1;
    this.sourceTopicMaxOffset = -1;
    this.leaderState = LeaderFollowerStateType.STANDBY;
  }

  public int getPartition() {
    return this.partition;
  }
  public void setOffsetRecord(OffsetRecord offsetRecord) {
    this.offsetRecord = offsetRecord;
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
    return this.offsetRecord.getOffset() > 0;
  }
  public boolean isEndOfPushReceived() {
    return this.offsetRecord.isEndOfPushReceived();
  }
  public boolean isWaitingForReplicationLag() {
    return isEndOfPushReceived() && !lagCaughtUp;
  }
  public void lagHasCaughtUp() {
    this.lagCaughtUp = true;
  }
  public boolean isCompletionReported() {
    return completionReported;
  }
  public void completionReported() {
    this.completionReported = true;
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
  public void incrementProcessedRecordNum() {
    ++this.processedRecordNum;
  }
  public boolean isComplete() {
    if (!isEndOfPushReceived()) {
      return false;
    }

    //for regular push store, receiving EOP is good to go
    return (!isIncrementalPushEnabled && !hybrid) || lagCaughtUp;
  }

  /**
   * This value is not reliable for Hybrid ingest.  Check its current usage before relying on it.
   * @return
   */
  public int getProcessedRecordNum() {
    return this.processedRecordNum;
  }
  public void resetProcessedRecordNum() {
    this.processedRecordNum = 0;
  }
  public void incrementProcessedRecordSize(int recordSize) {
    this.processedRecordSize += recordSize;
  }
  public int getProcessedRecordSize() {
    return this.processedRecordSize;
  }
  public void resetProcessedRecordSize() {
    this.processedRecordSize = 0;
  }
  public IncrementalPush getIncrementalPush() {
    return this.offsetRecord.getIncrementalPush();
  }
  public void setIncrementalPush(IncrementalPush ip) {
    this.offsetRecord.setIncrementalPush(ip);
  }
  public long getLastTimeOfSourceTopicOffsetLookup() {
    return lastTimeOfSourceTopicOffsetLookup;
  }
  public void setLastTimeOfSourceTopicOffsetLookup(long lastTimeOfSourceTopicOffsetLookup) {
    this.lastTimeOfSourceTopicOffsetLookup = lastTimeOfSourceTopicOffsetLookup;
  }

  public boolean isHybrid() {
    return hybrid;
  }

  public boolean isBatchOnly() {
    return !isHybrid() && !isIncrementalPushEnabled();
  }

  @Override
  public String toString() {
    return "PartitionConsumptionState{" +
        "partition=" + partition +
        ", hybrid=" + hybrid +
        ", offsetRecord=" + offsetRecord +
        ", errorReported=" + errorReported +
        ", started=" + isStarted() +
        ", lagCaughtUp=" + lagCaughtUp +
        ", processedRecordNum=" + processedRecordNum +
        ", processedRecordSize=" + processedRecordSize +
        ", processedRecordSizeSinceLastSync=" + processedRecordSizeSinceLastSync +
        '}';
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

  public boolean isIncrementalPushEnabled() {
    return isIncrementalPushEnabled;
  }

  public void setLeaderState(LeaderFollowerStateType state) {
    this.leaderState = state;
  }

  public LeaderFollowerStateType getLeaderState() {
    return this.leaderState;
  }

  public void setLastLeaderProduceFuture(Future<RecordMetadata> future) {
    this.lastLeaderProduceCallback = future;
  }

  public Future<RecordMetadata> getLastLeaderProduceFuture() {
    return this.lastLeaderProduceCallback;
  }

  /**
   * Update the in-memory state for TopicSwitch whenever encounter a new TopicSwitch message or after a restart.
   */
  public void setTopicSwitch(TopicSwitch topicSwitch) {
    this.topicSwitch = topicSwitch;
  }

  public TopicSwitch getTopicSwitch() {
    return this.topicSwitch;
  }

  public void setConsumerService(KafkaConsumerService consumerService) {
    this.consumerService = consumerService;
  }

  public KafkaConsumerService getConsumerService() {
    return this.consumerService;
  }
}
