package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfBufferReplay;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.stats.StatsErrorCode;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class OnlineOfflineStoreIngestionTask extends StoreIngestionTask {
  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = LogManager.getLogger(OnlineOfflineStoreIngestionTask.class);

  public OnlineOfflineStoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend) {
    super(builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend,
        builder.getOnlineOfflineNotifiers());

    if (amplificationFactor != 1) {
      throw new VeniceException("amplificationFactor is not supported in Online/Offline state model");
    }
  }

  @Override
  protected void processConsumerAction(ConsumerAction message) throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case STANDBY_TO_LEADER:
      case LEADER_TO_STANDBY:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + this.getClass().getName());
      default:
        processCommonConsumerAction(operation, topic, partition, message.getLeaderState());
    }
  }

  @Override
  public void promoteToLeader(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throw new VeniceException("Leader/Follower related API should not be invoked in Online/Offline state model");
  }

  @Override
  public void demoteToStandby(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throw new VeniceException("Leader/Follower related API should not be invoked in Online/Offline state model");
  }

  @Override
  protected void checkLongRunningTaskState() {
    // No-op for online/offline state model
  }

  @Override
  protected Set<String> getConsumptionSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    return Collections.singleton(localKafkaServer);
  }

  @Override
  protected void updateOffsetMetadataInOffsetRecord(
      PartitionConsumptionState partitionConsumptionState,
      OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl) {
    offsetRecord.setCheckpointLocalVersionTopicOffset(consumerRecord.offset());
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl) {
    partitionConsumptionState.updateLatestProcessedLocalVersionTopicOffset(consumerRecord.offset());
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    return partitionConsumptionState.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent();
  }

  /**
   * For Online/Offline state model, we use the StartOfBufferReplay message to calculate the lag.
   */
  @Override
  protected long measureHybridOffsetLag(PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    Optional<StoreVersionState> storeVersionStateOptional = storageMetadataService.getStoreVersionState(
        kafkaVersionTopic);
    Optional<Long> sobrDestinationOffsetOptional = partitionConsumptionState.getOffsetRecord().getStartOfBufferReplayDestinationOffset();
    if (!(storeVersionStateOptional.isPresent() && sobrDestinationOffsetOptional.isPresent())) {
      // In this case, we have a hybrid store which has received its EOP, but has not yet received its SOBR.
      // Therefore, we cannot precisely measure its offset, but we know for sure that it is lagging since
      // the RT buffer replay has not even started yet.
      logger.warn(consumerTaskId + " Cannot measure replication lag because the SOBR info is not present"
          + ", storeVersionStateOptional: " + storeVersionStateOptional + ", sobrDestinationOffsetOptional: " + sobrDestinationOffsetOptional);
      return Long.MAX_VALUE;
    }

    /**
     * startOfBufferReplay could be null if the storeVersionState was deleted without deleting the corresponding
     * partitionConsumptionState. This will cause storeVersionState to be recreated without startOfBufferReplay.
     */
    if (storeVersionStateOptional.get().startOfBufferReplay == null) {
      throw new VeniceInconsistentStoreMetadataException("Inconsistent store metadata detected for topic " + kafkaVersionTopic
          + ", partition " + partitionConsumptionState.getPartition()
          +". Will clear the metadata and restart ingestion.");
    }

    int partition = partitionConsumptionState.getPartition();
    long currentOffset = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
    /**
     * After END_OF_PUSH received, `isReadyToServe()` is invoked for each message until the lag is caught up (otherwise,
     * if we only check ready to serve periodically, the lag may never catch up); in order not to slow down the hybrid
     * ingestion, {@link CachedKafkaMetadataGetter} was introduced to get the latest offset periodically;
     * with this strategy, it is possible that partition could become 'ONLINE' at most
     * {@link CachedKafkaMetadataGetter#ttlMs} earlier.
     */
    StartOfBufferReplay sobr = storeVersionStateOptional.get().startOfBufferReplay;
    long sourceTopicMaxOffset = cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), sobr.sourceTopicName.toString(), partition);
    long sobrSourceOffset = sobr.sourceOffsets.get(partition);

    if (!partitionConsumptionState.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent()) {
      throw new IllegalArgumentException("SOBR DestinationOffset is not presented.");
    }

    long sobrDestinationOffset = partitionConsumptionState.getOffsetRecord().getStartOfBufferReplayDestinationOffset().get();
    long lag = (sourceTopicMaxOffset - sobrSourceOffset) - (currentOffset - sobrDestinationOffset);

    if (shouldLogLag) {
      logger.info(String.format("%s partition %d real-time buffer lag offset is: " + "(Source Max [%d] - SOBR Source [%d]) - (Dest Current [%d] - SOBR Dest [%d]) = Lag [%d]",
          consumerTaskId, partition, sourceTopicMaxOffset, sobrSourceOffset, currentOffset, sobrDestinationOffset, lag));
    }
    return lag;
  }

  @Override
  protected void reportIfCatchUpBaseTopicOffset(PartitionConsumptionState partitionConsumptionState) {
    //No-op for Online/Offline ingestion task
  }

  /**
   * For Online/Offline model, we should check whether the record is always from version topic;
   * besides, since the messages always comes from version topic, we don't need to process a message
   * that has lower offset than the recorded latest consumed offset.
   */
  @Override
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, int subPartition) {
    String recordTopic = record.topic();
    if(!kafkaVersionTopic.equals(recordTopic)) {
      throw new VeniceMessageException(consumerTaskId + "Message retrieved from different topic. Expected " + this.kafkaVersionTopic
          + " Actual " + recordTopic);
    }

    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if(null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + kafkaVersionTopic + " Partition Id: " + partitionId);
      return false;
    }

    long lastOffset = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
    if(lastOffset >= record.offset()) {
      logger.info(consumerTaskId + "The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
      return false;
    }

    return super.shouldProcessRecord(record, subPartition);
  }

  @Override
  public void consumerUnSubscribeAllTopics(PartitionConsumptionState partitionConsumptionState) {
    /**
     * O/O replica will always be consuming from one topic only, which is the local version topic.
     */
    consumerUnSubscribe(kafkaVersionTopic, partitionConsumptionState);
  }

  @Override
  public long getBatchReplicationLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getLeaderOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getBatchLeaderOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getHybridLeaderOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getFollowerOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getBatchFollowerOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getHybridFollowerOffsetLag() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public long getRegionHybridOffsetLag(int regionId) {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public int getWriteComputeErrorCode() {
    return StatsErrorCode.METRIC_ONLY_AVAILABLE_FOR_LEADER_FOLLOWER_STORES.code;
  }

  @Override
  public void updateLeaderTopicOnFollower(PartitionConsumptionState partitionConsumptionState) {
    //No-op for Online/Offline ingestion task
  }
}
