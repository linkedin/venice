package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfBufferReplay;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DiskUsage;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;


public class OnlineOfflineStoreIngestionTask extends StoreIngestionTask {
  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(OnlineOfflineStoreIngestionTask.class);

  public OnlineOfflineStoreIngestionTask(
      Store store,
      Version version,
      KafkaClientFactory consumerFactory,
      Properties kafkaConsumerProperties,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      Queue<VeniceNotifier> notifiers,
      EventThrottler bandwidthThrottler,
      EventThrottler recordsThrottler,
      EventThrottler unorderedBandwidthThrottler,
      EventThrottler unorderedRecordsThrottler,
      ReadOnlySchemaRepository schemaRepo,
      ReadOnlyStoreRepository storeRepo,
      TopicManagerRepository topicManagerRepository,
      TopicManagerRepository topicManagerRepositoryJavaBased,
      AggStoreIngestionStats storeIngestionStats,
      AggVersionedDIVStats versionedDIVStats,
      AggVersionedStorageIngestionStats versionedStorageIngestionStats,
      AbstractStoreBufferService storeBufferService,
      BooleanSupplier isCurrentVersion,
      VeniceStoreConfig storeConfig,
      DiskUsage diskUsage,
      RocksDBMemoryStats rocksDBMemoryStats,
      AggKafkaConsumerService aggKafkaConsumerService,
      VeniceServerConfig serverConfig,
      int partitionId,
      ExecutorService cacheWarmingThreadPool,
      long startReportingReadyToServeTimestamp,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend) {
    super(
        store,
        version,
        consumerFactory,
        kafkaConsumerProperties,
        storageEngineRepository,
        storageMetadataService,
        notifiers,
        bandwidthThrottler,
        recordsThrottler,
        unorderedBandwidthThrottler,
        unorderedRecordsThrottler,
        schemaRepo,
        storeRepo,
        topicManagerRepository,
        topicManagerRepositoryJavaBased,
        storeIngestionStats,
        versionedDIVStats,
        versionedStorageIngestionStats,
        storeBufferService,
        isCurrentVersion,
        storeConfig,
        diskUsage,
        rocksDBMemoryStats,
        aggKafkaConsumerService,
        serverConfig,
        partitionId,
        cacheWarmingThreadPool,
        startReportingReadyToServeTimestamp,
        partitionStateSerializer,
        isIsolatedIngestion,
        cacheBackend);
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
  protected void updateOffsetRecord(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {
    offsetRecord.setLocalVersionTopicOffset(consumerRecordWrapper.consumerRecord().offset());
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
    long currentOffset = partitionConsumptionState.getOffsetRecord().getLocalVersionTopicOffset();
    /**
     * After END_OF_PUSH received, `isReadyToServe()` is invoked for each message until the lag is caught up (otherwise,
     * if we only check ready to serve periodically, the lag may never catch up); in order not to slow down the hybrid
     * ingestion, {@link CachedKafkaMetadataGetter} was introduced to get the latest offset periodically;
     * with this strategy, it is possible that partition could become 'ONLINE' at most
     * {@link CachedKafkaMetadataGetter#ttlMs} earlier.
     */
    StartOfBufferReplay sobr = storeVersionStateOptional.get().startOfBufferReplay;
    long sourceTopicMaxOffset = cachedKafkaMetadataGetter.getOffset(localKafkaServer, sobr.sourceTopicName.toString(), partition);
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
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
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

    long lastOffset = partitionConsumptionState.getOffsetRecord()
        .getLocalVersionTopicOffset();
    if(lastOffset >= record.offset()) {
      logger.info(consumerTaskId + "The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
      return false;
    }

    return super.shouldProcessRecord(record);
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
}
