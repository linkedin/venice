package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfBufferReplay;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;


public class OnlineOfflineStoreIngestionTask extends StoreIngestionTask {
  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(OnlineOfflineStoreIngestionTask.class);

  public OnlineOfflineStoreIngestionTask(
      VeniceWriterFactory writerFactory,
      VeniceConsumerFactory consumerFactory,
      Properties kafkaConsumerProperties,
      StoreRepository storeRepository,
      StorageMetadataService storageMetadataService,
      Queue<VeniceNotifier> notifiers,
      EventThrottler bandwidthThrottler,
      EventThrottler recordsThrottler,
      ReadOnlySchemaRepository schemaRepo,
      TopicManager topicManager,
      AggStoreIngestionStats storeIngestionStats,
      AggVersionedDIVStats versionedDIVStats,
      StoreBufferService storeBufferService,
      BooleanSupplier isCurrentVersion,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean isIncrementalPushEnabled,
      VeniceStoreConfig storeConfig,
      DiskUsage diskUsage,
      boolean bufferReplayEnabledForHybrid,
      VeniceServerConfig serverConfig) {
    super(writerFactory, consumerFactory, kafkaConsumerProperties, storeRepository, storageMetadataService, notifiers,
        bandwidthThrottler, recordsThrottler, schemaRepo, topicManager, storeIngestionStats, versionedDIVStats, storeBufferService,
        isCurrentVersion, hybridStoreConfig, isIncrementalPushEnabled, storeConfig, diskUsage, bufferReplayEnabledForHybrid, serverConfig);
  }

  @Override
  protected void processConsumerAction(ConsumerAction message)
      throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case STANDBY_TO_LEADER:
      case LEADER_TO_STANDBY:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + this.getClass().getName());
      default:
        processCommonConsumerAction(operation, topic, partition);
    }
  }

  @Override
  public void promoteToLeader(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    throw new VeniceException("Leader/Follower related API should not be invoked in Online/Offline state model");
  }

  @Override
  public void demoteToStandby(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    throw new VeniceException("Leader/Follower related API should not be invoked in Online/Offline state model");
  }

  @Override
  protected void checkLongRunningConsumerActionState() {
    // No-op for online/offline state model
  }

  @Override
  protected void updateOffset(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    offsetRecord.setOffset(consumerRecord.offset());
  }

  @Override
  protected void writeToDatabaseAndProduce(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      WriteToStorageEngine writeFunction, ProduceToTopic produceFunction) {
    // Execute the write function immediately
    writeFunction.apply();
  }

  /**
   * For Online/Offline state model, we use the StartOfBufferReplay message to calculate the lag.
   */
  @Override
  protected long measureHybridOffsetLag(PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    Optional<StoreVersionState> storeVersionStateOptional = storageMetadataService.getStoreVersionState(topic);
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
      throw new VeniceInconsistentStoreMetadataException("Inconsistent store metadata detected for topic " + topic
          + ", partition " + partitionConsumptionState.getPartition()
          +". Will clear the metadata and restart ingestion.");
    }

    int partition = partitionConsumptionState.getPartition();
    long currentOffset = partitionConsumptionState.getOffsetRecord().getOffset();
    /**
     * After END_OF_PUSH received, `isReadyToServe()` is invoked for each message until the lag is caught up (otherwise,
     * if we only check ready to serve periodically, the lag may never catch up); in order not to slow down the hybrid
     * ingestion, {@link CachedLatestOffsetGetter} was introduced to get the latest offset periodically;
     * with this strategy, it is possible that partition could become 'ONLINE' at most
     * {@link CachedLatestOffsetGetter#ttlMs} earlier.
     */
    if (bufferReplayEnabledForHybrid) {
      StartOfBufferReplay sobr = storeVersionStateOptional.get().startOfBufferReplay;
      long sourceTopicMaxOffset = cachedLatestOffsetGetter.getOffset(sobr.sourceTopicName.toString(), partition);

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
    } else {
      long storeVersionTopicLatestOffset = cachedLatestOffsetGetter.getOffset(topic, partition);
      long lag = storeVersionTopicLatestOffset - currentOffset;
      if (shouldLogLag) {
        logger.info(String.format("Store buffer replay was disabled, and %s partition %d lag offset is: (Dest Latest [%d] - Dest Current [%d]) = Lag [%d]",
            consumerTaskId, partition, storeVersionTopicLatestOffset, currentOffset, lag));
      }
      return lag;
    }
  }

  /**
   * For Online/Offline model, we should check whether the record is always from version topic;
   * besides, since the messages always comes from version topic, we don't need to process a message
   * that has lower offset than the recorded latest consumed offset.
   */
  @Override
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    String recordTopic = record.topic();
    if(!topic.equals(recordTopic)) {
      throw new VeniceMessageException(consumerTaskId + "Message retrieved from different topic. Expected " + this.topic + " Actual " + recordTopic);
    }

    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if(null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + topic + " Partition Id: " + partitionId);
      return false;
    }
    long lastOffset = partitionConsumptionState.getOffsetRecord()
        .getOffset();
    if(lastOffset >= record.offset()) {
      logger.info(consumerTaskId + "The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
      return false;
    }

    return super.shouldProcessRecord(record);
  }
}
