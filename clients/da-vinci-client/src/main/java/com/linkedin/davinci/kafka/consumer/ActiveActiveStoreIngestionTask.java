package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.venice.VeniceConstants.REWIND_TIME_DECIDED_BY_SERVER;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.replication.merge.MergeConflictResolverFactory;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains logic that SNs must perform if a store-version is running in Active/Active mode.
 */
public class ActiveActiveStoreIngestionTask extends LeaderFollowerStoreIngestionTask {
  private static final Logger LOGGER = LogManager.getLogger(ActiveActiveStoreIngestionTask.class);

  private final int rmdProtocolVersionId;
  private final MergeConflictResolver mergeConflictResolver;
  private final RmdSerDe rmdSerDe;
  private final RemoteIngestionRepairService remoteIngestionRepairService;

  public ActiveActiveStoreIngestionTask(
      StorageService storageService,
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      Lazy<ZKHelixAdmin> zkHelixAdmin) {
    super(
        storageService,
        builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend,
        recordTransformerConfig,
        zkHelixAdmin);

    this.rmdProtocolVersionId = version.getRmdVersionId();

    StringAnnotatedStoreSchemaCache annotatedReadOnlySchemaRepository =
        new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);

    this.rmdSerDe = new RmdSerDe(
        annotatedReadOnlySchemaRepository,
        rmdProtocolVersionId,
        getServerConfig().isComputeFastAvroEnabled());
    this.mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(
            annotatedReadOnlySchemaRepository,
            rmdSerDe,
            getStoreName(),
            isWriteComputationEnabled,
            getServerConfig().isComputeFastAvroEnabled());
    this.remoteIngestionRepairService = builder.getRemoteIngestionRepairService();
  }

  public static int getKeyLevelLockMaxPoolSizeBasedOnServerConfig(VeniceServerConfig serverConfig, int partitionCount) {
    int consumerPoolSizeForLeaderConsumption = 0;
    if (serverConfig.isDedicatedConsumerPoolForAAWCLeaderEnabled()) {
      consumerPoolSizeForLeaderConsumption = serverConfig.getDedicatedConsumerPoolSizeForAAWCLeader()
          + serverConfig.getDedicatedConsumerPoolSizeForSepRTLeader();
    } else if (serverConfig.getConsumerPoolStrategyType()
        .equals(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION)) {
      consumerPoolSizeForLeaderConsumption = serverConfig.getConsumerPoolSizeForCurrentVersionAAWCLeader()
          + serverConfig.getConsumerPoolSizeForCurrentVersionSepRTLeader()
          + serverConfig.getConsumerPoolSizeForNonCurrentVersionAAWCLeader();
    } else {
      consumerPoolSizeForLeaderConsumption = serverConfig.getConsumerPoolSizePerKafkaCluster();
    }
    int multiplier = 1;
    if (serverConfig.isAAWCWorkloadParallelProcessingEnabled()) {
      multiplier = serverConfig.getAAWCWorkloadParallelProcessingThreadPoolSize();
    }
    return Math.min(partitionCount, consumerPoolSizeForLeaderConsumption)
        * serverConfig.getKafkaClusterIdToUrlMap().size() * multiplier + 1;
  }

  @Override
  protected void putInStorageEngine(int partition, byte[] keyBytes, Put put) {
    try {

      // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
      StorageOperationType storageOperationType =
          getStorageOperationType(partition, put.putValue, put.replicationMetadataPayload);
      switch (storageOperationType) {
        case VALUE_AND_RMD:
          storageEngine.putWithReplicationMetadata(
              partition,
              keyBytes,
              put.putValue,
              prependReplicationMetadataBytesWithValueSchemaId(put.replicationMetadataPayload, put.schemaId));
          break;
        case RMD_CHUNK:
          storageEngine.putReplicationMetadata(
              partition,
              keyBytes,
              prependReplicationMetadataBytesWithValueSchemaId(put.replicationMetadataPayload, put.schemaId));
          break;
        case VALUE:
          storageEngine.put(partition, keyBytes, put.putValue);
          break;
        default:
          // do nothing
          break;
      }
    } catch (PersistenceFailureException e) {
      throwOrLogStorageFailureDependingIfStillSubscribed(partition, e);
    }
  }

  @Override
  protected void removeFromStorageEngine(int partition, byte[] keyBytes, Delete delete) {
    try {
      // TODO: Honor BatchConflictResolutionPolicy and maybe persist RMD for batch push records.
      switch (getStorageOperationType(partition, null, delete.replicationMetadataPayload)) {
        case VALUE_AND_RMD:
          byte[] metadataBytesWithValueSchemaId =
              prependReplicationMetadataBytesWithValueSchemaId(delete.replicationMetadataPayload, delete.schemaId);
          storageEngine.deleteWithReplicationMetadata(partition, keyBytes, metadataBytesWithValueSchemaId);
          break;
        case VALUE:
          storageEngine.delete(partition, keyBytes);
          break;
        default:
          // do nothing
          break;
      }
    } catch (PersistenceFailureException e) {
      throwOrLogStorageFailureDependingIfStillSubscribed(partition, e);
    }
  }

  /** @return what kind of storage operation to execute, if any. */
  private StorageOperationType getStorageOperationType(int partition, ByteBuffer valuePayload, ByteBuffer rmdPayload) {
    PartitionConsumptionState pcs = partitionConsumptionStateMap.get(partition);
    if (pcs == null) {
      logStorageOperationWhileUnsubscribed(partition);
      return StorageOperationType.NONE;
    }
    if (isDaVinciClient) {
      return StorageOperationType.VALUE;
    }
    if (rmdPayload == null) {
      throw new IllegalArgumentException("Replication metadata payload not found.");
    }
    if (pcs.isEndOfPushReceived() || rmdPayload.remaining() > 0) {
      // value payload == null means it is a DELETE request, while value payload size > 0 means it is a PUT request.
      if (valuePayload == null || valuePayload.remaining() > 0) {
        return StorageOperationType.VALUE_AND_RMD;
      }
      return StorageOperationType.RMD_CHUNK;
    } else {
      return StorageOperationType.VALUE;
    }
  }

  private enum StorageOperationType {
    VALUE_AND_RMD, // Operate on value associated with RMD
    VALUE, // Operate on full or chunked value
    RMD_CHUNK, // Operate on chunked RMD
    NONE
  }

  private byte[] prependReplicationMetadataBytesWithValueSchemaId(ByteBuffer metadata, int valueSchemaId) {
    // TODO: Currently this function makes a copy of the data in the original buffer. It may be possible to pre-allocate
    // space for the 4-byte header and reuse the original replication metadata ByteBuffer.
    ByteBuffer bufferWithHeader = ByteUtils.prependIntHeaderToByteBuffer(metadata, valueSchemaId, false);
    bufferWithHeader.position(bufferWithHeader.position() - ByteUtils.SIZE_OF_INT);
    byte[] replicationMetadataBytesWithValueSchemaId = ByteUtils.extractByteArray(bufferWithHeader);
    bufferWithHeader.position(bufferWithHeader.position() + ByteUtils.SIZE_OF_INT);
    return replicationMetadataBytesWithValueSchemaId;
  }

  @Override
  protected Map<String, Long> calculateLeaderUpstreamOffsetWithTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      PubSubTopic newSourceTopic,
      List<CharSequence> unreachableBrokerList) {
    TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch().getTopicSwitch();
    if (topicSwitch == null) {
      throw new VeniceException(
          "New leader does not have topic switch, unable to switch to realtime leader topic: " + newSourceTopic);
    }
    final PubSubTopicPartition sourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopic);
    long rewindStartTimestamp;
    // calculate the rewind start time here if controller asked to do so by using this sentinel value.
    if (topicSwitch.rewindStartTimestamp == REWIND_TIME_DECIDED_BY_SERVER) {
      rewindStartTimestamp = calculateRewindStartTime(partitionConsumptionState);
      LOGGER.info(
          "{} leader calculated rewindStartTimestamp: {} for topic-partition: {}",
          ingestionTaskName,
          rewindStartTimestamp,
          sourceTopicPartition);
    } else {
      rewindStartTimestamp = topicSwitch.rewindStartTimestamp;
    }
    Set<String> sourceKafkaServers = getKafkaUrlSetFromTopicSwitch(partitionConsumptionState.getTopicSwitch());
    Map<String, Long> upstreamOffsetsByKafkaURLs = new HashMap<>(sourceKafkaServers.size());
    sourceKafkaServers.forEach(sourceKafkaURL -> {
      Long upstreamStartOffset =
          partitionConsumptionState.getLatestProcessedUpstreamRTOffsetWithNoDefault(sourceKafkaURL);
      if (upstreamStartOffset == null || upstreamStartOffset < 0) {
        if (rewindStartTimestamp > 0) {
          PubSubTopicPartition newSourceTopicPartition =
              resolveTopicPartitionWithKafkaURL(newSourceTopic, partitionConsumptionState, sourceKafkaURL);
          try {
            upstreamStartOffset =
                getTopicPartitionOffsetByKafkaURL(sourceKafkaURL, newSourceTopicPartition, rewindStartTimestamp);
            LOGGER.info(
                "{} get upstreamStartOffset: {} for source URL: {}, topic-partition: {}, rewind timestamp: {}",
                ingestionTaskName,
                upstreamStartOffset,
                sourceKafkaURL,
                newSourceTopicPartition,
                rewindStartTimestamp);
            upstreamOffsetsByKafkaURLs.put(sourceKafkaURL, upstreamStartOffset);
          } catch (Exception e) {
            /**
             * This is actually tricky. Potentially we could return a -1 value here, but this has the gotcha that if we
             * have a non-symmetrical failure (like, region1 can't talk to the region2 broker) this will result in a remote
             * colo rewinding to a potentially non-deterministic offset when the remote becomes available again. So
             * instead, we record the context of this call and commit it to a repair queue to rewind to a
             * consistent place on the RT
             *
             * NOTE: It is possible that the outage is so long and the rewind so large that we fall off retention. If
             * this happens we can detect and repair the inconsistency from the offline DCR validator.
             */
            unreachableBrokerList.add(sourceKafkaURL);
            upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
            LOGGER.error(
                "Failed contacting broker {} when processing topic switch! for {}. Setting upstream start offset to {}",
                sourceKafkaURL,
                sourceTopicPartition,
                upstreamStartOffset);
            hostLevelIngestionStats.recordIngestionFailure();
            /**
             *  Add to repair queue. We won't attempt to resubscribe for brokers we couldn't compute an upstream offset
             *  accurately for. We will not persist the wrong offset into OffsetRecord, we'll reattempt subscription later.
             */
            if (remoteIngestionRepairService != null) {
              this.remoteIngestionRepairService.registerRepairTask(
                  this,
                  buildRepairTask(
                      sourceKafkaURL,
                      sourceTopicPartition,
                      rewindStartTimestamp,
                      partitionConsumptionState));
            } else {
              // If there isn't an available repair service, then we need to abort in order to make sure the error is
              // propagated up
              throw new VeniceException(
                  String.format(
                      "Failed contacting broker (%s) and no repair service available!  Aborting topic switch processing for %s. Setting upstream start offset to %d",
                      sourceKafkaURL,
                      sourceTopicPartition,
                      upstreamStartOffset));
            }
          }
        } else {
          LOGGER.warn(
              "{} got unexpected rewind time: {}, will start ingesting upstream from the beginning",
              ingestionTaskName,
              rewindStartTimestamp);
          upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
          upstreamOffsetsByKafkaURLs.put(sourceKafkaURL, upstreamStartOffset);
        }
      } else {
        upstreamOffsetsByKafkaURLs.put(sourceKafkaURL, upstreamStartOffset);
      }
    });
    if (unreachableBrokerList.size() >= ((sourceKafkaServers.size() + 1) / 2)) {
      // We couldn't reach a quorum of brokers and that's a red flag, so throw exception and abort!
      throw new VeniceException("Couldn't reach any broker!!  Aborting topic switch triggered consumer subscription!");
    }
    return upstreamOffsetsByKafkaURLs;
  }

  @Override
  protected void startConsumingAsLeader(PartitionConsumptionState partitionConsumptionState) {
    final int partition = partitionConsumptionState.getPartition();
    final OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    final PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);

    /**
     * Note that this function is called after the new leader has waited for 5 minutes of inactivity on the local VT topic.
     * The new leader might NOT need to switch to remote consumption in a case where map-reduce jobs of a batch job stuck
     * on producing to the local VT so that there is no activity in the local VT.
     */
    if (shouldNewLeaderSwitchToRemoteConsumption(partitionConsumptionState)) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER
          .info("{} enabled remote consumption from topic {} partition {}", ingestionTaskName, leaderTopic, partition);
    }
    partitionConsumptionState.setLeaderFollowerState(LEADER);
    prepareLeaderOffsetCheckpointAndStartConsumptionAsLeader(leaderTopic, partitionConsumptionState, false);
  }

  /**
   * Ensures the PubSub URL is present in the PubSub cluster URL-to-ID map before subscribing to a topic.
   * Prevents subscription to unknown PubSub URLs, which can cause issues during message consumption.
   */
  public void consumerSubscribe(PubSubTopicPartition pubSubTopicPartition, long startOffset, String pubSubAddress) {
    VeniceServerConfig serverConfig = getServerConfig();
    if (isDaVinciClient() || serverConfig.getKafkaClusterUrlToIdMap().containsKey(pubSubAddress)) {
      super.consumerSubscribe(pubSubTopicPartition, startOffset, pubSubAddress);
      return;
    }
    LOGGER.error(
        "PubSub address: {} is not in the pubsub cluster map: {}. Cannot subscribe to topic-partition: {}",
        pubSubAddress,
        serverConfig.getKafkaClusterUrlToIdMap(),
        pubSubTopicPartition);
    throw new VeniceException(
        String.format(
            "PubSub address: %s is not in the pubsub cluster map. Cannot subscribe to topic-partition: %s",
            pubSubAddress,
            pubSubTopicPartition));
  }

  @Override
  protected void leaderExecuteTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      TopicSwitch topicSwitch,
      PubSubTopic newSourceTopic) {
    if (partitionConsumptionState.getLeaderFollowerState() != LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s", LEADER, partitionConsumptionState));
    }
    if (topicSwitch.sourceKafkaServers.isEmpty()) {
      throw new VeniceException(
          "In the A/A mode, source Kafka URL list cannot be empty in Topic Switch control message.");
    }

    final int partition = partitionConsumptionState.getPartition();
    final PubSubTopic currentLeaderTopic =
        partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    final PubSubTopicPartition sourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopic);

    // unsubscribe the old source and subscribe to the new source
    unsubscribeFromTopic(currentLeaderTopic, partitionConsumptionState);
    waitForLastLeaderPersistFuture(
        partitionConsumptionState,
        String.format(
            "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
            currentLeaderTopic,
            newSourceTopic,
            partition));

    if (topicSwitch.sourceKafkaServers.size() != 1
        || (!Objects.equals(topicSwitch.sourceKafkaServers.get(0).toString(), localKafkaServer))) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER.info(
          "{} enabled remote consumption and switch to topic-partition {}",
          ingestionTaskName,
          sourceTopicPartition);
    }
    // Update leader topic.
    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    // Calculate leader offset and start consumption
    prepareLeaderOffsetCheckpointAndStartConsumptionAsLeader(newSourceTopic, partitionConsumptionState, true);
  }

  /**
   * Process {@link TopicSwitch} control message at given partition offset for a specific {@link PartitionConsumptionState}.
   * Return whether we need to execute additional ready-to-serve check after this message is processed.
   */
  @Override
  protected boolean processTopicSwitch(
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {
    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
    ingestionNotificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState);
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(newSourceTopicName);

    /**
     * TopicSwitch needs to be persisted locally for both servers and DaVinci clients so that ready-to-serve check
     * can make the correct decision.
     */
    syncTopicSwitchToIngestionMetadataService(topicSwitch, partitionConsumptionState);
    if (!isLeader(partitionConsumptionState)) {
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    }
    return false;
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl,
      boolean dryRun) {
    updateOffsetsFromConsumerRecord(
        partitionConsumptionState,
        consumerRecord,
        leaderProducedRecordContext,
        partitionConsumptionState::updateLatestProcessedLocalVersionTopicOffset,
        (sourceKafkaUrl, upstreamTopic, upstreamTopicOffset) -> {
          if (upstreamTopic.isRealTime()) {
            partitionConsumptionState.updateLatestProcessedUpstreamRTOffset(sourceKafkaUrl, upstreamTopicOffset);
          } else {
            partitionConsumptionState.updateLatestProcessedUpstreamVersionTopicOffset(upstreamTopicOffset);
          }
        },
        (sourceKafkaUrl, upstreamTopic) -> upstreamTopic.isRealTime()
            ? partitionConsumptionState.getLatestProcessedUpstreamRTOffset(sourceKafkaUrl)
            : partitionConsumptionState.getLatestProcessedUpstreamVersionTopicOffset(),
        () -> getUpstreamKafkaUrl(partitionConsumptionState, consumerRecord, kafkaUrl),
        dryRun);
  }

  /**
   * Get upstream kafka url for record.
   *   1. For leaders, it is the source where the record was consumed from
   *   2. For Followers, it is either
   *       a. The upstream kafka url populated by the leader in the record, or
   *       b. The local kafka address
   *
   * @param partitionConsumptionState The current {@link PartitionConsumptionState} for the partition
   * @param consumerRecord The record for which the upstream Kafka url needs to be computed
   * @param recordSourceKafkaUrl The Kafka URL from where the record was consumed
   * @return The computed upstream Kafka URL for the record
   */
  private String getUpstreamKafkaUrl(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String recordSourceKafkaUrl) {
    final String upstreamKafkaURL;
    if (isLeader(partitionConsumptionState)) {
      // Wherever leader consumes from is considered as "upstream"
      upstreamKafkaURL = recordSourceKafkaUrl;
    } else {
      KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
      if (kafkaValue.leaderMetadataFooter == null) {
        /**
         * This "leaderMetadataFooter" field do not get populated in case the source fabric is the same as the
         * local fabric since the VT source will be one of the prod fabric after batch NR is fully ramped. The
         * leader replica consumes from local Kafka URL. Hence, from a follower's perspective, the upstream Kafka
         * cluster which the leader consumes from should be the local Kafka URL.
         */
        upstreamKafkaURL = localKafkaServer;
      } else {
        upstreamKafkaURL =
            getUpstreamKafkaUrlFromKafkaValue(consumerRecord, recordSourceKafkaUrl, this.kafkaClusterIdToUrlMap);
      }
    }
    return upstreamKafkaURL;
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    TopicSwitchWrapper topicSwitchWrapper = partitionConsumptionState.getTopicSwitch();
    if (topicSwitchWrapper == null) {
      return false;
    }
    if (topicSwitchWrapper.getTopicSwitch().sourceKafkaServers.isEmpty()) {
      throw new VeniceException("Got empty source Kafka URLs in Topic Switch.");
    }
    return topicSwitchWrapper.getNewSourceTopic().isRealTime();
  }

  /**
   * For A/A, there are multiple entries in upstreamOffsetMap during RT ingestion.
   * If the current DataReplicationPolicy is on Aggregate mode, A/A will check the upstream offset lags from all regions;
   * otherwise, only check the upstream offset lag from the local region.
   */
  @Override
  protected long getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String upstreamKafkaUrl) {
    return pcs.getLatestProcessedUpstreamRTOffsetWithIgnoredMessages(upstreamKafkaUrl);
  }

  /**
   * Different from the persisted upstream offset map in OffsetRecord, latest consumed upstream offset map is maintained
   * for each individual Kafka url.
   */
  @Override
  protected long getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String upstreamKafkaUrl) {
    return pcs.getLeaderConsumedUpstreamRTOffset(upstreamKafkaUrl);
  }

  @Override
  void updateLatestInMemoryLeaderConsumedRTOffset(PartitionConsumptionState pcs, String kafkaUrl, long offset) {
    pcs.updateLeaderConsumedUpstreamRTOffset(kafkaUrl, offset);
  }

  /**
   * N.B. package-private for testing purposes.
   */
  static String getUpstreamKafkaUrlFromKafkaValue(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String recordSourceKafkaUrl,
      Int2ObjectMap<String> kafkaClusterIdToUrlMap) {
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    if (kafkaValue.leaderMetadataFooter == null) {
      throw new VeniceException("leaderMetadataFooter field in KME should have been set.");
    }
    String upstreamKafkaURL = kafkaClusterIdToUrlMap.get(kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId);
    if (upstreamKafkaURL == null) {
      MessageType type = MessageType.valueOf(kafkaValue.messageType);
      throw new VeniceException(
          String.format(
              "No Kafka cluster ID found in the cluster ID to Kafka URL map. "
                  + "Got cluster ID %d and ID to cluster URL map %s. Source Kafka: %s; "
                  + "%s; Offset: %d; Message type: %s; ProducerMetadata: %s; LeaderMetadataFooter: %s",
              kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId,
              kafkaClusterIdToUrlMap,
              recordSourceKafkaUrl,
              consumerRecord.getTopicPartition(),
              consumerRecord.getOffset(),
              type.toString() + (type == MessageType.CONTROL_MESSAGE
                  ? "/" + ControlMessageType.valueOf((ControlMessage) kafkaValue.getPayloadUnion())
                  : ""),
              kafkaValue.producerMetadata,
              kafkaValue.leaderMetadataFooter));
    }
    return upstreamKafkaURL;
  }

  /**
   * For Active-Active this buffer is always used.
   * @return
   */
  @Override
  public boolean isTransientRecordBufferUsed() {
    return true;
  }

  @Override
  protected boolean shouldCheckLeaderCompleteStateInFollower() {
    return getServerConfig().isLeaderCompleteStateCheckInFollowerEnabled();
  }

  @Override
  public long getRegionHybridOffsetLag(int regionId) {
    StoreVersionState svs = storageEngine.getStoreVersionState();
    if (svs == null) {
      /**
       * Store version metadata is created for the first time when the first START_OF_PUSH message is processed;
       * however, the ingestion stat is created the moment an ingestion task is created, so there is a short time
       * window where there is no version metadata, which is not an error.
       */
      return 0;
    }

    if (partitionConsumptionStateMap.isEmpty()) {
      /**
       * Partition subscription happens after the ingestion task and stat are created, it's not an error.
       */
      return 0;
    }

    String kafkaSourceAddress = kafkaClusterIdToUrlMap.get(regionId);
    // This storage node does not register with the given region ID.
    if (kafkaSourceAddress == null) {
      return 0;
    }

    long offsetLag = partitionConsumptionStateMap.values()
        .stream()
        .filter(LeaderFollowerStoreIngestionTask.LEADER_OFFSET_LAG_FILTER)
        // the lag is (latest fabric RT offset - consumed fabric RT offset)
        .mapToLong((pcs) -> {
          PubSubTopic resolvedLeaderTopic =
              resolveTopicWithKafkaURL(pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository), kafkaSourceAddress);
          if (resolvedLeaderTopic == null || !resolvedLeaderTopic.isRealTime()) {
            // Leader topic not found, indicating that it is VT topic.
            return 0;
          }
          String resolvedKafkaUrl = Utils.resolveKafkaUrlForSepTopic(kafkaSourceAddress);
          // Consumer might not exist after the consumption state is created, but before attaching the corresponding
          // consumer.
          long lagBasedOnMetrics =
              getPartitionOffsetLagBasedOnMetrics(resolvedKafkaUrl, resolvedLeaderTopic, pcs.getPartition());
          if (lagBasedOnMetrics >= 0) {
            return lagBasedOnMetrics;
          }

          // Fall back to calculate offset lag in the old way
          return measureLagWithCallToPubSub(
              resolvedKafkaUrl,
              resolvedLeaderTopic,
              pcs.getPartition(),
              pcs.getLeaderConsumedUpstreamRTOffset(kafkaSourceAddress));
        })
        .filter(VALID_LAG)
        .sum();

    return minZeroLag(offsetLag);
  }

  /**
   * For stores in aggregate mode this is optimistic and returns the minimum lag of all fabric. This is because in
   * aggregate mode duplicate msg consumption happen from all fabric. So it should be fine to consider the lowest lag.
   *
   * For stores in active/active mode, if no fabric is unreachable, return the maximum lag of all fabrics. If only one
   * fabric is unreachable, return the maximum lag of other fabrics. If more than one fabrics are unreachable, return
   * Long.MAX_VALUE, which means the partition is not ready-to-serve.
   * TODO: For active/active incremental push stores or stores with only one samza job, we should consider the weight of
   * unreachable fabric and make the decision. For example, we should not let partition ready-to-serve when the only
   * source fabric is unreachable.
   *
   * In non-aggregate mode of consumption only return the local fabric lag
   * @param sourceRealTimeTopicKafkaURLs
   * @param partitionConsumptionState
   * @param shouldLogLag
   * @return
   */
  @Override
  protected long measureRTOffsetLagForMultiRegions(
      Set<String> sourceRealTimeTopicKafkaURLs,
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    long maxLag = Long.MIN_VALUE;
    int numberOfUnreachableRegions = 0;
    for (String sourceRealTimeTopicKafkaURL: sourceRealTimeTopicKafkaURLs) {
      try {
        long lag =
            measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
        maxLag = Math.max(lag, maxLag);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to measure RT offset lag for replica: {} in {}/{}",
            partitionConsumptionState.getReplicaId(),
            Utils.getReplicaId(
                partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository),
                partitionConsumptionState.getPartition()),
            sourceRealTimeTopicKafkaURL,
            e);
        if (++numberOfUnreachableRegions > 1) {
          LOGGER.error(
              "More than one regions are unreachable. Returning lag: {} as replica: {} may not be ready-to-serve.",
              Long.MAX_VALUE,
              partitionConsumptionState.getReplicaId());
          return Long.MAX_VALUE;
        }
      }
    }
    return maxLag;
  }

  /** used for metric purposes **/
  @Override
  public boolean isReadyToServeAnnouncedWithRTLag() {
    if (!hybridStoreConfig.isPresent() || partitionConsumptionStateMap.isEmpty()) {
      return false;
    }
    long offsetLagThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
    for (PartitionConsumptionState pcs: partitionConsumptionStateMap.values()) {
      if (pcs.hasLagCaughtUp() && offsetLagThreshold >= 0) {
        // If pcs is marked as having caught up, but we're not ready to serve, that means we're lagging
        // after having announced that we are ready to serve.
        try {
          if (!this.isReadyToServe(pcs)) {
            return true;
          }
        } catch (Exception e) {
          // Something wasn't reachable, we'll report that something is amiss.
          return true;
        }
      }
    }
    return false;
  }

  Runnable buildRepairTask(
      String sourceKafkaUrl,
      PubSubTopicPartition sourceTopicPartition,
      long rewindStartTimestamp,
      PartitionConsumptionState pcs) {
    return () -> {
      PubSubTopic pubSubTopic = sourceTopicPartition.getPubSubTopic();
      PubSubTopicPartition resolvedTopicPartition = resolveTopicPartitionWithKafkaURL(pubSubTopic, pcs, sourceKafkaUrl);
      // Calculate upstream offset
      long upstreamOffset =
          getTopicPartitionOffsetByKafkaURL(sourceKafkaUrl, resolvedTopicPartition, rewindStartTimestamp);
      // Subscribe (unsubscribe should have processed correctly regardless of remote broker state)
      consumerSubscribe(pubSubTopic, pcs, upstreamOffset, sourceKafkaUrl);
      // syncConsumedUpstreamRTOffsetMapIfNeeded
      Map<String, Long> urlToOffsetMap = new HashMap<>();
      urlToOffsetMap.put(sourceKafkaUrl, upstreamOffset);
      syncConsumedUpstreamRTOffsetMapIfNeeded(pcs, urlToOffsetMap);

      LOGGER.info(
          "Successfully repaired consumption and subscribed to {} at offset {}",
          sourceTopicPartition,
          upstreamOffset);
    };
  }

  @Override
  int getRmdProtocolVersionId() {
    return rmdProtocolVersionId;
  }

  @Override
  MergeConflictResolver getMergeConflictResolver() {
    return mergeConflictResolver;
  }

  @Override
  RmdSerDe getRmdSerDe() {
    return rmdSerDe;
  }

  /**
   * This method does a few things for leader topic-partition subscription:
   * (1) Calculate Kafka URL to leader subscribe offset map.
   * (2) Subscribe to all the Kafka upstream.
   * (3) Potentially sync offset to PartitionConsumptionState map if needed.
   */
  void prepareLeaderOffsetCheckpointAndStartConsumptionAsLeader(
      PubSubTopic leaderTopic,
      PartitionConsumptionState partitionConsumptionState,
      boolean calculateUpstreamOffsetFromTopicSwitch) {
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    Map<String, Long> leaderOffsetByKafkaURL = new HashMap<>(leaderSourceKafkaURLs.size());
    List<CharSequence> unreachableBrokerList = new ArrayList<>();
    // TODO: Potentially this logic can be merged into below branch.
    if (calculateUpstreamOffsetFromTopicSwitch) {
      leaderOffsetByKafkaURL =
          calculateLeaderUpstreamOffsetWithTopicSwitch(partitionConsumptionState, leaderTopic, unreachableBrokerList);
    } else {
      // Read previously checkpointed offset and maybe fallback to TopicSwitch if any of upstream offset is missing.
      for (String kafkaURL: leaderSourceKafkaURLs) {
        leaderOffsetByKafkaURL
            .put(kafkaURL, partitionConsumptionState.getLeaderOffset(kafkaURL, pubSubTopicRepository));
      }
      if (leaderTopic.isRealTime() && leaderOffsetByKafkaURL.containsValue(OffsetRecord.LOWEST_OFFSET)) {
        leaderOffsetByKafkaURL =
            calculateLeaderUpstreamOffsetWithTopicSwitch(partitionConsumptionState, leaderTopic, unreachableBrokerList);
      }
    }

    if (!unreachableBrokerList.isEmpty()) {
      LOGGER.warn(
          "Failed to reach broker urls {}, will schedule retry to compute upstream offset and resubscribe!",
          unreachableBrokerList.toString());
    }
    // subscribe to the new upstream
    leaderOffsetByKafkaURL.forEach((kafkaURL, leaderStartOffset) -> {
      consumerSubscribe(leaderTopic, partitionConsumptionState, leaderStartOffset, kafkaURL);
    });

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, leaderOffsetByKafkaURL);

    LOGGER.info(
        "{}, as a leader, started consuming from topic {} partition {} with offset by Kafka URL mapping {}",
        ingestionTaskName,
        leaderTopic,
        partitionConsumptionState.getPartition(),
        leaderOffsetByKafkaURL);
  }

  private long calculateRewindStartTime(PartitionConsumptionState partitionConsumptionState) {
    long rewindStartTime;
    long rewindTimeInMs = hybridStoreConfig.get().getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    if (isDataRecovery) {
      // Override the user rewind if the version is under data recovery to avoid data loss when user have short rewind.
      rewindTimeInMs = Math.max(PubSubConstants.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN, rewindTimeInMs);
    }
    switch (hybridStoreConfig.get().getBufferReplayPolicy()) {
      case REWIND_FROM_SOP:
        rewindStartTime = partitionConsumptionState.getStartOfPushTimestamp() - rewindTimeInMs;
        break;
      case REWIND_FROM_EOP:
      default:
        rewindStartTime = partitionConsumptionState.getEndOfPushTimestamp() - rewindTimeInMs;
    }
    return rewindStartTime;
  }

}
