package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.ingestion.LagType.OFFSET_LAG;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.LEADER_TO_STANDBY;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.LagType;
import com.linkedin.davinci.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.view.ChangeCaptureViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains the state transition work between leader and follower; both leader and follower
 * will keep track of information like which topic leader is consuming from and the corresponding offset
 * as well as the latest successfully consumed or produced offset in the version topic (VT).
 *
 * State Transition:
 *     1. OFFLINE -> STANDBY:
 *        Generate a SUBSCRIBE message in the consumer action queue; the logic
 *        here is the same as Online/Offline model; all it needs to do is to
 *        restore the checkpointed state from OffsetRecord;
 *     2. STANDBY -> LEADER:
 *        The partition will be marked as in the transition progress from STANDBY
 *        to LEADER and completes the action immediately; after processing the rest
 *        of the consumer actions in the queue, check whether there is any partition
 *        is in the transition progress, if so:
 *        (i)   consume the latest messages from version topic;
 *        (ii)  drain all the messages in drainer queue in order to update the latest
 *              consumed message replication metadata;
 *        (iii) check whether there has been at least 5 minutes (configurable) of
 *              inactivity for this partition (meaning no new messages); if so,
 *              turn on the LEADER flag for this partition.
 *     3. LEADER -> STANDBY:
 *        a. if the leader is consuming from VT, just set "isLeader" field to
 *           false and resume consumption;
 *        b. if the leader is consuming from anything other than VT, it needs to
 *           unsubscribe from the leader topic for this partition first, drain
 *           all the messages in the drainer queue for this leader topic/partition
 *           so that it can get the last producer callback for the last message it
 *           produces to VT; block on getting the result from the callback to
 *           update the corresponding offset in version topic, so that the new
 *           follower can subscribe back to VT using the recently updated VT offset.
 */
public class LeaderFollowerStoreIngestionTask extends StoreIngestionTask {
  private static final Logger LOGGER = LogManager.getLogger(LeaderFollowerStoreIngestionTask.class);

  /**
   * The new leader will stay inactive (not switch to any new topic or produce anything) for
   * some time after seeing the last messages in version topic.
   */
  private final long newLeaderInactiveTime;
  private final StoreWriteComputeProcessor storeWriteComputeHandler;
  private final boolean isNativeReplicationEnabled;
  private final String nativeReplicationSourceVersionTopicKafkaURL;
  private final Set<String> nativeReplicationSourceVersionTopicKafkaURLSingletonSet;
  private final VeniceWriterFactory veniceWriterFactory;

  private final HeartbeatMonitoringService heartbeatMonitoringService;

  /**
   * Leader must maintain producer DIV states separate from drainers, because leader is always ahead of drainer;
   * if leader and drainer share the same DIV validator, leader will pollute the data in shared DIV validator;
   * in other words, if leader shares the same DIV validator with drainer, leader will move the DIV info ahead of the
   * actual persisted data.
   *
   * N.B.: Note that currently the state clearing happens only in the "main" {@link KafkaDataIntegrityValidator},
   * located in the parent class, used by drainers, and which persist its state to disk. This one here is transient
   * and not persisted to disk. As such, its state is not expected to grow as large, and bouncing effectively clears
   * it (which is not the case for the other one which gets persisted). Later on, we could trigger state cleaning
   * for this leader validator as well, if deemed necessary.
   */
  private final KafkaDataIntegrityValidator kafkaDataIntegrityValidatorForLeaders;

  /**
   * N.B.:
   *    With L/F+native replication and many Leader partitions getting assigned to a single SN this {@link VeniceWriter}
   *    may be called from multiple thread simultaneously, during start of batch push. Therefore, we wrap it in
   *    {@link Lazy} to initialize it in a thread safe way and to ensure that only one instance is created for the
   *    entire ingestion task.
   *
   *    Important:
   *    Please don't use these writers directly, and you should retrieve the writer from {@link PartitionConsumptionState#getVeniceWriterLazyRef()}
   *    when producing to the local topic.
   */
  protected Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriter;
  protected final Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterForRealTime;
  protected final Int2ObjectMap<String> kafkaClusterIdToUrlMap;
  private long dataRecoveryCompletionTimeLagThresholdInMs = 0;

  protected final Map<String, VeniceViewWriter> viewWriters;
  protected final boolean hasChangeCaptureView;

  protected final AvroStoreDeserializerCache storeDeserializerCache;

  private final AtomicLong lastSendIngestionHeartbeatTimestamp = new AtomicLong(0);

  private final Lazy<IngestionBatchProcessor> ingestionBatchProcessingLazy;
  private final Version version;

  public LeaderFollowerStoreIngestionTask(
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
        builder.getLeaderFollowerNotifiers(),
        zkHelixAdmin);
    this.version = version;
    this.heartbeatMonitoringService = builder.getHeartbeatMonitoringService();
    /**
     * We are going to apply fast leader failover for per user store system store since it is time sensitive, and if the
     * split-brain problem happens in prod, we could design a way to periodically produce snapshot to the meta system
     * store to make correction in the future.
     */
    if (isUserSystemStore()) {
      newLeaderInactiveTime = serverConfig.getServerSystemStorePromotionToLeaderReplicaDelayMs();
    } else {
      newLeaderInactiveTime = serverConfig.getServerPromotionToLeaderReplicaDelayMs();
    }
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    this.storeWriteComputeHandler = new StoreWriteComputeProcessor(
        storeName,
        schemaRepository,
        mergeRecordHelper,
        serverConfig.isComputeFastAvroEnabled());
    this.isNativeReplicationEnabled = version.isNativeReplicationEnabled();

    /**
     * Native replication could also be used to perform data recovery by pointing the source version topic kafka url to
     * a topic with good data in another child fabric.
     * During data recovery for DaVinci client running in follower mode does not have the configs needed for data recovery
     * which leads to DaVinci host ingestion failure, ignore data recovery config for daVinci.
     */
    if (version.getDataRecoveryVersionConfig() == null || isDaVinciClient) {
      this.nativeReplicationSourceVersionTopicKafkaURL = version.getPushStreamSourceAddress();
      isDataRecovery = false;
    } else {
      this.nativeReplicationSourceVersionTopicKafkaURL = serverConfig.getKafkaClusterIdToUrlMap()
          .get(
              serverConfig.getKafkaClusterAliasToIdMap()
                  .getInt(version.getDataRecoveryVersionConfig().getDataRecoverySourceFabric()));
      if (nativeReplicationSourceVersionTopicKafkaURL == null) {
        throw new VeniceException(
            "Unable to get data recovery source kafka url from the provided source fabric:"
                + version.getDataRecoveryVersionConfig().getDataRecoverySourceFabric());
      }
      isDataRecovery = true;
      dataRecoverySourceVersionNumber = version.getDataRecoveryVersionConfig().getDataRecoverySourceVersionNumber();
      if (isHybridMode()) {
        dataRecoveryCompletionTimeLagThresholdInMs = PubSubConstants.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN / 2;
        LOGGER.info(
            "Data recovery info for topic: {}, source kafka url: {}, time lag threshold for completion: {}",
            getVersionTopic(),
            nativeReplicationSourceVersionTopicKafkaURL,
            dataRecoveryCompletionTimeLagThresholdInMs);
      }
    }
    this.nativeReplicationSourceVersionTopicKafkaURLSingletonSet =
        Collections.singleton(nativeReplicationSourceVersionTopicKafkaURL);
    LOGGER.info(
        "Native replication source version topic kafka url set to: {} for topic: {}",
        nativeReplicationSourceVersionTopicKafkaURL,
        getVersionTopic());

    this.veniceWriterFactory = builder.getVeniceWriterFactory();
    /**
     * In general, a partition in version topic follows this pattern:
     * {Start_of_Segment, Start_of_Push, End_of_Segment, Start_of_Segment, data..., End_of_Segment, Start_of_Segment, End_of_Push, End_of_Segment}
     * Therefore, in native replication where leader needs to producer all messages it consumes from remote, the first
     * message that leader consumes is not SOP, in this case, leader doesn't know whether chunking is enabled.
     *
     * Notice that the pattern is different in stream reprocessing which contains a lot more segments and is also
     * different in some test cases which reuse the same VeniceWriter.
     */
    String versionTopicName = getVersionTopic().getName();
    this.veniceWriter = Lazy.of(() -> constructVeniceWriter(veniceWriterFactory, versionTopicName, version, true, 1));
    if (getServerConfig().isNearlineWorkloadProducerThroughputOptimizationEnabled() && isHybridMode()
        && (!store.isNearlineProducerCompressionEnabled() || store.getNearlineProducerCountPerWriter() > 1)) {
      this.veniceWriterForRealTime = Lazy.of(() -> {
        LOGGER.info(
            "Constructing a VeniceWriter with producer compression: {} and producer count:{} for topic: {} for nearline workload",
            store.isNearlineProducerCompressionEnabled(),
            store.getNearlineProducerCountPerWriter(),
            versionTopicName);
        return constructVeniceWriter(
            veniceWriterFactory,
            versionTopicName,
            version,
            store.isNearlineProducerCompressionEnabled(),
            store.getNearlineProducerCountPerWriter());
      });
    } else {
      this.veniceWriterForRealTime = this.veniceWriter;
    }

    this.kafkaClusterIdToUrlMap = serverConfig.getKafkaClusterIdToUrlMap();
    this.kafkaDataIntegrityValidatorForLeaders = new KafkaDataIntegrityValidator(kafkaVersionTopic);
    if (builder.getVeniceViewWriterFactory() != null && !store.getViewConfigs().isEmpty()) {
      viewWriters = builder.getVeniceViewWriterFactory()
          .buildStoreViewWriters(
              store,
              version.getNumber(),
              schemaRepository.getKeySchema(store.getName()).getSchema());
      boolean tmpValueForHasChangeCaptureViewWriter = false;
      for (Map.Entry<String, VeniceViewWriter> viewWriter: viewWriters.entrySet()) {
        if (viewWriter.getValue() instanceof ChangeCaptureViewWriter) {
          tmpValueForHasChangeCaptureViewWriter = true;
          break;
        }
      }
      hasChangeCaptureView = tmpValueForHasChangeCaptureViewWriter;
    } else {
      viewWriters = Collections.emptyMap();
      hasChangeCaptureView = false;
    }
    this.storeDeserializerCache = new AvroStoreDeserializerCache(
        builder.getSchemaRepo(),
        getStoreName(),
        serverConfig.isComputeFastAvroEnabled());
    this.ingestionBatchProcessingLazy = Lazy.of(() -> {
      if (!serverConfig.isAAWCWorkloadParallelProcessingEnabled()) {
        LOGGER.info("AA/WC workload parallel processing is disabled for store version: {}", getKafkaVersionTopic());
        return null;
      }
      LOGGER.info("AA/WC workload parallel processing is enabled for store version: {}", getKafkaVersionTopic());
      return new IngestionBatchProcessor(
          kafkaVersionTopic,
          parallelProcessingThreadPool,
          null,
          isWriteComputationEnabled,
          isActiveActiveReplicationEnabled(),
          builder.getVersionedStorageIngestionStats(),
          getHostLevelIngestionStats());
    });
  }

  public static VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
      VeniceWriterFactory veniceWriterFactory,
      String topic,
      Version version,
      boolean producerCompressionEnabled,
      int producerCnt) {
    PartitionerConfig partitionerConfig = version.getPartitionerConfig();
    VenicePartitioner venicePartitioner = partitionerConfig == null
        ? new DefaultVenicePartitioner()
        : PartitionUtils.getVenicePartitioner(partitionerConfig);
    return veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setPartitioner(venicePartitioner)
            .setChunkingEnabled(version.isChunkingEnabled())
            .setRmdChunkingEnabled(version.isRmdChunkingEnabled())
            .setPartitionCount(version.getPartitionCount())
            .setProducerCompressionEnabled(producerCompressionEnabled)
            .setProducerCount(producerCnt)
            .build());
  }

  @Override
  public void closeVeniceWriters(boolean doFlush) {
    if (veniceWriter.isPresent()) {
      veniceWriter.get().close(doFlush);
    }
    if (veniceWriterForRealTime.isPresent()) {
      veniceWriterForRealTime.get().close(doFlush);
    }
  }

  @Override
  IngestionBatchProcessor getIngestionBatchProcessor() {
    return ingestionBatchProcessingLazy.get();
  }

  @Override
  public Map<String, VeniceViewWriter> getViewWriters() {
    return viewWriters;
  }

  @Override
  protected void closeVeniceViewWriters() {
    if (!viewWriters.isEmpty()) {
      viewWriters.forEach((k, v) -> v.close());
    }
  }

  @Override
  public synchronized void promoteToLeader(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    partitionToPendingConsumerActionCountMap
        .computeIfAbsent(topicPartition.getPartitionNumber(), x -> new AtomicInteger(0))
        .incrementAndGet();
    consumerActionsQueue.add(new ConsumerAction(STANDBY_TO_LEADER, topicPartition, nextSeqNum(), checker, true));
  }

  @Override
  public synchronized void demoteToStandby(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    partitionToPendingConsumerActionCountMap
        .computeIfAbsent(topicPartition.getPartitionNumber(), x -> new AtomicInteger(0))
        .incrementAndGet();
    consumerActionsQueue.add(new ConsumerAction(LEADER_TO_STANDBY, topicPartition, nextSeqNum(), checker, true));
  }

  @Override
  protected void processConsumerAction(ConsumerAction message, Store store) throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topicName = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case STANDBY_TO_LEADER:
        LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker = message.getLeaderSessionIdChecker();
        if (!checker.isSessionIdValid()) {
          /**
           * If the session id in this consumer action is not equal to the latest session id in the state model,
           * it indicates that Helix has already assigned a new role to this replica (can be leader or follower),
           * so quickly skip this state transition and go straight to the final transition.
           */
          LOGGER.warn(
              "State transition from STANDBY to LEADER is skipped for replica: {}, because Helix has assigned another role to it.",
              Utils.getReplicaId(topicName, partition));
          return;
        }

        PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState == null) {
          LOGGER.warn(
              "State transition from STANDBY to LEADER is skipped for replica: {}, because PCS is null and the partition may have been unsubscribed.",
              Utils.getReplicaId(topicName, partition));
          return;
        }

        if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
          LOGGER.warn(
              "State transition from STANDBY to LEADER is skipped for replica: {} as it is already the partition leader.",
              Utils.getReplicaId(topicName, partition));
          return;
        }
        if (store.isMigrationDuplicateStore()) {
          partitionConsumptionState.setLeaderFollowerState(PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER);
          LOGGER.warn(
              "State transition from STANDBY to LEADER is paused for replica: {} as this store is undergoing migration",
              partitionConsumptionState.getReplicaId());
        } else {
          // Mark this partition in the middle of STANDBY to LEADER transition
          partitionConsumptionState.setLeaderFollowerState(IN_TRANSITION_FROM_STANDBY_TO_LEADER);
          LOGGER.info(
              "Replica: {} is in state transition from STANDBY to LEADER",
              partitionConsumptionState.getReplicaId());
        }
        break;
      case LEADER_TO_STANDBY:
        checker = message.getLeaderSessionIdChecker();
        if (!checker.isSessionIdValid()) {
          /**
           * If the session id in this consumer action is not equal to the latest session id in the state model,
           * it indicates that Helix has already assigned a new role to this replica (can be leader or follower),
           * so quickly skip this state transition and go straight to the final transition.
           */
          LOGGER.warn(
              "State transition from LEADER to STANDBY is skipped for replica: {}, because Helix has assigned another role to this replica.",
              Utils.getReplicaId(topicName, partition));
          return;
        }

        partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState == null) {
          LOGGER.warn(
              "State transition from LEADER to STANDBY is skipped for replica: {}, because PCS is null and the partition may have been unsubscribed.",
              Utils.getReplicaId(topicName, partition));
          return;
        }

        if (partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)) {
          LOGGER.warn(
              "State transition from LEADER to STANDBY is skipped for replica: {} as this replica is already a follower.",
              partitionConsumptionState.getReplicaId());
          return;
        }

        /**
         * 1. If leader(itself) was consuming from local VT previously, just set the state as STANDBY for this partition;
         * 2. otherwise, leader would unsubscribe from the previous feed topic (real-time topic, reprocessing
         *    transient topic or remote VT); then drain all the messages from the feed topic, which would produce the
         *    corresponding result message to local VT; block on the callback of the final message that it needs to
         *    produce; finally the new follower will switch back to consume from local VT using the latest VT offset
         *    tracked by producer callback.
         */
        OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
        PubSubTopic topic = message.getTopicPartition().getPubSubTopic();
        PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
        if (leaderTopic != null && (!topic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely())) {
          unsubscribeFromTopic(leaderTopic, partitionConsumptionState);
          waitForAllMessageToBeProcessedFromTopicPartition(
              new PubSubTopicPartitionImpl(leaderTopic, partition),
              partitionConsumptionState);

          partitionConsumptionState.setConsumeRemotely(false);
          LOGGER.info(
              "Disabled remote consumption from topic-partition: {} for replica: {}",
              Utils.getReplicaId(leaderTopic, partition),
              partitionConsumptionState.getReplicaId());
          // Followers always consume local VT and should not skip kafka message
          partitionConsumptionState.setSkipKafkaMessage(false);
          partitionConsumptionState.setLeaderFollowerState(STANDBY);
          updateLeaderTopicOnFollower(partitionConsumptionState);
          // subscribe back to local VT/partition
          consumerSubscribe(
              topic,
              partitionConsumptionState,
              partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
              localKafkaServer);
          /**
           * When switching leader to follower, we may adjust the underlying storage partition to optimize the performance.
           * Only adjust the storage engine after the batch portion as compaction tuning is meaningless for the batch portion.
           */
          if (partitionConsumptionState.isEndOfPushReceived()) {
            storageEngine.adjustStoragePartition(
                partition,
                AbstractStorageEngine.StoragePartitionAdjustmentTrigger.DEMOTE_TO_FOLLOWER,
                getStoragePartitionConfig(partitionConsumptionState));
          }
        } else {
          partitionConsumptionState.setLeaderFollowerState(STANDBY);
          updateLeaderTopicOnFollower(partitionConsumptionState);
        }
        LOGGER.info("Replica: {} moved to standby/follower state", partitionConsumptionState.getReplicaId());

        /**
         * Close the writer to make sure the current segment is closed after the leader is demoted to standby.
         *
         * An NPE can happen if:
         * 1. A partition receives STANDBY to LEADER transition, but not yet fully finished to set veniceWriterLazyRef
         *   in PCS (e.g. still in IN_TRANSITION_FROM_STANDBY_TO_LEADER).
         * 2. Then it receives LEADER to STANDBY transition and veniceWriterLazyRef is still null in PCS.
         */
        // If the VeniceWriter doesn't exist, then no need to end any segment, and this function becomes a no-op
        Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterLazyRef =
            partitionConsumptionState.getVeniceWriterLazyRef();
        if (veniceWriterLazyRef != null) {
          veniceWriterLazyRef.ifPresent(vw -> vw.closePartition(partition));
        }
        break;
      default:
        processCommonConsumerAction(message);
    }
  }

  /**
   * The following function will be executed after processing all the quick actions in the consumer action queues,
   * so that the long running actions doesn't block other partition's consumer actions. Besides, there is no thread
   * sleeping operations in this function in order to be efficient, but this function will be invoked again and again in
   * the main loop of the StoreIngestionTask to check whether some long-running actions can finish now.
   *
   * The only drawback is that for regular batch push, leader flag is never on at least a few minutes after the leader
   * consumes the last message (END_OF_PUSH), which is an acceptable trade-off for us in order to share and test the
   * same code path between regular push job, hybrid store and reprocessing job.
   */
  @Override
  protected void checkLongRunningTaskState() throws InterruptedException {
    boolean pushTimeout = false;
    Set<Integer> timeoutPartitions = null;
    long checkStartTimeInNS = System.nanoTime();
    for (PartitionConsumptionState partitionConsumptionState: partitionConsumptionStateMap.values()) {
      final int partition = partitionConsumptionState.getPartition();

      /**
       * Check whether the push timeout
       */
      if (!partitionConsumptionState.isComplete() && LatencyUtils.getElapsedTimeFromMsToMs(
          partitionConsumptionState.getConsumptionStartTimeInMs()) > this.bootstrapTimeoutInMs) {
        if (!pushTimeout) {
          pushTimeout = true;
          timeoutPartitions = new HashSet<>();
        }
        timeoutPartitions.add(partition);
      }
      switch (partitionConsumptionState.getLeaderFollowerState()) {
        case PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER:
          Store store = storeRepository.getStoreOrThrow(storeName);
          if (!store.isMigrationDuplicateStore()) {
            partitionConsumptionState.setLeaderFollowerState(IN_TRANSITION_FROM_STANDBY_TO_LEADER);
            LOGGER.info(
                "Resumed state transition from STANDBY to LEADER for replica: {}",
                partitionConsumptionState.getReplicaId());
          }
          break;

        case IN_TRANSITION_FROM_STANDBY_TO_LEADER:
          if (canSwitchToLeaderTopic(partitionConsumptionState)) {
            LOGGER.info(
                "Initiating promotion of replica: {} to leader for the partition. Unsubscribing from the current topic: {}",
                partitionConsumptionState.getReplicaId(),
                kafkaVersionTopic);
            /**
             * There isn't any new message from the old leader for at least {@link newLeaderInactiveTime} minutes,
             * this replica can finally be promoted to leader.
             */
            // unsubscribe from previous topic/partition
            unsubscribeFromTopic(versionTopic, partitionConsumptionState);

            LOGGER.info(
                "Starting promotion of replica: {} to leader for the partition, unsubscribed from current topic: {}",
                partitionConsumptionState.getReplicaId(),
                kafkaVersionTopic);
            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            if (offsetRecord.getLeaderTopic(pubSubTopicRepository) == null) {
              /**
               * If this follower has processed a TS, the leader topic field should have been set. So, it must have been
               * consuming from version topic. Now it is becoming the leader. So the VT becomes its leader topic.
               */
              offsetRecord.setLeaderTopic(versionTopic);
            }

            // Setup venice writer reference for producing
            if (partitionConsumptionState.isEndOfPushReceived()) {
              partitionConsumptionState.setVeniceWriterLazyRef(veniceWriterForRealTime);
            } else {
              partitionConsumptionState.setVeniceWriterLazyRef(veniceWriter);
            }

            /**
             * When leader promotion actually happens and before leader switch to a new topic, restore the latest DIV check
             * results from drainer service to the DIV check validator that will be used in leader consumption thread
             */
            restoreProducerStatesForLeaderConsumption(partition);

            startConsumingAsLeaderInTransitionFromStandby(partitionConsumptionState);
            /**
             * May adjust the underlying storage partition to optimize the ingestion performance.
             * Only adjust the underlying storage partition after batch portion as the compaction
             * tuning is not meaningful for batch portion.
             */
            if (partitionConsumptionState.isEndOfPushReceived()) {
              storageEngine.adjustStoragePartition(
                  partition,
                  AbstractStorageEngine.StoragePartitionAdjustmentTrigger.PROMOTE_TO_LEADER,
                  getStoragePartitionConfig(partitionConsumptionState));
            }

            /**
             * The topic switch operation will be recorded but the actual topic switch happens only after the replica
             * is promoted to leader; we should check whether it's ready to serve after switching topic.
             *
             * In extreme case, if there is no message in real-time topic, there will be no new message after leader switch
             * to the real-time topic, so `isReadyToServe()` check will never be invoked.
             */
            maybeApplyReadyToServeCheck(partitionConsumptionState);
          }
          break;

        case LEADER:
          /**
           * Leader should finish consuming all the messages inside version topic before switching to real-time topic;
           * if upstreamOffset exists, rewind to RT with the upstreamOffset instead of using the start timestamp in TS.
           */
          PubSubTopic currentLeaderTopic =
              partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null) {
            String errorMsg =
                "Missing leader topic for actual leader replica: " + partitionConsumptionState.getReplicaId()
                    + ". OffsetRecord: " + partitionConsumptionState.getOffsetRecord().toSimplifiedString();
            LOGGER.error(errorMsg);
            throw new VeniceException(errorMsg);
          }

          /** If LEADER is consuming remote VT or SR, EOP is already received, switch back to local fabrics. */
          if (shouldLeaderSwitchToLocalConsumption(partitionConsumptionState)) {
            // Unsubscribe from remote Kafka topic, but keep the consumer in cache.
            unsubscribeFromTopic(currentLeaderTopic, partitionConsumptionState);
            // If remote consumption flag is false, existing messages for the partition in the drainer queue should be
            // processed before that
            PubSubTopicPartition leaderTopicPartition =
                new PubSubTopicPartitionImpl(currentLeaderTopic, partitionConsumptionState.getPartition());
            waitForAllMessageToBeProcessedFromTopicPartition(leaderTopicPartition, partitionConsumptionState);

            partitionConsumptionState.setConsumeRemotely(false);
            LOGGER.info(
                "Disabled remote consumption from topic-partition: {} for replica: {}",
                Utils.getReplicaId(currentLeaderTopic, partition),
                partitionConsumptionState.getReplicaId());
            if (isDataRecovery && partitionConsumptionState.isBatchOnly() && !versionTopic.equals(currentLeaderTopic)) {
              partitionConsumptionState.getOffsetRecord().setLeaderTopic(versionTopic);
              currentLeaderTopic = versionTopic;
            }
            /**
             * The flag is turned on in {@link StorePartitionDataReceiver#shouldProcessRecord} avoid consuming
             * unwanted messages after EOP in remote VT, such as SOBR. Now that the leader switches to consume locally,
             * it should not skip any message.
             */
            partitionConsumptionState.setSkipKafkaMessage(false);
            // Subscribe to local Kafka topic
            consumerSubscribe(
                currentLeaderTopic,
                partitionConsumptionState,
                partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
                localKafkaServer);
          }

          TopicSwitchWrapper topicSwitchWrapper = partitionConsumptionState.getTopicSwitch();
          if (topicSwitchWrapper == null) {
            break;
          }

          PubSubTopic newSourceTopic = topicSwitchWrapper.getNewSourceTopic();
          if (currentLeaderTopic.equals(newSourceTopic)) {
            break;
          }

          /**
           * Otherwise, execute the TopicSwitch message stored in metadata store if one of the below conditions is true:
           * 1. it has been 5 minutes since the last update in the current topic
           * 2. leader is consuming SR topic right now and TS wants leader to switch to another topic.
           */
          long lastTimestamp = getLastConsumedMessageTimestamp(partition);
          if (LatencyUtils.getElapsedTimeFromMsToMs(lastTimestamp) > newLeaderInactiveTime
              || switchAwayFromStreamReprocessingTopic(currentLeaderTopic, newSourceTopic)) {
            leaderExecuteTopicSwitch(partitionConsumptionState, topicSwitchWrapper.getTopicSwitch(), newSourceTopic);
          }
          break;

        case STANDBY:
          // no long running task for follower
          break;
      }
    }
    if (emitMetrics.get()) {
      hostLevelIngestionStats
          .recordCheckLongRunningTasksLatency(LatencyUtils.getElapsedTimeFromNSToMS(checkStartTimeInNS));
    }

    if (pushTimeout) {
      // Timeout
      String errorMsg = "After waiting " + TimeUnit.MILLISECONDS.toHours(this.bootstrapTimeoutInMs)
          + " hours, resource:" + storeName + " partitions:" + timeoutPartitions + " still can not complete ingestion.";
      LOGGER.error(errorMsg);
      throw new VeniceTimeoutException(errorMsg);
    }

    checkWhetherToCloseUnusedVeniceWriter(veniceWriter, veniceWriterForRealTime, partitionConsumptionStateMap, () -> {
      veniceWriter =
          Lazy.of(() -> constructVeniceWriter(veniceWriterFactory, getVersionTopic().getName(), version, true, 1));
    }, getVersionTopic().getName());
  }

  protected static boolean checkWhetherToCloseUnusedVeniceWriter(
      Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterLazy,
      Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterForRealTimeLazy,
      Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap,
      Runnable reInitializeVeniceWriterLazyRunnable,
      String versionTopicName) {
    if (!veniceWriterLazy.isPresent() || !veniceWriterForRealTimeLazy.isPresent()) {
      // There is at most one valid Venice writer, so we don't need to clean up anything here.
      return false;
    }
    if (veniceWriterLazy.get().equals(veniceWriterForRealTimeLazy.get())) {
      // Both Venice writers point to the same instance, so no cleanup
      return false;
    }
    /**
     * Now, {@link #veniceWriterForRealTime} is referring to a different instance from {@link #veniceWriter},
     * and we need to check whether we can close {@link #veniceWriter} or not.
     * The criteria are that all the leader partitions in the current node have finished the producing of the batch data.
     */
    for (PartitionConsumptionState partitionConsumptionState: partitionConsumptionStateMap.values()) {
      if (isLeader(partitionConsumptionState) && !partitionConsumptionState.isEndOfPushReceived()) {
        /**
         * There are some leader partitions, which are still consuming batch data.
         */
        return false;
      }
    }
    /**
     * Now, we can close the VeniceWriter, which is used to produce batch data to free up resources,
     * and here, we will re-initialize {@link #veniceWriter} to handle some edge cases, which would
     * re-produce the batch data to the local Kafka cluster.
     */
    veniceWriterLazy.get().close(true);
    LOGGER.info(
        "Closed VeniceWriter for batch data producing to free up resource for store version: {}",
        versionTopicName);
    reInitializeVeniceWriterLazyRunnable.run();
    return true;
  }

  private boolean canSwitchToLeaderTopic(PartitionConsumptionState pcs) {
    /**
     * Potential risk: it's possible that Kafka consumer would starve one of the partitions for a long
     * time even though there are new messages in it, so it's possible that the old leader is still producing
     * after waiting for 5 minutes; if it does happen, followers will detect upstream offset rewind by
     * a different producer host name.
     */
    long lastTimestamp = getLastConsumedMessageTimestamp(pcs.getPartition());
    if (LatencyUtils.getElapsedTimeFromMsToMs(lastTimestamp) <= newLeaderInactiveTime) {
      return false;
    }

    /**
     * For user system stores, it requires all messages in local VT topic to be consumed before switching to leader topic,
     * otherwise, it can get into an error replica issue when the followings happen:
     *
     * 1. Consumed RT data is still less than threshold thus has not been persisted to disk {@link StoreIngestionTask#shouldSyncOffset}.
     *    After host restarts, the RT offset retrieved from disk is stale.
     * 2. RT is truncated due to retention and has no data message in it.
     * 3. User system store switches to RT so quickly that has not yet fully consumed data from local VT.
     *
     * When all above conditions happen together, user system stores switch to RT, but subscribe to a stale offset and
     * cannot consume anything from it (empty) nor update, thus
     *    RT end offset - offset in leader replica > threshold
     * and replica cannot become online {@link StoreIngestionTask#isReadyToServe}.
     */
    if (isUserSystemStore() && !isLocalVersionTopicPartitionFullyConsumed(pcs)) {
      return false;
    }
    return true;
  }

  /**
   * @return <code>true</code>, if local kafka topic partition is fully consumed by comparing its offset against end offset;
   *         <code>false</code>, otherwise.
   */
  private boolean isLocalVersionTopicPartitionFullyConsumed(PartitionConsumptionState pcs) {
    long localVTOff = pcs.getLatestProcessedLocalVersionTopicOffset();
    long localVTEndOffset = getTopicPartitionEndOffSet(localKafkaServer, versionTopic, pcs.getPartition());

    if (localVTEndOffset == StatsErrorCode.LAG_MEASUREMENT_FAILURE.code) {
      return false;
    }

    // If end offset == 0, then no message has been written to the partition, consider it as fully consumed.
    if (localVTEndOffset == 0 && localVTOff == OffsetRecord.LOWEST_OFFSET) {
      return true;
    }

    // End offset is the last successful message offset + 1.
    return localVTOff + 1 >= localVTEndOffset;
  }

  protected void startConsumingAsLeaderInTransitionFromStandby(PartitionConsumptionState partitionConsumptionState) {
    if (partitionConsumptionState.getLeaderFollowerState() != IN_TRANSITION_FROM_STANDBY_TO_LEADER) {
      throw new VeniceException(
          String.format("Expect state %s but got %s", IN_TRANSITION_FROM_STANDBY_TO_LEADER, partitionConsumptionState));
    }
    startConsumingAsLeader(partitionConsumptionState);
  }

  protected Map<String, Long> calculateLeaderUpstreamOffsetWithTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      PubSubTopic newSourceTopic,
      List<CharSequence> unreachableBrokerList) {
    /**
     * In this case, new leader should already been seeing a TopicSwitch, which has the rewind time information for
     * it to replay from realtime topic.
     */
    TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch().getTopicSwitch();
    if (topicSwitch == null) {
      throw new VeniceException(
          "New leader does not have topic switch, unable to switch to realtime leader topic: " + newSourceTopic);
    }
    final String newSourceKafkaServer = topicSwitch.sourceKafkaServers.get(0).toString();
    final PubSubTopicPartition newSourceTopicPartition =
        partitionConsumptionState.getSourceTopicPartition(newSourceTopic);

    long upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
    if (topicSwitch.rewindStartTimestamp > 0) {
      upstreamStartOffset = getTopicPartitionOffsetByKafkaURL(
          newSourceKafkaServer,
          newSourceTopicPartition,
          topicSwitch.rewindStartTimestamp);
      LOGGER.info(
          "UpstreamStartOffset: {} for source URL: {}, topic: {}, rewind timestamp: {}. Replica: {}",
          upstreamStartOffset,
          newSourceKafkaServer,
          Utils.getReplicaId(newSourceTopic, partitionConsumptionState.getPartition()),
          topicSwitch.rewindStartTimestamp,
          partitionConsumptionState.getReplicaId());
    }
    return Collections.singletonMap(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, upstreamStartOffset);
  }

  @Override
  protected void startConsumingAsLeader(PartitionConsumptionState partitionConsumptionState) {
    /**
     * When a leader replica is actually promoted to LEADER role and if native replication is enabled, there could
     * be 4 cases:
     * 1. Local fabric hasn't consumed anything from remote yet; in this case, EOP is not received, source topic
     * still exists, leader will rebuild the consumer with the proper remote Kafka bootstrap server url and start
     * consuming remotely;
     * 2. Local fabric hasn't finished VT consumption, but the host is restarted or leadership is handed over; in
     * this case, EOP is also not received, leader will resume the consumption from remote at the specific offset
     * which is checkpointed in the leader offset metadata;
     * 3. A re-balance happens, leader is bootstrapping a new replica for a version that is already online; in this
     * case, source topic might be removed already and the {@link isCurrentVersion} flag should be true; so leader
     * doesn't need to switch to remote, all the data messages have been replicated to local fabric, leader just
     * needs to consume locally. For hybrid stores in aggregate mode, after leader processes TS and existing
     * real-time data replicated to local VT, it will switch to remote RT with latest upstream offset.
     * 4. Local fabric hasn't finished RT consumption, but the host is restarted or leadership is handed over; in
     * this case, the newly selected leader will resume the consumption from RT specified in TS at the offset
     * checkpointed in leader offset metadata.
     */

    final int partition = partitionConsumptionState.getPartition();
    final OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    if (isNativeReplicationEnabled) {
      if (nativeReplicationSourceVersionTopicKafkaURL == null
          || nativeReplicationSourceVersionTopicKafkaURL.isEmpty()) {
        throw new VeniceException("Native replication is enabled but remote source address is not found");
      }
      if (shouldNewLeaderSwitchToRemoteConsumption(partitionConsumptionState)) {
        partitionConsumptionState.setConsumeRemotely(true);
        LOGGER.info(
            "Enabled remote consumption from topic-partition: {} for replica: {}",
            Utils.getReplicaId(offsetRecord.getLeaderTopic(pubSubTopicRepository), partition),
            partitionConsumptionState.getReplicaId());
      }
    }

    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    if (leaderSourceKafkaURLs.size() != 1) {
      throw new VeniceException("In L/F mode, expect only one leader source Kafka URL. Got: " + leaderSourceKafkaURLs);
    }

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    if (isDataRecovery && partitionConsumptionState.isBatchOnly() && partitionConsumptionState.consumeRemotely()) {
      // Batch-only store data recovery might consume from a previous version in remote colo.
      String dataRecoveryVersionTopic = Version.composeKafkaTopic(storeName, dataRecoverySourceVersionNumber);
      offsetRecord.setLeaderTopic(pubSubTopicRepository.getTopic(dataRecoveryVersionTopic));
    }
    final PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
    final PubSubTopicPartition leaderTopicPartition = partitionConsumptionState.getSourceTopicPartition(leaderTopic);
    long leaderStartOffset = partitionConsumptionState
        .getLeaderOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, pubSubTopicRepository);
    String leaderSourceKafkaURL = leaderSourceKafkaURLs.iterator().next();
    if (leaderStartOffset < 0 && leaderTopic.isRealTime()) {
      leaderStartOffset =
          calculateLeaderUpstreamOffsetWithTopicSwitch(partitionConsumptionState, leaderTopic, Collections.emptyList())
              .getOrDefault(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, OffsetRecord.LOWEST_OFFSET);
    }

    // subscribe to the new upstream
    LOGGER.info(
        "Replica: {} has been promoted to the leader role for the partition. It will start consuming from: {} at offset: {} with the source Kafka URL: {}. Remote consumption flag: {}",
        partitionConsumptionState.getReplicaId(),
        leaderTopicPartition,
        leaderStartOffset,
        leaderSourceKafkaURL,
        partitionConsumptionState.consumeRemotely());

    consumerSubscribe(leaderTopic, partitionConsumptionState, leaderStartOffset, leaderSourceKafkaURL);

    syncConsumedUpstreamRTOffsetMapIfNeeded(
        partitionConsumptionState,
        Collections.singletonMap(leaderSourceKafkaURL, leaderStartOffset));

    LOGGER.info(
        "The leader replica: {} started consuming from: {} at offset: {} url: {}",
        partitionConsumptionState.getReplicaId(),
        leaderTopicPartition,
        leaderStartOffset,
        leaderSourceKafkaURL);
  }

  private boolean switchAwayFromStreamReprocessingTopic(PubSubTopic currentLeaderTopic, PubSubTopic topicSwitchTopic) {
    return currentLeaderTopic.isStreamReprocessingTopic() && !topicSwitchTopic.isStreamReprocessingTopic();
  }

  protected void leaderExecuteTopicSwitch(
      PartitionConsumptionState partitionConsumptionState,
      TopicSwitch topicSwitch,
      PubSubTopic newSourceTopic) {
    if (partitionConsumptionState.getLeaderFollowerState() != LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s", LEADER, partitionConsumptionState));
    }
    if (topicSwitch.sourceKafkaServers.size() != 1) {
      throw new VeniceException(
          "In the L/F mode, expect only one source Kafka URL in Topic Switch control message. " + "But got: "
              + topicSwitch.sourceKafkaServers);
    }

    // leader switch local or remote topic, depending on the sourceKafkaServers specified in TS
    final PubSubTopic currentLeaderTopic =
        partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    final String newSourceKafkaServer = topicSwitch.sourceKafkaServers.get(0).toString();
    final PubSubTopicPartition newSourceTopicPartition =
        partitionConsumptionState.getSourceTopicPartition(newSourceTopic);

    // Only use the latestProcessedUpstreamRTOffset if we are switching to the new topic from a VT topic or if we are
    // switching to the same RT topic. The upstreamStartOffset won't be correct if we are already consuming from a RT
    // topic. e.g. If we are already consuming from RT and would like to switch to some arbitrary topic.
    boolean switchingToSameTopic = currentLeaderTopic.getName().equals(newSourceTopic.getName());
    long upstreamStartOffset = switchingToSameTopic || currentLeaderTopic.isVersionTopic()
        ? partitionConsumptionState
            .getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY)
        : OffsetRecord.LOWEST_OFFSET;
    if (upstreamStartOffset < 0 && newSourceTopic.isRealTime()) {
      upstreamStartOffset = calculateLeaderUpstreamOffsetWithTopicSwitch(
          partitionConsumptionState,
          newSourceTopic,
          Collections.emptyList())
              .getOrDefault(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, OffsetRecord.LOWEST_OFFSET);
    }

    // unsubscribe the old source and subscribe to the new source
    unsubscribeFromTopic(currentLeaderTopic, partitionConsumptionState);
    waitForLastLeaderPersistFuture(
        partitionConsumptionState,
        String.format(
            "Leader replica: %s failed to produce the last message to version topic before switching feed topic from %s to %s",
            partitionConsumptionState.getReplicaId(),
            currentLeaderTopic,
            newSourceTopic));

    // subscribe to the new upstream
    if (isNativeReplicationEnabled && !newSourceKafkaServer.equals(localKafkaServer)) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER.info(
          "Enabled remote consumption from {} for replica: {}",
          newSourceTopicPartition,
          partitionConsumptionState.getReplicaId());
    }
    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    partitionConsumptionState.getOffsetRecord()
        .setLeaderUpstreamOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, upstreamStartOffset);

    Set<String> sourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    if (sourceKafkaURLs.size() != 1) {
      throw new VeniceException("In L/F mode, expect only one leader source Kafka URL. Got: " + sourceKafkaURLs);
    }
    String sourceKafkaURL = sourceKafkaURLs.iterator().next();
    consumerSubscribe(newSourceTopic, partitionConsumptionState, upstreamStartOffset, sourceKafkaURL);

    syncConsumedUpstreamRTOffsetMapIfNeeded(
        partitionConsumptionState,
        Collections.singletonMap(sourceKafkaURL, upstreamStartOffset));

    LOGGER.info(
        "Leader replica: {} successfully switched feed topic from: {} to: {} offset: {}",
        partitionConsumptionState.getReplicaId(),
        currentLeaderTopic,
        newSourceTopic,
        upstreamStartOffset);

    // In case new topic is empty and leader can never become online
    maybeApplyReadyToServeCheck(partitionConsumptionState);
  }

  protected void syncConsumedUpstreamRTOffsetMapIfNeeded(
      PartitionConsumptionState pcs,
      Map<String, Long> upstreamStartOffsetByKafkaURL) {
    // Update in-memory consumedUpstreamRTOffsetMap in case no RT record is consumed after the subscription
    final PubSubTopic leaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    if (leaderTopic != null && leaderTopic.isRealTime()) {
      upstreamStartOffsetByKafkaURL.forEach((kafkaURL, upstreamStartOffset) -> {
        if (upstreamStartOffset > getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(pcs, kafkaURL)) {
          updateLatestInMemoryLeaderConsumedRTOffset(pcs, kafkaURL, upstreamStartOffset);
        }
      });
    }
  }

  protected void waitForLastLeaderPersistFuture(PartitionConsumptionState partitionConsumptionState, String errorMsg) {
    try {
      Future<Void> lastFuture = partitionConsumptionState.getLastLeaderPersistFuture();
      if (lastFuture != null) {
        lastFuture.get();
      }
    } catch (Exception e) {
      LOGGER.error(errorMsg, e);
      versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
      ingestionNotificationDispatcher.reportError(Collections.singletonList(partitionConsumptionState), errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  protected long getTopicPartitionOffsetByKafkaURL(
      CharSequence kafkaURL,
      PubSubTopicPartition pubSubTopicPartition,
      long rewindStartTimestamp) {
    long topicPartitionOffset =
        getTopicManager(kafkaURL.toString()).getOffsetByTime(pubSubTopicPartition, rewindStartTimestamp);
    /**
     * {@link com.linkedin.venice.pubsub.manager.TopicManager#getOffsetByTime} will always
     * return the next offset to consume, but {@link ApacheKafkaConsumer#subscribe} is always
     * seeking the next offset, so we will deduct 1 from the returned offset here.
     */
    return topicPartitionOffset - 1;
  }

  @Override
  protected Set<String> getConsumptionSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    if (partitionConsumptionState.consumeRemotely()) {
      if (partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository).isRealTime()) {
        Set<String> realTimeDataSourceKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
        if (!realTimeDataSourceKafkaURLs.isEmpty()) {
          return realTimeDataSourceKafkaURLs;
        } else {
          throw new VeniceException(
              "Expect RT Kafka URL when leader topic is a real-time topic. Got: " + partitionConsumptionState);
        }
      } else {
        return nativeReplicationSourceVersionTopicKafkaURLSingletonSet;
      }
    }
    return localKafkaServerSingletonSet;
  }

  @Override
  protected Set<String> getRealTimeDataSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    if (!isNativeReplicationEnabled) {
      return localKafkaServerSingletonSet;
    }
    TopicSwitchWrapper topicSwitchWrapper = partitionConsumptionState.getTopicSwitch();
    if (topicSwitchWrapper == null) {
      return Collections.emptySet();
    }
    return getKafkaUrlSetFromTopicSwitch(topicSwitchWrapper);
  }

  /**
   * This method gets the timestamp when consumption thread is about to process the "last" message in topic/partition;
   * note that when the function returns, new messages can be appended to the partition already, so it's not guaranteed
   * that this timestamp is from the last message.
   *
   * See {@link PartitionConsumptionState#getLatestMessageConsumedTimestampInMs} for details.
   */
  private long getLastConsumedMessageTimestamp(int partition) {
    // Consumption thread would update the last consumed message timestamp for the corresponding partition.
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    return partitionConsumptionState.getLatestMessageConsumedTimestampInMs();
  }

  protected boolean shouldNewLeaderSwitchToRemoteConsumption(PartitionConsumptionState partitionConsumptionState) {
    return isConsumingFromRemoteVersionTopic(partitionConsumptionState)
        || isLeaderConsumingRemoteRealTimeTopic(partitionConsumptionState);
  }

  private boolean isConsumingFromRemoteVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    return !partitionConsumptionState.isEndOfPushReceived() && !isCurrentVersion.getAsBoolean()
    // Do not enable remote consumption for the source fabric leader. Otherwise, it will produce extra messages.
        && !Objects.equals(nativeReplicationSourceVersionTopicKafkaURL, localKafkaServer);
  }

  private boolean isLeaderConsumingRemoteRealTimeTopic(PartitionConsumptionState partitionConsumptionState) {
    if (!partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository).isRealTime()) {
      return false; // Not consuming a RT at all
    }
    Set<String> realTimeTopicKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
    if (realTimeTopicKafkaURLs.isEmpty()) {
      throw new VeniceException("Expect at least one RT Kafka URL for " + partitionConsumptionState);
    } else if (realTimeTopicKafkaURLs.size() == 1) {
      return !Objects.equals(realTimeTopicKafkaURLs.iterator().next(), localKafkaServer);
    } else {
      return true; // If there are multiple RT Kafka URLs, it must be consuming from at least one remote RT
    }
  }

  /**
   * If leader is consuming remote VT or SR, once EOP is received, switch back to local VT to consume TOPIC_SWITCH,
   * unless there are more data to be consumed in remote topic in the following case:
   * The version is hybrid or incremental push enabled and data recovery is in progress.
   */
  private boolean shouldLeaderSwitchToLocalConsumption(PartitionConsumptionState partitionConsumptionState) {
    /**
     * If the store is not batch only and data recovery is still in progress then we cannot switch to local consumption
     * yet even when EOP is received. There might be RT data in the source fabric that we need to replicate
     */
    if (isDataRecovery && !partitionConsumptionState.isBatchOnly()) {
      if (partitionConsumptionState.isEndOfPushReceived()) {
        // Data recovery can only complete after EOP received.
        checkAndUpdateDataRecoveryStatusOfHybridStore(partitionConsumptionState);
      }
    }
    return partitionConsumptionState.consumeRemotely() && partitionConsumptionState.isEndOfPushReceived()
        && partitionConsumptionState.getOffsetRecord()
            .getLeaderTopic(pubSubTopicRepository)
            .isVersionTopicOrStreamReprocessingTopic()
        && (!isDataRecovery || partitionConsumptionState.isDataRecoveryCompleted());
  }

  private void checkAndUpdateDataRecoveryStatusOfHybridStore(PartitionConsumptionState partitionConsumptionState) {
    boolean isDataRecoveryCompleted = partitionConsumptionState.isDataRecoveryCompleted();
    if (isDataRecoveryCompleted) {
      return;
    }
    if (partitionConsumptionState.getTopicSwitch() != null) {
      // If the partition contains topic switch that mean data recovery completed at some point already.
      isDataRecoveryCompleted = true;
    }
    if (!isDataRecoveryCompleted) {
      long latestConsumedProducerTimestamp =
          partitionConsumptionState.getOffsetRecord().getLatestProducerProcessingTimeInMs();
      if (LatencyUtils
          .getElapsedTimeFromMsToMs(latestConsumedProducerTimestamp) < dataRecoveryCompletionTimeLagThresholdInMs) {
        LOGGER.info(
            "Data recovery completed for replica: {} upon consuming records with "
                + "producer timestamp of {} which is within the data recovery completion lag threshold of {} ms",
            partitionConsumptionState.getReplicaId(),
            latestConsumedProducerTimestamp,
            dataRecoveryCompletionTimeLagThresholdInMs);
        isDataRecoveryCompleted = true;
      }
    }
    if (!isDataRecoveryCompleted) {
      // If the last few records in source VT is old then we can also complete data recovery if the leader idles, and we
      // have reached/passed the end offset of the VT (around the time when ingestion is started for that partition).
      // We subtract 1 because we are skipping the remote TS message.
      long localVTOffsetProgress = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset() - 1;
      long dataRecoveryLag = measureLagWithCallToPubSub(
          nativeReplicationSourceVersionTopicKafkaURL,
          versionTopic,
          partitionConsumptionState.getPartition(),
          localVTOffsetProgress);
      boolean isAtEndOfSourceVT = dataRecoveryLag <= 0;
      long lastTimestamp = getLastConsumedMessageTimestamp(partitionConsumptionState.getPartition());
      if (isAtEndOfSourceVT && LatencyUtils.getElapsedTimeFromMsToMs(lastTimestamp) > newLeaderInactiveTime) {
        LOGGER.info(
            "Data recovery completed for replica: {} upon exceeding leader inactive time of {} ms",
            partitionConsumptionState.getReplicaId(),
            newLeaderInactiveTime);
        isDataRecoveryCompleted = true;
      }
    }

    if (isDataRecoveryCompleted) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      ingestionNotificationDispatcher.reportDataRecoveryCompleted(partitionConsumptionState);
    }
  }

  /**
   * For the corresponding partition being tracked in `partitionConsumptionState`, if it's in LEADER state and it's
   * not consuming from version topic, it should produce the new message to version topic; besides, if LEADER is
   * consuming remotely, it should also produce to local fabric.
   *
   * If buffer replay is disable, all replicas will stick to version topic, no one is going to produce any message.
   */
  @Override
  boolean shouldProduceToVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    if (!isLeader(partitionConsumptionState)) {
      return false; // Not leader
    }
    PubSubTopic leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    return (!versionTopic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely());
  }

  public static boolean isLeader(PartitionConsumptionState partitionConsumptionState) {
    return Objects.equals(partitionConsumptionState.getLeaderFollowerState(), LEADER);
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
    /**
     * Currently just check whether the sourceKafkaServers list inside TopicSwitch control message only contains
     * one Kafka server url. If native replication is enabled, kafka server url in TopicSwitch control message
     * might be different from the url in consumer.
     *
     * TODO: When we support consuming from multiple remote Kafka servers, we need to remove the single url check.
     */
    List<CharSequence> kafkaServerUrls = topicSwitch.sourceKafkaServers;
    if (kafkaServerUrls.size() != 1) {
      throw new VeniceException(
          "More than one Kafka server urls in TopicSwitch control message, " + "TopicSwitch.sourceKafkaServers: "
              + kafkaServerUrls);
    }
    ingestionNotificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState);

    String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(newSourceTopicName);

    /**
     * TopicSwitch needs to be persisted locally for both servers and DaVinci clients so that ready-to-serve check
     * can make the correct decision.
     */
    syncTopicSwitchToIngestionMetadataService(topicSwitch, partitionConsumptionState);
    /**
     * Leader shouldn't switch topic here (drainer thread), which would conflict with the ingestion thread which would
     * also access consumer.
     *
     * Besides, if there is re-balance, leader should finish consuming everything in VT before switching topics;
     * there could be more than one TopicSwitch message in VT, we should honor the last one during re-balance; so
     * don't update the consumption state like leader topic until actually switching topic. The leaderTopic field
     * should be used to track the topic that leader is actually consuming.
     */

    if (!isLeader(partitionConsumptionState)) {
      /**
       * For follower, just keep track of what leader is doing now.
       */
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
      /**
       * We need to measure offset lag after processing TopicSwitch for follower; if real-time topic is empty and never
       * gets any new message, follower replica will never become online.
       * If we measure lag here for follower, follower might become online faster than leader in extreme case:
       * Real time topic for that partition is empty or the rewind start offset is very closed to the end, followers
       * calculate the lag of the leader and decides the lag is small enough.
       */
      return isHybridAggregateMode();
    }
    return false;
  }

  protected void syncTopicSwitchToIngestionMetadataService(
      TopicSwitch topicSwitch,
      PartitionConsumptionState partitionConsumptionState) {
    storageMetadataService.computeStoreVersionState(kafkaVersionTopic, previousStoreVersionState -> {
      if (previousStoreVersionState != null) {
        if (previousStoreVersionState.topicSwitch == null) {
          LOGGER.info(
              "First time receiving a TopicSwitch message (new source topic: {}; "
                  + "rewind start time: {}) for replica: {}",
              topicSwitch.sourceTopicName,
              topicSwitch.rewindStartTimestamp,
              partitionConsumptionState.getReplicaId());
        } else {
          LOGGER.info(
              "Previous TopicSwitch message in metadata store (source topic: {}; rewind start time: {}; "
                  + "source kafka servers: {}) will be replaced by the new TopicSwitch message (new source topic: {}; "
                  + "rewind start time: {}) for replica: {}",
              previousStoreVersionState.topicSwitch.sourceTopicName,
              previousStoreVersionState.topicSwitch.rewindStartTimestamp,
              topicSwitch.sourceKafkaServers,
              topicSwitch.sourceTopicName,
              topicSwitch.rewindStartTimestamp,
              partitionConsumptionState.getReplicaId());
        }
        previousStoreVersionState.topicSwitch = topicSwitch;

        // Put TopicSwitch message into in-memory state to avoid poking metadata store
        TopicSwitch resolvedTopicSwitch = resolveSourceKafkaServersWithinTopicSwitch(topicSwitch);
        partitionConsumptionState.setTopicSwitch(
            new TopicSwitchWrapper(
                resolvedTopicSwitch,
                pubSubTopicRepository.getTopic(resolvedTopicSwitch.sourceTopicName.toString())));

        // Sync latest store version level metadata to disk
        return previousStoreVersionState;
      } else {
        throw new VeniceException(
            "Unexpected: received some " + ControlMessageType.TOPIC_SWITCH.name()
                + " control message in a topic where we have not yet received a "
                + ControlMessageType.START_OF_PUSH.name() + " control message, for partition "
                + partitionConsumptionState);
      }
    });
  }

  /**
   * A helper function to the latest in-memory offsets processed by drainers in {@link PartitionConsumptionState},
   * after processing the given {@link PubSubMessage}.
   *
   * When using this helper function to update the latest in-memory offsets processed by drainers in {@link PartitionConsumptionState}:
   * "updateVersionTopicOffsetFunction" should try to update the VT offset in {@link PartitionConsumptionState}
   * "updateRealtimeTopicOffsetFunction" should try to update the latest processed upstream offset map in {@link PartitionConsumptionState}
   *
   * In LeaderFollowerStoreIngestionTask, "sourceKafkaUrlSupplier" should always return {@link OffsetRecord#NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY};
   * in ActiveActiveStoreIngestionTask, "sourceKafkaUrlSupplier" should return the actual source Kafka url of the "consumerRecordWrapper"
   *
   * Dry-run mode would only check whether the offset rewind is benign or not instead of persisting the processed offset.
   */
  protected void updateOffsetsFromConsumerRecord(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      UpdateVersionTopicOffset updateVersionTopicOffsetFunction,
      UpdateUpstreamTopicOffset updateUpstreamTopicOffsetFunction,
      GetLastKnownUpstreamTopicOffset lastKnownUpstreamTopicOffsetSupplier,
      Supplier<String> sourceKafkaUrlSupplier,
      boolean dryRun) {

    // Only update the metadata if this replica should NOT produce to version topic.
    if (!shouldProduceToVersionTopic(partitionConsumptionState)) {
      PubSubTopic consumedTopic = consumerRecord.getTopicPartition().getPubSubTopic();
      if (consumedTopic.isRealTime()) {
        // Does this ever happen?
        LOGGER.warn(
            "Will short-circuit updateOffsetsFromConsumerRecord because the consumerRecord is coming from a "
                + "RT topic ({}), partitionConsumptionState: {}",
            consumedTopic,
            partitionConsumptionState);
        return;
      }
      /**
       * If either (1) this is a follower replica or (2) this is a leader replica who is consuming from version topic
       * in a local Kafka cluster, we can update the offset metadata in offset record right after consuming a message;
       * otherwise, if the leader is consuming from real-time topic or reprocessing topic, it should update offset
       * metadata after successfully produce a corresponding message.
       */
      KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
      updateVersionTopicOffsetFunction.apply(consumerRecord.getOffset());

      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      // DaVinci clients don't need to maintain leader production states
      if (!isDaVinciClient) {
        // also update the leader topic offset using the upstream offset in ProducerMetadata
        if (shouldUpdateUpstreamOffset(consumerRecord)) {
          final String sourceKafkaUrl = sourceKafkaUrlSupplier.get();
          final long newUpstreamOffset = kafkaValue.leaderMetadataFooter.upstreamOffset;
          PubSubTopic upstreamTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
          if (upstreamTopic == null) {
            upstreamTopic = versionTopic;
          }
          if (dryRun) {
            final long previousUpstreamOffset =
                lastKnownUpstreamTopicOffsetSupplier.apply(sourceKafkaUrl, upstreamTopic);
            checkAndHandleUpstreamOffsetRewind(
                partitionConsumptionState,
                consumerRecord,
                newUpstreamOffset,
                previousUpstreamOffset,
                this);
          } else {
            /**
             * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
             * to the true leader when the old leader doesn't stop producing.
             */
            updateUpstreamTopicOffsetFunction.apply(sourceKafkaUrl, upstreamTopic, newUpstreamOffset);
          }
        }
        if (!dryRun) {
          // update leader producer GUID
          partitionConsumptionState.setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
          if (kafkaValue.leaderMetadataFooter != null) {
            partitionConsumptionState.setLeaderHostId(kafkaValue.leaderMetadataFooter.hostName.toString());
          }
        }
      }
    } else if (!dryRun) {
      updateOffsetsAsRemoteConsumeLeader(
          partitionConsumptionState,
          leaderProducedRecordContext,
          sourceKafkaUrlSupplier.get(),
          consumerRecord,
          updateVersionTopicOffsetFunction,
          updateUpstreamTopicOffsetFunction);
    }
  }

  @Override
  protected void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState) {
    OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    offsetRecord
        .setCheckpointLocalVersionTopicOffset(partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset());
    // DaVinci clients don't need to maintain leader production states
    if (!isDaVinciClient) {
      PubSubTopic upstreamTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
      if (upstreamTopic == null) {
        upstreamTopic = versionTopic;
      }
      if (upstreamTopic.isRealTime()) {
        offsetRecord.resetUpstreamOffsetMap(partitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap());
      } else {
        offsetRecord.setCheckpointUpstreamVersionTopicOffset(
            partitionConsumptionState.getLatestProcessedUpstreamVersionTopicOffset());
      }
      offsetRecord.setLeaderGUID(partitionConsumptionState.getLeaderGUID());
      offsetRecord.setLeaderHostId(partitionConsumptionState.getLeaderHostId());
    }
  }

  private void updateOffsetsAsRemoteConsumeLeader(
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String upstreamKafkaURL,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      UpdateVersionTopicOffset updateVersionTopicOffsetFunction,
      UpdateUpstreamTopicOffset updateUpstreamTopicOffsetFunction) {
    // Leader will only update the offset from leaderProducedRecordContext in VT.
    if (leaderProducedRecordContext != null) {
      if (leaderProducedRecordContext.hasCorrespondingUpstreamMessage()) {
        updateVersionTopicOffsetFunction.apply(leaderProducedRecordContext.getProducedOffset());
        OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
        PubSubTopic upstreamTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
        if (upstreamTopic == null) {
          upstreamTopic = versionTopic;
        }
        updateUpstreamTopicOffsetFunction
            .apply(upstreamKafkaURL, upstreamTopic, leaderProducedRecordContext.getConsumedOffset());
      }
    } else {
      // Ideally this should never happen.
      String msg = "UpdateOffset: Produced record should not be null in LEADER for: "
          + consumerRecord.getTopicPartition() + ". Replica: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.warn(msg, partitionConsumptionState.getReplicaId());
      }
    }
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl,
      boolean dryRun) {
    updateOffsetsFromConsumerRecord(
        partitionConsumptionState,
        consumerRecordWrapper,
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
        () -> OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY,
        dryRun);
  }

  protected static void checkAndHandleUpstreamOffsetRewind(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      final long newUpstreamOffset,
      final long previousUpstreamOffset,
      LeaderFollowerStoreIngestionTask ingestionTask) {
    if (newUpstreamOffset >= previousUpstreamOffset) {
      return; // Rewind did not happen
    }
    if (!ingestionTask.isHybridMode()) {
      /**
       * The lossy rewind issue will only affect hybrid store since only hybrid store version topics
       * would enable log compaction, which might produce wrong compacted result because of
       * lossy rewind.
       *
       * For ordered input (batch push from Venice Push Job), the storage engine is not usable to
       * validate the data since the storage node is using {@link org.rocksdb.SstFileWriter} to ingest
       * the data and the storage engine will only ingest these generated SST files at the end of
       * the data push.
       * If there are some true data integrity issue for batch-only stores, DIV will handle this before invoking this function.
       */
      ingestionTask.getVersionedDIVStats()
          .recordBenignLeaderOffsetRewind(ingestionTask.getStoreName(), ingestionTask.getVersionNumber());
      return;
    }
    /**
     * If upstream offset is rewound and it's from a different producer, we encounter a split-brain
     * issue (multiple leaders producing to the same partition at the same time)
     *
     * Since every SN enables pass-through mode for messages before EOP, leader will re-use the same GUID as the one
     * passed from the upstream message. For reprocessing job, GUIDs of contiguous upstream messages might be different.
     * Instead of comparing GUIDs, we compare leader host names to identify the split-brain issue.
     */
    final KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    if (kafkaValue.leaderMetadataFooter != null && partitionConsumptionState.getLeaderHostId() != null
        && !kafkaValue.leaderMetadataFooter.hostName.toString().equals(partitionConsumptionState.getLeaderHostId())) {
      /**
       * Check whether the data inside rewind message is the same the data inside storage engine; if so,
       * we don't consider it as lossy rewind; otherwise, report potentially lossy upstream rewind.
       *
       * Fail the job if it's lossy and it's during the GF job (before END_OF_PUSH received);
       * otherwise, don't fail the push job, it's streaming ingestion now so it's serving online traffic already.
       */
      String logMsg = String.format(
          "Replica: %s received message at offset: %s with upstreamOffset: %d;"
              + " but recorded upstreamOffset is: %d. Received message producer GUID: %s; Recorded producer GUID: %s;"
              + " Received message producer host: %s; Recorded producer host: %s."
              + " Multiple leaders are producing. ",
          partitionConsumptionState.getReplicaId(),
          consumerRecord.getOffset(),
          newUpstreamOffset,
          previousUpstreamOffset,
          kafkaValue.producerMetadata.producerGUID == null
              ? "unknown"
              : GuidUtils.getHexFromGuid(kafkaValue.producerMetadata.producerGUID),
          partitionConsumptionState.getLeaderGUID() == null
              ? "unknown"
              : GuidUtils.getHexFromGuid(partitionConsumptionState.getLeaderGUID()),
          kafkaValue.leaderMetadataFooter.hostName.toString(),
          partitionConsumptionState.getLeaderHostId());

      boolean lossy = true;
      try {
        KafkaKey key = consumerRecord.getKey();
        KafkaMessageEnvelope envelope = consumerRecord.getValue();
        AbstractStorageEngine storageEngine = ingestionTask.getStorageEngine();
        switch (MessageType.valueOf(envelope)) {
          case PUT:
            // Issue an read to get the current value of the key
            byte[] actualValue =
                storageEngine.get(consumerRecord.getTopicPartition().getPartitionNumber(), key.getKey());
            if (actualValue != null) {
              int actualSchemaId = ByteUtils.readInt(actualValue, 0);
              Put put = (Put) envelope.payloadUnion;
              if (actualSchemaId == put.schemaId) {

                if (put.putValue.equals(
                    ByteBuffer.wrap(
                        actualValue,
                        ValueRecord.SCHEMA_HEADER_LENGTH,
                        actualValue.length - ValueRecord.SCHEMA_HEADER_LENGTH))) {
                  lossy = false;
                  logMsg += Utils.NEW_LINE_CHAR;
                  logMsg +=
                      "But this rewound PUT is not lossy because the data in the rewind message is the same as the data inside Venice";
                }
              }
            }
            break;
          case DELETE:
            /**
             * Lossy if the key/value pair is added back to the storage engine after the first DELETE message.
             */
            actualValue = storageEngine.get(consumerRecord.getTopicPartition().getPartitionNumber(), key.getKey());
            if (actualValue == null) {
              lossy = false;
              logMsg += Utils.NEW_LINE_CHAR;
              logMsg +=
                  "But this rewound DELETE is not lossy because the data in the rewind message is deleted already";
            }
            break;
          default:
            // Consider lossy for both control message and PartialUpdate
            break;
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Failed comparing the rewind message with the actual value in the database for replica: {}",
            partitionConsumptionState.getReplicaId(),
            e);
      }

      /**
       * Handles rewind scenarios as follows:
       * 1. If the rewind does not result in data loss, there is no need to fail the job.
       * 2. If the rewind results in data loss and occurs before the END_OF_PUSH is received, the job should be failed.
       * 3. If the rewind results in data loss but occurs after the END_OF_PUSH is received, the job should not be failed,
       *    as it is now in streaming ingestion mode and serving online traffic.
       */
      if (!lossy) {
        LOGGER.info(logMsg);
        ingestionTask.getVersionedDIVStats()
            .recordBenignLeaderOffsetRewind(ingestionTask.getStoreName(), ingestionTask.getVersionNumber());
        return;
      }

      if (partitionConsumptionState.isEndOfPushReceived()) {
        logMsg += Utils.NEW_LINE_CHAR;
        logMsg += "Don't fail the job after receiving EndOfPush.";
        LOGGER.error(logMsg);
        ingestionTask.getVersionedDIVStats()
            .recordPotentiallyLossyLeaderOffsetRewind(ingestionTask.getStoreName(), ingestionTask.getVersionNumber());
        return;
      }

      logMsg += Utils.NEW_LINE_CHAR;
      logMsg += "Failing the job because lossy rewind happens before receiving EndOfPush.";
      LOGGER.error(logMsg);
      ingestionTask.getVersionedDIVStats()
          .recordPotentiallyLossyLeaderOffsetRewind(ingestionTask.getStoreName(), ingestionTask.getVersionNumber());
      VeniceException e = new VeniceException(logMsg);
      ingestionTask.getIngestionNotificationDispatcher()
          .reportError(Collections.singletonList(partitionConsumptionState), logMsg, e);
      throw e;
    }
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    TopicSwitchWrapper topicSwitch = partitionConsumptionState.getTopicSwitch();
    if (topicSwitch == null) {
      return false;
    }
    if (topicSwitch.getSourceServers().size() != 1) {
      throw new VeniceException(
          "Expect only one source Kafka URLs in Topic Switch. Got: " + topicSwitch.getSourceServers());
    }
    return topicSwitch.getNewSourceTopic().isRealTime();
  }

  // TODO: Consider overriding this in A/A?
  /**
   * For Leader/Follower state model, we already keep track of the consumption progress in leader, so directly calculate
   * the lag with the real-time topic and the leader consumption offset.
   */
  @Override
  protected long measureHybridOffsetLag(PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    int partition = partitionConsumptionState.getPartition();
    OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();

    /**
     * After END_OF_PUSH received, `isReadyToServe()` is invoked for each message until the lag is caught up (otherwise,
     * if we only check ready to serve periodically, the lag may never catch up); in order not to slow down the hybrid
     * ingestion, {@link com.linkedin.venice.pubsub.manager.TopicMetadataFetcher} was introduced to get the latest
     * offset periodically and cache them; with this strategy, it is possible that partition could become 'ONLINE' at
     * most {@link com.linkedin.venice.pubsub.manager.TopicMetadataFetcher#ttlInNs} earlier.
     */
    PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
    if (leaderTopic == null || !leaderTopic.isRealTime()) {
      /**
       * 1. Usually there is a batch-push or empty push for the hybrid store before replaying messages from real-time
       *    topic; since we need to wait for at least 5 minutes of inactivity since the last successful consumed message
       *    before promoting a replica to leader, the leader topic metadata may not be initialized yet (the first time
       *    when we initialize the leader topic is either when a replica is promoted to leader successfully or encounter
       *    TopicSwitch control message.), so leader topic can be null during the 5 minutes inactivity.
       * 2. It's also possible that the replica is promoted to leader already but haven't received the TopicSwitch
       *    command from controllers to start consuming from real-time topic (for example, reprocessing Samza job has
       *    finished producing the batch input to the transient reprocessing topic, but user haven't sent END_OF_PUSH
       *    so controllers haven't sent TopicSwitch).
       */
      return Long.MAX_VALUE;
    }

    // Followers and Davinci clients, use local VT to compute hybrid lag.
    if (isDaVinciClient || partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)) {
      return measureLagWithCallToPubSub(
          localKafkaServer,
          versionTopic,
          partition,
          partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset());
    }

    // leaderTopic is the real-time topic now
    String sourceRealTimeTopicKafkaURL;
    Set<String> sourceRealTimeTopicKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
    if (sourceRealTimeTopicKafkaURLs.isEmpty()) {
      throw new VeniceException("Expect a real-time source Kafka URL for " + partitionConsumptionState);
    } else if (sourceRealTimeTopicKafkaURLs.size() == 1) {
      sourceRealTimeTopicKafkaURL = sourceRealTimeTopicKafkaURLs.iterator().next();
      return measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, partitionConsumptionState, shouldLogLag);
    } else {
      return measureRTOffsetLagForMultiRegions(sourceRealTimeTopicKafkaURLs, partitionConsumptionState, shouldLogLag);
    }
  }

  protected long measureRTOffsetLagForMultiRegions(
      Set<String> sourceRealTimeTopicKafkaURLs,
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    throw new VeniceException(
        String.format(
            "%s Multi colo RT offset lag calculation is not supported for non Active-Active stores",
            ingestionTaskName));
  }

  @Override
  public boolean isReadyToServeAnnouncedWithRTLag() {
    if (!hybridStoreConfig.isPresent() || partitionConsumptionStateMap.isEmpty()) {
      return false;
    }
    long offsetLagThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
    for (PartitionConsumptionState pcs: partitionConsumptionStateMap.values()) {
      if (pcs.hasLagCaughtUp() && offsetLagThreshold >= 0) {
        Set<String> sourceRealTimeTopicKafkaURLs = getRealTimeDataSourceKafkaAddress(pcs);
        if (sourceRealTimeTopicKafkaURLs.isEmpty()) {
          return true;
        }
        String sourceRealTimeTopicKafkaURL = sourceRealTimeTopicKafkaURLs.iterator().next();
        try {
          if (measureRTOffsetLagForSingleRegion(sourceRealTimeTopicKafkaURL, pcs, false) > offsetLagThreshold) {
            return true;
          }
        } catch (Exception e) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  protected void reportIfCatchUpVersionTopicOffset(PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();

    if (pcs.isEndOfPushReceived() && pcs.isLatchCreated() && !pcs.isLatchReleased()) {
      long lag = measureLagWithCallToPubSub(
          localKafkaServer,
          versionTopic,
          partition,
          pcs.getLatestProcessedLocalVersionTopicOffset());
      if (lag <= 0) {
        ingestionNotificationDispatcher.reportCatchUpVersionTopicOffsetLag(pcs);

        /**
         * Relax to report completion
         *
         * There is a safeguard latch that is optionally replaced during Offline to Follower
         * state transition in order to prevent "over-rebalancing". However, there is
         * still an edge case that could make Venice lose all of the Online SNs.
         *
         * 1. Helix rebalances the leader replica of a partition; old leader shuts down,
         * so no one is replicating data from RT topic to VT;
         * 2. New leader is block while transitioning to follower; after consuming the end
         * of VT, the latch releases; new leader replica quickly transits to leader role
         * from Helix's point of view; but actually it's waiting for 5 minutes before switching
         * to RT;
         * 3. After the new leader replica transition completes; Helix shutdowns the other 2 old
         * follower replicas, and bootstrap 2 new followers one by one; however, in this case,
         * even though the latches of the new followers have released; their push status is not
         * completed yet, since the new leader hasn't caught up the end of RT;
         * 4. The new leader replica is having the same issue; it hasn't caught up RT yet so its
         * push status is not COMPLETED; from router point of view, there is no online replica after
         * the rebalance.
         */
        if (isCurrentVersion.getAsBoolean()) {
          pcs.lagHasCaughtUp();
          reportCompleted(pcs, true);
        }
      }
    }
  }

  /**
   * Additional safeguards in Leader/Follower ingestion:
   * 1. Check whether the incoming messages are from the expected source topics
   */
  @Override
  protected boolean shouldPersistRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      PartitionConsumptionState partitionConsumptionState) {
    if (!super.shouldPersistRecord(record, partitionConsumptionState)) {
      return false;
    }

    PubSubTopic incomingMessageTopic = record.getTopicPartition().getPubSubTopic();
    switch (partitionConsumptionState.getLeaderFollowerState()) {
      case LEADER:
        PubSubTopic currentLeaderTopic =
            partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
        if (!Utils.resolveLeaderTopicFromPubSubTopic(pubSubTopicRepository, incomingMessageTopic)
            .equals(currentLeaderTopic)) {
          String errorMsg =
              "Leader replica: {} received a pubsub message that doesn't belong to the leader topic. Leader topic: "
                  + currentLeaderTopic + ", topic of incoming message: " + incomingMessageTopic.getName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            LOGGER.error(errorMsg, partitionConsumptionState.getReplicaId());
          }
          return false;
        }
        break;
      default:
        if (!incomingMessageTopic.equals(this.versionTopic)) {
          String errorMsg = partitionConsumptionState.getLeaderFollowerState() + " replica: "
              + partitionConsumptionState.getReplicaId()
              + " received a pubsub message that doesn't belong to version topic. Topic of incoming message: "
              + incomingMessageTopic.getName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            LOGGER.error(errorMsg);
          }
          return false;
        }
        break;
    }
    return true;
  }

  @Override
  protected void recordWriterStats(
      long consumerTimestampMs,
      long producerBrokerLatencyMs,
      long brokerConsumerLatencyMs,
      PartitionConsumptionState partitionConsumptionState) {
    if (isUserSystemStore()) {
      return;
    }
    if (isNativeReplicationEnabled) {
      // Emit latency metrics separately for leaders and followers
      boolean isLeader = partitionConsumptionState.getLeaderFollowerState().equals(LEADER);
      if (isLeader) {
        versionedIngestionStats.recordLeaderLatencies(
            storeName,
            versionNumber,
            consumerTimestampMs,
            producerBrokerLatencyMs,
            brokerConsumerLatencyMs);
      } else {
        versionedIngestionStats.recordFollowerLatencies(
            storeName,
            versionNumber,
            consumerTimestampMs,
            producerBrokerLatencyMs,
            brokerConsumerLatencyMs);
      }
    }
  }

  @Override
  protected void recordProcessedRecordStats(
      PartitionConsumptionState partitionConsumptionState,
      int processedRecordSize) {
    if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
      versionedIngestionStats.recordLeaderConsumed(storeName, versionNumber, processedRecordSize);
      hostLevelIngestionStats.recordTotalLeaderBytesConsumed(processedRecordSize);
      hostLevelIngestionStats.recordTotalLeaderRecordsConsumed();
    } else {
      versionedIngestionStats.recordFollowerConsumed(storeName, versionNumber, processedRecordSize);
      hostLevelIngestionStats.recordTotalFollowerBytesConsumed(processedRecordSize);
      hostLevelIngestionStats.recordTotalFollowerRecordsConsumed();
    }
  }

  /**
   * maxRecordSizeBytes (and the nearline variant) is a store-level config that defaults to -1.
   * The default value will be set fleet-wide using the default.max.record.size.bytes on config the server and controller.
   */
  private int backfillRecordSizeLimit(int recordSizeLimit) {
    return (recordSizeLimit > 0) ? recordSizeLimit : serverConfig.getDefaultMaxRecordSizeBytes();
  }

  protected int getMaxRecordSizeBytes() {
    return backfillRecordSizeLimit(storeRepository.getStore(storeName).getMaxRecordSizeBytes());
  }

  protected int getMaxNearlineRecordSizeBytes() {
    return backfillRecordSizeLimit(storeRepository.getStore(storeName).getMaxNearlineRecordSizeBytes());
  }

  @Override
  protected final double calculateAssembledRecordSizeRatio(long recordSize) {
    return (double) recordSize / getMaxRecordSizeBytes();
  }

  @Override
  protected final void recordAssembledRecordSizeRatio(double ratio, long currentTimeMs) {
    if (getMaxRecordSizeBytes() != VeniceWriter.UNLIMITED_MAX_RECORD_SIZE && ratio > 0) {
      hostLevelIngestionStats.recordAssembledRecordSizeRatio(ratio, currentTimeMs);
    }
  }

  @Override
  protected boolean isHybridFollower(PartitionConsumptionState partitionConsumptionState) {
    return isHybridMode() && (isDaVinciClient || partitionConsumptionState.getLeaderFollowerState().equals(STANDBY));
  }

  /**
   * For non AA hybrid stores with AGGREGATE DRP, SIT reads from parent RT while the HB is written to the child RTs.
   * Once all hybrid stores are either AA for cross colo replication and non AA otherwise, DRP and this extra
   * check can also be removed.
   */
  protected boolean shouldCheckLeaderCompleteStateInFollower() {
    return (getServerConfig().isLeaderCompleteStateCheckInFollowerEnabled() && this.hybridStoreConfig.isPresent()
        && this.hybridStoreConfig.get().getDataReplicationPolicy() != DataReplicationPolicy.AGGREGATE);
  }

  /**
   * Checks whether the lag is acceptable for hybrid stores
   * <p>
   * If the instance is a hybrid standby or DaVinciClient: Also check if <br>
   * 1. checkLeaderCompleteStateInFollower feature is enabled based on configs <br>
   * 2. leaderCompleteStatus has the leader state=completed and <br>
   * 3. the last update time was within the configured time interval to not use the stale leader state: check
   *    {@link com.linkedin.venice.ConfigKeys#SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS}
   */
  @Override
  protected boolean checkAndLogIfLagIsAcceptableForHybridStore(
      PartitionConsumptionState pcs,
      long lag,
      long threshold,
      boolean shouldLogLag,
      LagType lagType,
      long latestConsumedProducerTimestamp) {
    boolean isLagAcceptable = lag <= threshold;
    boolean isHybridFollower = isHybridFollower(pcs);

    // if lag is acceptable and is a hybrid standby or DaVinciClient: check and
    // override it based on leader follower state
    if (isLagAcceptable && isHybridFollower && shouldCheckLeaderCompleteStateInFollower()) {
      isLagAcceptable = pcs.isLeaderCompleted()
          && ((System.currentTimeMillis() - pcs.getLastLeaderCompleteStateUpdateInMs()) <= getServerConfig()
              .getLeaderCompleteStateCheckInFollowerValidIntervalMs());
    }

    if (shouldLogLag) {
      StringBuilder leaderCompleteHeaderDetails = new StringBuilder();
      if (isHybridFollower) {
        leaderCompleteHeaderDetails.append("Leader Complete State: {")
            .append(pcs.getLeaderCompleteState().toString())
            .append("}, Last update In Ms: {")
            .append(pcs.getLastLeaderCompleteStateUpdateInMs())
            .append("}.");
      }
      LOGGER.info(
          "[{} lag] replica: {} is {}. {}Lag: [{}] {} Threshold [{}]. {}",
          lagType.prettyString(),
          pcs.getReplicaId(),
          (isLagAcceptable ? "not lagging" : "lagging"),
          (lagType == OFFSET_LAG ? "" : "The latest producer timestamp is " + latestConsumedProducerTimestamp + ". "),
          lag,
          lag <= threshold ? "<=" : ">",
          threshold,
          leaderCompleteHeaderDetails.toString());
    }

    return isLagAcceptable;
  }

  @Override
  protected void recordHeartbeatReceived(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String kafkaUrl) {
    if (heartbeatMonitoringService == null) {
      // Not enabled!
      return;
    }

    // Separate incremental push pubsub entries has the same pubsub url but different cluster id, which creates
    // confusion for heartbeat tracking. We need to resolve the kafka url to the actual kafka cluster url.
    String resolvedKafkaUrl = kafkaClusterUrlResolver != null ? kafkaClusterUrlResolver.apply(kafkaUrl) : kafkaUrl;
    // This is just sanity check, as there is no leader producing HB to sep topic by design.
    if (!Objects.equals(resolvedKafkaUrl, kafkaUrl)) {
      return;
    }
    if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
      heartbeatMonitoringService.recordLeaderHeartbeat(
          storeName,
          versionNumber,
          partitionConsumptionState.getPartition(),
          serverConfig.getKafkaClusterUrlToAliasMap().get(resolvedKafkaUrl),
          consumerRecord.getValue().producerMetadata.messageTimestamp,
          partitionConsumptionState.isComplete());
    } else {
      heartbeatMonitoringService.recordFollowerHeartbeat(
          storeName,
          versionNumber,
          partitionConsumptionState.getPartition(),
          serverConfig.getKafkaClusterUrlToAliasMap().get(resolvedKafkaUrl),
          consumerRecord.getValue().producerMetadata.messageTimestamp,
          partitionConsumptionState.isComplete());
    }
  }

  /**
   * Besides draining messages in the drainer queue, wait for the last producer future.
   */
  @Override
  protected void waitForAllMessageToBeProcessedFromTopicPartition(
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    super.waitForAllMessageToBeProcessedFromTopicPartition(topicPartition, partitionConsumptionState);

    /**
     * In case of L/F model in Leader we first produce to local kafka then queue to drainer from kafka callback thread.
     * The above waiting is not sufficient enough since it only waits for whatever was queue prior to calling the
     * above api. This alone would not guarantee that all messages from that topic partition
     * has been processed completely. Additionally we need to wait for the last leader producer future to complete.
     *
     * Practically the above is not needed for Leader at all if we are waiting for the future below. But waiting does not
     * cause any harm and also keep this function simple. Otherwise we might have to check if this is the Leader for the partition.
     *
     * The code should only be effective in L/F model Leader instances as lastFuture should be null in all other scenarios.
     */
    if (partitionConsumptionState != null) {
      /**
       * The following logic will make sure all the records queued in the buffer queue will be processed completely.
       */
      try {
        CompletableFuture<Void> lastQueuedRecordPersistedFuture =
            partitionConsumptionState.getLastQueuedRecordPersistedFuture();
        if (lastQueuedRecordPersistedFuture != null) {
          lastQueuedRecordPersistedFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
        }
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Got interrupted while waiting for the last queued record to be persisted from: {} for replica: {}"
                + "Will throw the interrupt exception.",
            topicPartition,
            partitionConsumptionState.getReplicaId(),
            e);
        throw e;
      } catch (Exception e) {
        LOGGER.error(
            "Got exception while waiting for the last queued record to be persisted from: {} for replica: {}. Will swallow.",
            topicPartition,
            partitionConsumptionState.getReplicaId(),
            e);
      }
      Future<Void> lastFuture = null;
      try {
        lastFuture = partitionConsumptionState.getLastLeaderPersistFuture();
        if (lastFuture != null) {
          long synchronizeStartTimeInNS = System.nanoTime();
          lastFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
          hostLevelIngestionStats
              .recordLeaderProducerSynchronizeLatency(LatencyUtils.getElapsedTimeFromNSToMS(synchronizeStartTimeInNS));
        }
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Got interrupted while waiting for the last leader producer future of record from: {} for replica: {}. "
                + "No data loss. Will throw the interrupt exception.",
            topicPartition,
            partitionConsumptionState.getReplicaId(),
            e);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
        throw e;
      } catch (TimeoutException e) {
        LOGGER.error(
            "Timeout while waiting for the last leader producer future of record from topic-partition: {} for replica: {}. No data loss. Will swallow.",
            topicPartition,
            partitionConsumptionState.getReplicaId(),
            e);
        lastFuture.cancel(true);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      } catch (Exception e) {
        LOGGER.error(
            "Got exception while waiting for the latest producer future to be completed. Will swallow. Topic-partition: {} Replica: {}",
            topicPartition,
            e);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        // No need to fail the push job; just record the failure.
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      }
    }
  }

  // calculate the replication once per partition, checking Leader instance will make sure we calculate it just once
  // per partition.
  private static final Predicate<? super PartitionConsumptionState> BATCH_REPLICATION_LAG_FILTER =
      pcs -> !pcs.isEndOfPushReceived() && pcs.consumeRemotely() && pcs.getLeaderFollowerState().equals(LEADER);

  @Override
  public long getBatchReplicationLag() {
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

    long replicationLag =
        partitionConsumptionStateMap.values().stream().filter(BATCH_REPLICATION_LAG_FILTER).mapToLong((pcs) -> {
          PubSubTopic currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null) {
            currentLeaderTopic = versionTopic;
          }

          String sourceKafkaURL = getSourceKafkaUrlForOffsetLagMeasurement(pcs);
          // Consumer might not exist after the consumption state is created, but before attaching the corresponding
          // consumer.
          long lagBasedOnMetrics =
              getPartitionOffsetLagBasedOnMetrics(sourceKafkaURL, currentLeaderTopic, pcs.getPartition());
          if (lagBasedOnMetrics >= 0) {
            return lagBasedOnMetrics;
          }
          // Fall back to use the old way (latest VT offset in remote kafka - latest VT offset in local kafka)
          long localOffset =
              getTopicManager(localKafkaServer).getLatestOffsetCached(currentLeaderTopic, pcs.getPartition()) - 1;
          return measureLagWithCallToPubSub(
              nativeReplicationSourceVersionTopicKafkaURL,
              currentLeaderTopic,
              pcs.getPartition(),
              localOffset);
        }).filter(VALID_LAG).sum();
    return minZeroLag(replicationLag);
  }

  protected static final LongPredicate VALID_LAG = value -> value < Long.MAX_VALUE;
  public static final Predicate<? super PartitionConsumptionState> LEADER_OFFSET_LAG_FILTER =
      pcs -> pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> BATCH_LEADER_OFFSET_LAG_FILTER =
      pcs -> !pcs.isEndOfPushReceived() && pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> HYBRID_LEADER_OFFSET_LAG_FILTER =
      pcs -> pcs.isEndOfPushReceived() && pcs.isHybrid() && pcs.getLeaderFollowerState().equals(LEADER);

  /** used for metric purposes **/
  private long getLeaderOffsetLag(Predicate<? super PartitionConsumptionState> partitionConsumptionStateFilter) {

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

    long offsetLag = partitionConsumptionStateMap.values()
        .stream()
        .filter(partitionConsumptionStateFilter)
        // the lag is (latest VT offset - consumed VT offset)
        .mapToLong((pcs) -> {
          PubSubTopic currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null) {
            currentLeaderTopic = versionTopic;
          }
          final String kafkaSourceAddress = getSourceKafkaUrlForOffsetLagMeasurement(pcs);
          // Consumer might not exist after the consumption state is created, but before attaching the corresponding
          // consumer.
          long lagBasedOnMetrics =
              getPartitionOffsetLagBasedOnMetrics(kafkaSourceAddress, currentLeaderTopic, pcs.getPartition());
          if (lagBasedOnMetrics >= 0) {
            return lagBasedOnMetrics;
          }

          // Fall back to calculate offset lag in the original approach
          if (currentLeaderTopic.isRealTime()) {
            return this.measureHybridOffsetLag(pcs, false);
          } else {
            return measureLagWithCallToPubSub(
                kafkaSourceAddress,
                currentLeaderTopic,
                pcs.getPartition(),
                pcs.getLatestProcessedLocalVersionTopicOffset());
          }
        })
        .filter(VALID_LAG)
        .sum();

    return minZeroLag(offsetLag);
  }

  @Override
  public long getLeaderOffsetLag() {
    return getLeaderOffsetLag(LEADER_OFFSET_LAG_FILTER);
  }

  @Override
  public long getBatchLeaderOffsetLag() {
    return getLeaderOffsetLag(BATCH_LEADER_OFFSET_LAG_FILTER);
  }

  @Override
  public long getHybridLeaderOffsetLag() {
    return getLeaderOffsetLag(HYBRID_LEADER_OFFSET_LAG_FILTER);
  }

  private final Predicate<? super PartitionConsumptionState> FOLLOWER_OFFSET_LAG_FILTER =
      pcs -> pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1
          && !pcs.getLeaderFollowerState().equals(LEADER);
  private final Predicate<? super PartitionConsumptionState> BATCH_FOLLOWER_OFFSET_LAG_FILTER =
      pcs -> !pcs.isEndOfPushReceived()
          && pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1
          && !pcs.getLeaderFollowerState().equals(LEADER);
  private final Predicate<? super PartitionConsumptionState> HYBRID_FOLLOWER_OFFSET_LAG_FILTER =
      pcs -> pcs.isEndOfPushReceived() && pcs.isHybrid()
          && pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1
          && !pcs.getLeaderFollowerState().equals(LEADER);

  private long getFollowerOffsetLag(Predicate<? super PartitionConsumptionState> partitionConsumptionStateFilter) {
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

    long offsetLag = partitionConsumptionStateMap.values()
        .stream()
        // only calculate followers who have received EOP since before that, both leaders and followers
        // consume from VT
        .filter(partitionConsumptionStateFilter)
        // the lag is (latest VT offset - consumed VT offset)
        .mapToLong((pcs) -> {
          // Consumer might not exist after the consumption state is created, but before attaching the corresponding
          // consumer.
          long lagBasedOnMetrics =
              getPartitionOffsetLagBasedOnMetrics(localKafkaServer, versionTopic, pcs.getPartition());
          if (lagBasedOnMetrics >= 0) {
            return lagBasedOnMetrics;
          }
          // Fall back to calculate offset lag in the old way
          return measureLagWithCallToPubSub(
              localKafkaServer,
              versionTopic,
              pcs.getPartition(),
              pcs.getLatestProcessedLocalVersionTopicOffset());
        })
        .filter(VALID_LAG)
        .sum();

    return minZeroLag(offsetLag);
  }

  private String getSourceKafkaUrlForOffsetLagMeasurement(PartitionConsumptionState pcs) {
    Set<String> sourceKafkaURLs = getConsumptionSourceKafkaAddress(pcs);
    String sourceKafkaURL;
    if (sourceKafkaURLs.size() == 1) {
      sourceKafkaURL = sourceKafkaURLs.iterator().next();
    } else {
      if (sourceKafkaURLs.contains(localKafkaServer)) {
        sourceKafkaURL = localKafkaServer;
      } else {
        throw new VeniceException(
            String.format(
                "Expect source Kafka URLs contains local Kafka URL. Got local "
                    + "Kafka URL %s and source Kafka URLs %s",
                localKafkaServer,
                sourceKafkaURLs));
      }
    }
    return sourceKafkaURL;
  }

  /**
   * For L/F or NR, there is only one entry in upstreamOffsetMap whose key is NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY.
   * Return the value of the entry.
   */
  protected long getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String ignoredUpstreamKafkaUrl) {
    return pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  /**
   * For regular L/F stores without A/A enabled, there is always only one real-time source.
   */
  protected long getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(
      PartitionConsumptionState pcs,
      String ignoredKafkaUrl) {
    return pcs.getLeaderConsumedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  @Override
  void updateLatestInMemoryLeaderConsumedRTOffset(PartitionConsumptionState pcs, String ignoredKafkaUrl, long offset) {
    pcs.updateLeaderConsumedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, offset);
  }

  @Override
  public long getFollowerOffsetLag() {
    return getFollowerOffsetLag(FOLLOWER_OFFSET_LAG_FILTER);
  }

  @Override
  public long getBatchFollowerOffsetLag() {
    return getFollowerOffsetLag(BATCH_FOLLOWER_OFFSET_LAG_FILTER);
  }

  @Override
  public long getHybridFollowerOffsetLag() {
    return getFollowerOffsetLag(HYBRID_FOLLOWER_OFFSET_LAG_FILTER);
  }

  @Override
  public long getRegionHybridOffsetLag(int regionId) {
    return StatsErrorCode.ACTIVE_ACTIVE_NOT_ENABLED.code;
  }

  /**
   * Unsubscribe from all the topics being consumed for the partition in partitionConsumptionState
   */
  @Override
  public void consumerUnSubscribeAllTopics(PartitionConsumptionState partitionConsumptionState) {
    PubSubTopic leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    int partitionId = partitionConsumptionState.getPartition();
    if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER) && leaderTopic != null) {
      aggKafkaConsumerService
          .unsubscribeConsumerFor(versionTopic, new PubSubTopicPartitionImpl(leaderTopic, partitionId));
    } else {
      aggKafkaConsumerService
          .unsubscribeConsumerFor(versionTopic, new PubSubTopicPartitionImpl(versionTopic, partitionId));
    }

    /**
     * Leader of the user partition should close all subPartitions it is producing to.
     */
    if (partitionConsumptionState.getVeniceWriterLazyRef() != null) {
      partitionConsumptionState.getVeniceWriterLazyRef().ifPresent(vw -> vw.closePartition(partitionId));
    }
  }

  @Override
  public int getWriteComputeErrorCode() {
    return writeComputeFailureCode;
  }

  @Override
  public void updateLeaderTopicOnFollower(PartitionConsumptionState partitionConsumptionState) {
    if (isLeader(partitionConsumptionState)) {
      return;
    }
    OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
    if (isDataRecovery && partitionConsumptionState.isBatchOnly() && !versionTopic.equals(leaderTopic)) {
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(versionTopic);
    }
    /**
     * When the node works as a leader, it does not update leader topic when processing TS. When the node demotes to
     * follower after leadership handover or becomes follower after restart, it should track the topic that leader will
     * consume. Otherwise, for hybrid stores: 1. If the node remains as follower, it might never become online because
     * hybrid lag measurement will return a large value for VT. 2. If the node promotes to leader, it will subscribe to
     * VT at RT offset.
     */
    TopicSwitchWrapper topicSwitch = partitionConsumptionState.getTopicSwitch();
    if (topicSwitch != null) {
      if (!topicSwitch.getNewSourceTopic().equals(leaderTopic)) {
        offsetRecord.setLeaderTopic(topicSwitch.getNewSourceTopic());
      }
    }
  }

  /**
   * Clone DIV check results from OffsetRecord to the DIV validator that is used for leader consumption thread.
   *
   * This step is necessary in case leadership handovers happen during batch push; if this node is used to be a
   * follower replica for a partition and it's promoted to leader in the middle of a batch push, leader DIV validator
   * should inherit/clone the latest producer states from the DIV validator in drainer; otherwise, leader will encounter
   * MISSING message DIV error immediately.
   */
  private void restoreProducerStatesForLeaderConsumption(int partition) {
    kafkaDataIntegrityValidatorForLeaders.clearPartition(partition);
    cloneProducerStates(partition, kafkaDataIntegrityValidatorForLeaders);
  }

  /**
   * A function to update version topic offset.
   */
  @FunctionalInterface
  interface UpdateVersionTopicOffset {
    void apply(long versionTopicOffset);
  }

  /**
   * A function to update realtime topic offset.
   */
  @FunctionalInterface
  interface UpdateUpstreamTopicOffset {
    void apply(String sourceKafkaUrl, PubSubTopic upstreamTopic, long upstreamTopicOffset);
  }

  /**
   * A function to get last known realtime topic offset.
   */
  @FunctionalInterface
  interface GetLastKnownUpstreamTopicOffset {
    long apply(String sourceKafkaUrl, PubSubTopic upstreamTopic);
  }

  /**
   * This method fetches/calculates latest leader persisted offset and last offset in RT topic. The method relies on
   * {@link #getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(PartitionConsumptionState, String)} to fetch
   * latest leader persisted offset for different data replication policy.
   * @return the lag (lastOffsetInRealTimeTopic - latestPersistedLeaderOffset)
   */
  protected long measureRTOffsetLagForSingleRegion(
      String sourceRealTimeTopicKafkaURL,
      PartitionConsumptionState pcs,
      boolean shouldLog) {
    int partition = pcs.getPartition();
    long latestLeaderOffset;
    latestLeaderOffset =
        getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(pcs, sourceRealTimeTopicKafkaURL);
    PubSubTopic leaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    long lastOffsetInRealTimeTopic = getTopicPartitionEndOffSet(
        sourceRealTimeTopicKafkaURL,
        resolveTopicWithKafkaURL(leaderTopic, sourceRealTimeTopicKafkaURL),
        partition);

    if (lastOffsetInRealTimeTopic < 0) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException("Got a negative lastOffsetInRealTimeTopic")) {
        LOGGER.warn(
            "Unexpected! Got a negative lastOffsetInRealTimeTopic ({})! Will return Long.MAX_VALUE ({}) as the lag for replica: {}.",
            lastOffsetInRealTimeTopic,
            Long.MAX_VALUE,
            pcs.getReplicaId(),
            new VeniceException("Exception not thrown, just for logging purposes."));
      }
      return Long.MAX_VALUE;
    }

    if (latestLeaderOffset == -1) {
      // If leader hasn't consumed anything yet we should use the value of 0 to calculate the exact offset lag.
      latestLeaderOffset = 0;
    }
    long lag = lastOffsetInRealTimeTopic - latestLeaderOffset;
    if (shouldLog) {
      LOGGER.info(
          "Replica: {} RT lag offset for {} is: Latest RT offset [{}] - persisted offset [{}] = Lag [{}]",
          pcs.getReplicaId(),
          sourceRealTimeTopicKafkaURL,
          lastOffsetInRealTimeTopic,
          latestLeaderOffset,
          lag);
    }
    return lag;
  }

  @Override
  protected void processControlMessageForViews(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {

    // Iterate through list of views for the store and process the control message.
    for (VeniceViewWriter viewWriter: viewWriters.values()) {
      viewWriter
          .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, partition, partitionConsumptionState);
    }
  }

  Lazy<VeniceWriter<byte[], byte[], byte[]>> getVeniceWriter(PartitionConsumptionState partitionConsumptionState) {
    return partitionConsumptionState.getVeniceWriterLazyRef();
  }

  void setRealTimeVeniceWriterRef(PartitionConsumptionState partitionConsumptionState) {
    partitionConsumptionState.setVeniceWriterLazyRef(veniceWriterForRealTime);
  }

  // test method
  protected void addPartitionConsumptionState(Integer partition, PartitionConsumptionState pcs) {
    partitionConsumptionStateMap.put(partition, pcs);
  }

  /**
   * Log ingestion heartbeat propagated from upstream topic to version topic
   */
  private void logIngestionHeartbeat(PubSubTopicPartition topicPartition, Exception exception) {
    String topicPartitionName = topicPartition.getPubSubTopic().getName();
    int partitionId = topicPartition.getPartitionNumber();

    String logMessage = String.format(
        "Forward ingestion heartbeat from upstream topic to version topic-partition: %s %s.",
        Utils.getReplicaId(topicPartitionName, partitionId),
        exception != null ? "failed" : "succeeded");

    // using a redundant filter in case if the configured duration to send heartbeat
    // is too frequent for logging purposes
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
      if (exception != null) {
        LOGGER.error(logMessage, exception);
      } else {
        LOGGER.debug(logMessage);
      }
    }
  }

  CompletableFuture<PubSubProduceResult> sendIngestionHeartbeatToRT(PubSubTopicPartition topicPartition) {
    return sendIngestionHeartbeat(
        partitionConsumptionStateMap.get(topicPartition.getPartitionNumber()),
        topicPartition,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        false, // maybeSendIngestionHeartbeat logs for this case
        false,
        LeaderCompleteState.LEADER_NOT_COMPLETED,
        System.currentTimeMillis());
  }

  @Override
  CompletableFuture<PubSubProduceResult> sendIngestionHeartbeat(
      PartitionConsumptionState partitionConsumptionState,
      PubSubTopicPartition topicPartition,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      boolean shouldLog,
      boolean addLeaderCompleteState,
      LeaderCompleteState leaderCompleteState,
      long originTimeStampMs) {
    CompletableFuture<PubSubProduceResult> heartBeatFuture;
    try {
      heartBeatFuture = partitionConsumptionState.getVeniceWriterLazyRef()
          .get()
          .sendHeartbeat(
              topicPartition,
              callback,
              leaderMetadataWrapper,
              addLeaderCompleteState,
              leaderCompleteState,
              originTimeStampMs);
      if (shouldLog) {
        heartBeatFuture
            .whenComplete((ignore, throwable) -> logIngestionHeartbeat(topicPartition, (Exception) throwable));
      }
    } catch (Exception e) {
      heartBeatFuture = new CompletableFuture<>();
      heartBeatFuture.completeExceptionally(e);
      if (shouldLog) {
        logIngestionHeartbeat(topicPartition, e);
      }
    }
    return heartBeatFuture;
  }

  /**
   * For hybrid stores only, the leader periodically writes a special SOS message to the RT topic with the following properties: <br>
   * 1. Special key: This key contains constant bytes, allowing for compaction. <br>
   * 2. Fixed/known producer GUID: This GUID is dedicated to heartbeats and prevents DIV from breaking. <br>
   * 3. Special segment: This segment never contains data, eliminating the need for an EOS message. <br>
   * <p>
   * Upon consuming the SOS message, the leader writes it to its local VT. Once the drainer processes the record, the leader updates
   * its latest processed upstream RT topic offset. At this point, the offset reflects the correct position, regardless of trailing
   * CMs or skippable data records due to DCR.
   * <p>
   * This heartbeat message does not send a leader completion header. This results in having the leader completion states only in VTs and not
   * in the RT, avoiding the need to differentiate between heartbeats from leaders of different versions (backup/current/future) and colos.
   *
   * @return the set of partitions that failed to send heartbeat (used for tests)
   */
  @Override
  protected Set<String> maybeSendIngestionHeartbeat() {
    if (!isHybridMode() || isDaVinciClient) {
      // Skip if the store version is not hybrid or if this is a DaVinci client.
      return null;
    }
    long currentTimestamp = System.currentTimeMillis();
    if (lastSendIngestionHeartbeatTimestamp.get() + serverConfig.getIngestionHeartbeatIntervalMs() > currentTimestamp) {
      // Not time for another heartbeat yet.
      return null;
    }

    AtomicInteger numHeartBeatSuccess = new AtomicInteger(0);
    Map<Integer, CompletableFuture<PubSubProduceResult>> heartBeatFutures = new VeniceConcurrentHashMap<>();
    Set<String> failedPartitions = VeniceConcurrentHashMap.newKeySet();
    AtomicReference<CompletionException> completionException = new AtomicReference<>(null);
    for (PartitionConsumptionState pcs: partitionConsumptionStateMap.values()) {
      PubSubTopic leaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
      if (isLeader(pcs) && leaderTopic != null && leaderTopic.isRealTime()) {
        // Only leader replica consuming from RT topic may send sync offset control message.
        if (pcs.getTopicSwitch() != null
            && !leaderTopic.getName().equals(pcs.getTopicSwitch().getNewSourceTopic().getName())) {
          // Do not send heartbeat if we detected a topic switch is happening since TS requires the leader to be idle.
          continue;
        }
        int partition = pcs.getPartition();
        CompletableFuture<PubSubProduceResult> heartBeatFuture =
            sendIngestionHeartbeatToRT(new PubSubTopicPartitionImpl(leaderTopic, partition));
        heartBeatFuture.whenComplete((ignore, throwable) -> {
          if (throwable != null) {
            completionException.set(new CompletionException(throwable));
            failedPartitions.add(String.valueOf(partition));
          } else {
            numHeartBeatSuccess.getAndIncrement();
          }
        });
        heartBeatFutures.put(partition, heartBeatFuture);
      }
    }

    if (!heartBeatFutures.isEmpty()) {
      CompletableFuture.allOf(heartBeatFutures.values().toArray(new CompletableFuture[0]))
          .whenCompleteAsync((ignore, throwable) -> {
            if (!failedPartitions.isEmpty()) {
              int numFailedPartitions = failedPartitions.size();
              String logMessage = String.format(
                  "Send ingestion heartbeat for %d partitions of topic %s: %d succeeded, %d failed for partitions: %s",
                  heartBeatFutures.size(),
                  realTimeTopic,
                  numHeartBeatSuccess.get(),
                  numFailedPartitions,
                  String.join(",", failedPartitions));
              if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
                LOGGER.error(logMessage, completionException.get());
              }
            } else {
              String logMessage = String.format(
                  "Send ingestion heartbeat for %d partitions of topic %s: all succeeded",
                  heartBeatFutures.size(),
                  realTimeTopic);
              if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
                LOGGER.debug(logMessage);
              }
            }
          });
    }

    lastSendIngestionHeartbeatTimestamp.set(currentTimestamp);
    return failedPartitions;
  }

  /**
   *  Resubscribe operation by passing new version role and partition role to {@link AggKafkaConsumerService}. The action
   *  for leader and follower replica will be handled differently.
   */
  @Override
  protected void resubscribe(PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    if (isLeader(partitionConsumptionState)) {
      resubscribeAsLeader(partitionConsumptionState);
    } else {
      resubscribeAsFollower(partitionConsumptionState);
    }
  }

  protected void resubscribeAsFollower(PartitionConsumptionState partitionConsumptionState)
      throws InterruptedException {
    int partition = partitionConsumptionState.getPartition();
    unsubscribeFromTopic(versionTopic, partitionConsumptionState);
    waitForAllMessageToBeProcessedFromTopicPartition(
        new PubSubTopicPartitionImpl(versionTopic, partition),
        partitionConsumptionState);
    LOGGER.info(
        "Follower replica: {} unsubscribe finished for future resubscribe.",
        partitionConsumptionState.getReplicaId());
    long latestProcessedLocalVersionTopicOffset = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
    consumerSubscribe(
        versionTopic,
        partitionConsumptionState,
        latestProcessedLocalVersionTopicOffset,
        localKafkaServer);
    LOGGER.info(
        "Follower replica: {} resubscribe to offset: {}",
        partitionConsumptionState.getReplicaId(),
        latestProcessedLocalVersionTopicOffset);
  }

  protected void resubscribeAsLeader(PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
    PubSubTopic leaderTopic = offsetRecord.getLeaderTopic(pubSubTopicRepository);
    int partition = partitionConsumptionState.getPartition();
    unsubscribeFromTopic(leaderTopic, partitionConsumptionState);
    waitForAllMessageToBeProcessedFromTopicPartition(
        new PubSubTopicPartitionImpl(leaderTopic, partition),
        partitionConsumptionState);
    LOGGER.info(
        "Leader replica: {} unsubscribe finished for future resubscribe.",
        partitionConsumptionState.getReplicaId());
    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    for (String leaderSourceKafkaURL: leaderSourceKafkaURLs) {
      long leaderStartOffset = partitionConsumptionState.getLeaderOffset(leaderSourceKafkaURL, pubSubTopicRepository);
      consumerSubscribe(leaderTopic, partitionConsumptionState, leaderStartOffset, leaderSourceKafkaURL);
      LOGGER.info(
          "Leader replica: {} resubscribe to offset: {}",
          partitionConsumptionState.getReplicaId(),
          leaderStartOffset);
    }
  }

  /**
   * Once leader is marked completed, immediately reset {@link #lastSendIngestionHeartbeatTimestamp}
   * such that {@link #maybeSendIngestionHeartbeat()} will send HB SOS to the respective RT topics
   * rather than waiting for the timer to send HB SOS.
   */
  @Override
  void reportCompleted(PartitionConsumptionState partitionConsumptionState, boolean forceCompletion) {
    super.reportCompleted(partitionConsumptionState, forceCompletion);
    if (isHybridMode() && partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
      // reset lastSendIngestionHeartbeatTimestamp to force sending HB SOS to the respective RT topics.
      lastSendIngestionHeartbeatTimestamp.set(0);
    }
  }

  Set<String> getKafkaUrlSetFromTopicSwitch(TopicSwitchWrapper topicSwitchWrapper) {
    if (isSeparatedRealtimeTopicEnabled()) {
      Set<String> result = new HashSet<>();
      for (String server: topicSwitchWrapper.getSourceServers()) {
        result.add(server);
        result.add(server + Utils.SEPARATE_TOPIC_SUFFIX);
      }
      return result;
    }
    return topicSwitchWrapper.getSourceServers();
  }

  protected boolean hasViewWriters() {
    return viewWriters != null && !viewWriters.isEmpty();
  }

  @Override
  KafkaDataIntegrityValidator getKafkaDataIntegrityValidatorForLeaders() {
    return kafkaDataIntegrityValidatorForLeaders;
  }

  @Override
  byte[] applyWriteCompute(
      GenericRecord currValue,
      int writerValueSchemaId,
      int readerValueSchemaId,
      ByteBuffer writeComputeBytes,
      int writerUpdateProtocolVersion,
      int readerUpdateProtocolVersion) {
    return storeWriteComputeHandler.applyWriteCompute(
        currValue,
        writerValueSchemaId,
        readerValueSchemaId,
        writeComputeBytes,
        writerUpdateProtocolVersion,
        readerUpdateProtocolVersion);
  }

  @Override
  AvroStoreDeserializerCache getStoreDeserializerCache() {
    return storeDeserializerCache;
  }

  @Override
  String getKafkaUrl(int kafkaClusterId) {
    return kafkaClusterIdToUrlMap.get(kafkaClusterId);
  }

  @Override
  boolean hasChangeCaptureView() {
    return hasChangeCaptureView;
  }
}
