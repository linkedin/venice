package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.LEADER_TO_STANDBY;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.END_OF_PUSH;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.ChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericRecord;
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
   */
  protected final Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriter;
  protected final Int2ObjectMap<String> kafkaClusterIdToUrlMap;
  private long dataRecoveryCompletionTimeLagThresholdInMs = 0;

  protected final Map<String, VeniceViewWriter> viewWriters;

  protected final AvroStoreDeserializerCache storeDeserializerCache;

  public LeaderFollowerStoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend) {
    super(
        builder,
        store,
        version,
        kafkaConsumerProperties,
        isCurrentVersion,
        storeConfig,
        errorPartitionId,
        isIsolatedIngestion,
        cacheBackend,
        builder.getLeaderFollowerNotifiers());
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
     */
    if (version.getDataRecoveryVersionConfig() == null) {
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
        dataRecoveryCompletionTimeLagThresholdInMs = TopicManager.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN / 2;
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
    VeniceWriterOptions writerOptions =
        new VeniceWriterOptions.Builder(getVersionTopic().getName()).setPartitioner(venicePartitioner)
            .setChunkingEnabled(isChunked)
            .setRmdChunkingEnabled(version.isRmdChunkingEnabled())
            .setPartitionCount(storeVersionPartitionCount * amplificationFactor)
            .build();
    this.veniceWriter = Lazy.of(() -> veniceWriterFactory.createVeniceWriter(writerOptions));
    this.kafkaClusterIdToUrlMap = serverConfig.getKafkaClusterIdToUrlMap();
    this.kafkaDataIntegrityValidatorForLeaders = new KafkaDataIntegrityValidator(kafkaVersionTopic);
    if (builder.getVeniceViewWriterFactory() != null && !store.getViewConfigs().isEmpty()) {
      viewWriters = builder.getVeniceViewWriterFactory()
          .buildStoreViewWriters(
              store,
              version.getNumber(),
              schemaRepository.getKeySchema(store.getName()).getSchema());
    } else {
      viewWriters = Collections.emptyMap();
    }
    this.storeDeserializerCache = new AvroStoreDeserializerCache(
        builder.getSchemaRepo(),
        getStoreName(),
        serverConfig.isComputeFastAvroEnabled());
  }

  @Override
  protected void closeVeniceWriters(boolean doFlush) {
    if (veniceWriter.isPresent()) {
      veniceWriter.get().close(doFlush);
    }
  }

  @Override
  protected void closeVeniceViewWriters() {
    if (!viewWriters.isEmpty()) {
      viewWriters.forEach((k, v) -> v.close());
    }
  }

  /**
   * Close a DIV segment for a version topic partition.
   */
  private void endSegment(int partition) {
    // If the VeniceWriter doesn't exist, then no need to end any segment, and this function becomes a no-op
    veniceWriter.ifPresent(vw -> vw.endSegment(partition, true));
  }

  @Override
  public synchronized void promoteToLeader(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    amplificationFactorAdapter.execute(topicPartition.getPartitionNumber(), subPartition -> {
      partitionToPendingConsumerActionCountMap.computeIfAbsent(subPartition, x -> new AtomicInteger(0))
          .incrementAndGet();
      consumerActionsQueue.add(
          new ConsumerAction(
              STANDBY_TO_LEADER,
              new PubSubTopicPartitionImpl(topicPartition.getPubSubTopic(), subPartition),
              nextSeqNum(),
              checker));
    });
  }

  @Override
  public synchronized void demoteToStandby(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    amplificationFactorAdapter.execute(topicPartition.getPartitionNumber(), subPartition -> {
      partitionToPendingConsumerActionCountMap.computeIfAbsent(subPartition, x -> new AtomicInteger(0))
          .incrementAndGet();
      consumerActionsQueue.add(
          new ConsumerAction(
              LEADER_TO_STANDBY,
              new PubSubTopicPartitionImpl(topicPartition.getPubSubTopic(), subPartition),
              nextSeqNum(),
              checker));
    });
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
          LOGGER.info(
              "State transition from STANDBY to LEADER is skipped for topic {} partition {}, because Helix has assigned another role to this replica.",
              topicName,
              partition);
          return;
        }

        PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState == null) {
          LOGGER.info(
              "State transition from STANDBY to LEADER is skipped for topic {} partition {}, because partition consumption state is null and the partition may have been unsubscribed.",
              topicName,
              partition);
          return;
        }

        if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
          LOGGER.info(
              "State transition from STANDBY to LEADER is skipped for topic {} partition {}, because this replica is the leader already.",
              topicName,
              partition);
          return;
        }
        if (store.isMigrationDuplicateStore()) {
          partitionConsumptionState.setLeaderFollowerState(PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER);
          LOGGER.info("{} for partition {} is paused transition from STANDBY to LEADER", consumerTaskId, partition);
        } else {
          // Mark this partition in the middle of STANDBY to LEADER transition
          partitionConsumptionState.setLeaderFollowerState(IN_TRANSITION_FROM_STANDBY_TO_LEADER);

          LOGGER.info("{} for partition {} is in transition from STANDBY to LEADER", consumerTaskId, partition);
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
          LOGGER.info(
              "State transition from LEADER to STANDBY is skipped for topic {} partition {}, because Helix has assigned another role to this replica.",
              topicName,
              partition);
          return;
        }

        partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState == null) {
          LOGGER.info(
              "State transition from LEADER to STANDBY is skipped for topic {} partition {}, because partition consumption state is null and the partition may have been unsubscribed.",
              topicName,
              partition);
          return;
        }

        if (partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)) {
          LOGGER.info(
              "State transition from LEADER to STANDBY is skipped for topic {} partition {}, because this replica is a follower already.",
              topicName,
              partition);
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
          consumerUnSubscribe(leaderTopic, partitionConsumptionState);
          waitForAllMessageToBeProcessedFromTopicPartition(
              new PubSubTopicPartitionImpl(leaderTopic, partition),
              partitionConsumptionState);

          partitionConsumptionState.setConsumeRemotely(false);
          LOGGER.info(
              "{} disabled remote consumption from topic {} partition {}",
              consumerTaskId,
              leaderTopic,
              partition);
          // Followers always consume local VT and should not skip kafka message
          partitionConsumptionState.setSkipKafkaMessage(false);
          partitionConsumptionState.setLeaderFollowerState(STANDBY);
          updateLeaderTopicOnFollower(partitionConsumptionState);
          // subscribe back to local VT/partition
          consumerSubscribe(
              partitionConsumptionState.getSourceTopicPartition(topic),
              partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
              localKafkaServer);

          LOGGER.info("{} demoted to standby for partition {}", consumerTaskId, partition);
        } else {
          partitionConsumptionState.setLeaderFollowerState(STANDBY);
          updateLeaderTopicOnFollower(partitionConsumptionState);
        }

        /**
         * Close the writer to make sure the current segment is closed after the leader is demoted to standby.
         */
        endSegment(partition);
        break;
      default:
        processCommonConsumerAction(operation, message.getTopicPartition(), message.getLeaderState());
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
      if (!partitionConsumptionState.isComplete() && LatencyUtils
          .getElapsedTimeInMs(partitionConsumptionState.getConsumptionStartTimeInMs()) > this.bootstrapTimeoutInMs) {
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
                "{} became in transition to leader for partition {}",
                consumerTaskId,
                partitionConsumptionState.getPartition());
          }
          break;

        case IN_TRANSITION_FROM_STANDBY_TO_LEADER:
          if (canSwitchToLeaderTopic(partitionConsumptionState)) {
            LOGGER.info(
                "{} start promoting to leader for partition {} unsubscribing from current topic: {}",
                consumerTaskId,
                partition,
                kafkaVersionTopic);
            /**
             * There isn't any new message from the old leader for at least {@link newLeaderInactiveTime} minutes,
             * this replica can finally be promoted to leader.
             */
            // unsubscribe from previous topic/partition
            consumerUnSubscribe(versionTopic, partitionConsumptionState);

            LOGGER.info(
                "{} start promoting to leader for partition {}, unsubscribed from current topic: {}",
                consumerTaskId,
                partition,
                kafkaVersionTopic);
            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            if (offsetRecord.getLeaderTopic(pubSubTopicRepository) == null) {
              /**
               * If this follower has processed a TS, the leader topic field should have been set. So, it must have been
               * consuming from version topic. Now it is becoming the leader. So the VT becomes its leader topic.
               */
              offsetRecord.setLeaderTopic(versionTopic);
            }

            /**
             * When leader promotion actually happens and before leader switch to a new topic, restore the latest DIV check
             * results from drainer service to the DIV check validator that will be used in leader consumption thread
             */
            restoreProducerStatesForLeaderConsumption(partition);

            if (!amplificationFactorAdapter.isLeaderSubPartition(partition)
                && partitionConsumptionState.isEndOfPushReceived()) {
              LOGGER.info("Stop promoting non-leaderSubPartition: {} of store: {}} to leader.", partition, storeName);
              partitionConsumptionState.setLeaderFollowerState(STANDBY);
              consumerSubscribe(
                  partitionConsumptionState.getSourceTopicPartition(versionTopic),
                  partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
                  localKafkaServer);
            } else {
              startConsumingAsLeaderInTransitionFromStandby(partitionConsumptionState);
            }
            /**
             * The topic switch operation will be recorded but the actual topic switch happens only after the replica
             * is promoted to leader; we should check whether it's ready to serve after switching topic.
             *
             * In extreme case, if there is no message in real-time topic, there will be no new message after leader switch
             * to the real-time topic, so `isReadyToServe()` check will never be invoked.
             */
            defaultReadyToServeChecker.apply(partitionConsumptionState);
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
            String errorMsg = consumerTaskId + " Missing leader topic for actual leader. OffsetRecord: "
                + partitionConsumptionState.getOffsetRecord().toSimplifiedString();
            LOGGER.error(errorMsg);
            throw new VeniceException(errorMsg);
          }

          /** If LEADER is consuming remote VT or SR, EOP is already received, switch back to local fabrics. */
          if (shouldLeaderSwitchToLocalConsumption(partitionConsumptionState)) {
            // Unsubscribe from remote Kafka topic, but keep the consumer in cache.
            consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
            // If remote consumption flag is false, existing messages for the partition in the drainer queue should be
            // processed before that
            PubSubTopicPartition leaderTopicPartition =
                new PubSubTopicPartitionImpl(currentLeaderTopic, partitionConsumptionState.getPartition());
            waitForAllMessageToBeProcessedFromTopicPartition(leaderTopicPartition, partitionConsumptionState);

            partitionConsumptionState.setConsumeRemotely(false);
            LOGGER.info(
                "{} disabled remote consumption from topic {} partition {}",
                consumerTaskId,
                currentLeaderTopic,
                partition);
            if (isDataRecovery && partitionConsumptionState.isBatchOnly() && !versionTopic.equals(currentLeaderTopic)) {
              partitionConsumptionState.getOffsetRecord().setLeaderTopic(versionTopic);
              currentLeaderTopic = versionTopic;
            }
            /**
             * The flag is turned on in {@link LeaderFollowerStoreIngestionTask#shouldProcessRecord} avoid consuming
             * unwanted messages after EOP in remote VT, such as SOBR. Now that the leader switches to consume locally,
             * it should not skip any message.
             */
            partitionConsumptionState.setSkipKafkaMessage(false);
            // Subscribe to local Kafka topic
            consumerSubscribe(
                partitionConsumptionState.getSourceTopicPartition(currentLeaderTopic),
                partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
                localKafkaServer);
          }

          if (!amplificationFactorAdapter.isLeaderSubPartition(partition)
              && partitionConsumptionState.isEndOfPushReceived()) {
            consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
            partitionConsumptionState.setConsumeRemotely(false);
            partitionConsumptionState.setLeaderFollowerState(STANDBY);
            consumerSubscribe(
                partitionConsumptionState.getSourceTopicPartition(versionTopic),
                partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
                localKafkaServer);
            break;
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
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime
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
      hostLevelIngestionStats.recordCheckLongRunningTasksLatency(LatencyUtils.getLatencyInMS(checkStartTimeInNS));
    }

    if (pushTimeout) {
      // Timeout
      String errorMsg = "After waiting " + TimeUnit.MILLISECONDS.toHours(this.bootstrapTimeoutInMs)
          + " hours, resource:" + storeName + " partitions:" + timeoutPartitions + " still can not complete ingestion.";
      LOGGER.error(errorMsg);
      throw new VeniceTimeoutException(errorMsg);
    }
  }

  private boolean canSwitchToLeaderTopic(PartitionConsumptionState pcs) {
    /**
     * Potential risk: it's possible that Kafka consumer would starve one of the partitions for a long
     * time even though there are new messages in it, so it's possible that the old leader is still producing
     * after waiting for 5 minutes; if it does happen, followers will detect upstream offset rewind by
     * a different producer host name.
     */
    long lastTimestamp = getLastConsumedMessageTimestamp(pcs.getPartition());
    if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) <= newLeaderInactiveTime) {
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
    long localVTEndOffset = getKafkaTopicPartitionEndOffSet(localKafkaServer, versionTopic, pcs.getPartition());
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
            "{} enabled remote consumption from topic {} partition {}",
            consumerTaskId,
            offsetRecord.getLeaderTopic(pubSubTopicRepository),
            partition);
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
    final long leaderStartOffset = partitionConsumptionState
        .getLeaderOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, pubSubTopicRepository);

    // subscribe to the new upstream
    String leaderSourceKafkaURL = leaderSourceKafkaURLs.iterator().next();
    LOGGER.info(
        "{} is promoted to leader for partition {} and it is going to start consuming from "
            + "{} at offset {}; source Kafka url: {}; remote consumption flag: {}",
        consumerTaskId,
        partition,
        leaderTopicPartition,
        leaderStartOffset,
        leaderSourceKafkaURL,
        partitionConsumptionState.consumeRemotely());
    consumerSubscribe(leaderTopicPartition, leaderStartOffset, leaderSourceKafkaURL);

    syncConsumedUpstreamRTOffsetMapIfNeeded(
        partitionConsumptionState,
        Collections.singletonMap(leaderSourceKafkaURL, leaderStartOffset));

    LOGGER.info(
        "{}, as a leader, started consuming from {} at offset {}",
        consumerTaskId,
        leaderTopicPartition,
        leaderStartOffset);
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
    final int partition = partitionConsumptionState.getPartition();
    final PubSubTopic currentLeaderTopic =
        partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    final String newSourceKafkaServer = topicSwitch.sourceKafkaServers.get(0).toString();
    final PubSubTopicPartition newSourceTopicPartition =
        partitionConsumptionState.getSourceTopicPartition(newSourceTopic);
    long upstreamStartOffset = partitionConsumptionState
        .getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);

    if (upstreamStartOffset < 0) {
      if (topicSwitch.rewindStartTimestamp > 0) {
        upstreamStartOffset = getTopicPartitionOffsetByKafkaURL(
            newSourceKafkaServer,
            newSourceTopicPartition,
            topicSwitch.rewindStartTimestamp);
      } else {
        upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
      }
    }

    // unsubscribe the old source and subscribe to the new source
    consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
    waitForLastLeaderPersistFuture(
        partitionConsumptionState,
        String.format(
            "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
            currentLeaderTopic,
            newSourceTopic,
            partition));

    // subscribe to the new upstream
    if (isNativeReplicationEnabled && !newSourceKafkaServer.equals(localKafkaServer)) {
      partitionConsumptionState.setConsumeRemotely(true);
      LOGGER.info("{} enabled remote consumption from {}", consumerTaskId, newSourceTopicPartition);
    }
    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
    partitionConsumptionState.getOffsetRecord()
        .setLeaderUpstreamOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, upstreamStartOffset);

    Set<String> sourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    if (sourceKafkaURLs.size() != 1) {
      throw new VeniceException("In L/F mode, expect only one leader source Kafka URL. Got: " + sourceKafkaURLs);
    }
    String sourceKafkaURL = sourceKafkaURLs.iterator().next();
    consumerSubscribe(newSourceTopicPartition, upstreamStartOffset, sourceKafkaURL);

    syncConsumedUpstreamRTOffsetMapIfNeeded(
        partitionConsumptionState,
        Collections.singletonMap(sourceKafkaURL, upstreamStartOffset));

    LOGGER.info(
        "{} leader successfully switch feed topic from {} to {} offset {} partition {}",
        consumerTaskId,
        currentLeaderTopic,
        newSourceTopic,
        upstreamStartOffset,
        partition);

    // In case new topic is empty and leader can never become online
    defaultReadyToServeChecker.apply(partitionConsumptionState);
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
      statusReportAdapter.reportError(Collections.singletonList(partitionConsumptionState), errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  protected long getTopicPartitionOffsetByKafkaURL(
      CharSequence kafkaURL,
      PubSubTopicPartition pubSubTopicPartition,
      long rewindStartTimestamp) {
    long topicPartitionOffset =
        getTopicManager(kafkaURL.toString()).getPartitionOffsetByTime(pubSubTopicPartition, rewindStartTimestamp);
    /**
     * {@link com.linkedin.venice.kafka.TopicManager#getPartitionOffsetByTime} will always return the next offset
     * to consume, but {@link ApacheKafkaConsumer#subscribe} is always
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
    return topicSwitchWrapper.getSourceServers();
  }

  /**
   * This method get the timestamp of the "last" message in topic/partition; notice that when the function
   * returns, new messages can be appended to the partition already, so it's not guaranteed that this timestamp
   * is from the last message.
   */
  private long getLastConsumedMessageTimestamp(int partition) {

    /**
     * Ingestion thread would update the last consumed message timestamp for the corresponding partition.
     */
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    return partitionConsumptionState.getLatestMessageConsumptionTimestampInMs();
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
      if (amplificationFactorAdapter != null) {
        latestConsumedProducerTimestamp = getLatestConsumedProducerTimestampWithSubPartition(
            latestConsumedProducerTimestamp,
            partitionConsumptionState);
      }
      if (LatencyUtils
          .getElapsedTimeInMs(latestConsumedProducerTimestamp) < dataRecoveryCompletionTimeLagThresholdInMs) {
        LOGGER.info(
            "Data recovery completed for topic: {} partition: {} upon consuming records with "
                + "producer timestamp of {} which is within the data recovery completion lag threshold of {} ms",
            kafkaVersionTopic,
            partitionConsumptionState.getPartition(),
            latestConsumedProducerTimestamp,
            dataRecoveryCompletionTimeLagThresholdInMs);
        isDataRecoveryCompleted = true;
      }
    }
    if (!isDataRecoveryCompleted) {
      long dataRecoverySourceVTEndOffset = cachedKafkaMetadataGetter.getOffset(
          topicManagerRepository.getTopicManager(nativeReplicationSourceVersionTopicKafkaURL),
          versionTopic,
          partitionConsumptionState.getPartition());

      // If the last few records in source VT is old then we can also complete data recovery if the leader idles and we
      // have reached/passed the end offset of the VT (around the time when ingestion is started for that partition).
      long localVTOffsetProgress = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
      // -2 because getOffset returns the next available offset (-1) and we are skipping the remote TS message (-1).
      boolean isAtEndOfSourceVT = dataRecoverySourceVTEndOffset - 2 <= localVTOffsetProgress;
      long lastTimestamp = getLastConsumedMessageTimestamp(partitionConsumptionState.getPartition());
      if (isAtEndOfSourceVT && LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
        LOGGER.info(
            "Data recovery completed for topic: {} partition: {} upon exceeding leader inactive time of {} ms",
            kafkaVersionTopic,
            partitionConsumptionState.getPartition(),
            newLeaderInactiveTime);
        isDataRecoveryCompleted = true;
      }
    }

    if (isDataRecoveryCompleted) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      statusReportAdapter.reportDataRecoveryCompleted(partitionConsumptionState);
    }
  }

  /**
   * For the corresponding partition being tracked in `partitionConsumptionState`, if it's in LEADER state and it's
   * not consuming from version topic, it should produce the new message to version topic; besides, if LEADER is
   * consuming remotely, it should also produce to local fabric.
   *
   * If buffer replay is disable, all replicas will stick to version topic, no one is going to produce any message.
   */
  protected boolean shouldProduceToVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    if (!isLeader(partitionConsumptionState)) {
      return false; // Not leader
    }
    PubSubTopic leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    return (!versionTopic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely());
  }

  protected boolean isLeader(PartitionConsumptionState partitionConsumptionState) {
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
    /**
     * During batch push, all subPartitions in LEADER will consume from leader topic (either local or remote VT)
     * Once we switch into RT topic consumption, only leaderSubPartition should be acting as LEADER role.
     * Hence, before processing TopicSwitch message, we need to force downgrade other subPartitions into FOLLOWER.
     */
    if (isLeader(partitionConsumptionState) && !amplificationFactorAdapter.isLeaderSubPartition(partition)) {
      LOGGER.info("SubPartition: {} is demoted from LEADER to STANDBY.", partitionConsumptionState.getPartition());
      PubSubTopic currentLeaderTopic =
          partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
      consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
      waitForLastLeaderPersistFuture(
          partitionConsumptionState,
          String.format(
              "Leader failed to produce the last message to version topic before switching feed topic from %s to %s on partition %s",
              currentLeaderTopic,
              kafkaVersionTopic,
              partition));
      partitionConsumptionState.setConsumeRemotely(false);
      partitionConsumptionState.setLeaderFollowerState(STANDBY);
      consumerSubscribe(
          partitionConsumptionState.getSourceTopicPartition(versionTopic),
          partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset(),
          localKafkaServer);
    }

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
    statusReportAdapter.reportTopicSwitchReceived(partitionConsumptionState);
    String sourceKafkaURL = kafkaServerUrls.get(0).toString();

    // Venice servers calculate the start offset based on start timestamp
    String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(newSourceTopicName);
    long upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
    // Since DaVinci clients might not have network ACLs to remote RT, they will skip upstream start offset calculation.
    if (!isDaVinciClient && topicSwitch.rewindStartTimestamp > 0) {
      int newSourceTopicPartitionId = partitionConsumptionState.getSourceTopicPartitionNumber(newSourceTopic);
      PubSubTopicPartition newSourceTopicPartition =
          new PubSubTopicPartitionImpl(newSourceTopic, newSourceTopicPartitionId);
      upstreamStartOffset = getTopicManager(sourceKafkaURL)
          .getPartitionOffsetByTime(newSourceTopicPartition, topicSwitch.rewindStartTimestamp);
      if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
        upstreamStartOffset -= 1;
      }
    }

    syncTopicSwitchToIngestionMetadataService(
        topicSwitch,
        partitionConsumptionState,
        Collections.singletonMap(sourceKafkaURL, upstreamStartOffset));

    if (isLeader(partitionConsumptionState)) {
      /**
       * Leader shouldn't switch topic here (drainer thread), which would conflict with the ingestion thread which would
       * also access consumer.
       *
       * Besides, if there is re-balance, leader should finish consuming the everything in VT before switching topics;
       * there could be more than one TopicSwitch message in VT, we should honor the last one during re-balance; so
       * don't update the consumption state like leader topic until actually switching topic. The leaderTopic field
       * should be used to track the topic that leader is actually consuming.
       */
      partitionConsumptionState.getOffsetRecord()
          .setLeaderUpstreamOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, upstreamStartOffset);
    } else {
      /**
       * For follower, just keep track of what leader is doing now.
       */
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopic);
      partitionConsumptionState.getOffsetRecord()
          .setLeaderUpstreamOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, upstreamStartOffset);
      /**
       * We need to measure offset lag after processing TopicSwitch for follower; if real-time topic is empty and never
       * gets any new message, follower replica will never become online.
       * If we measure lag here for follower, follower might become online faster than leader in extreme case:
       * Real time topic for that partition is empty or the rewind start offset is very closed to the end, followers
       * calculate the lag of the leader and decides the lag is small enough.
       */
      return true;
    }
    return false;
  }

  protected void syncTopicSwitchToIngestionMetadataService(
      TopicSwitch topicSwitch,
      PartitionConsumptionState partitionConsumptionState,
      Map<String, Long> upstreamStartOffsetByKafkaURL) {
    storageMetadataService.computeStoreVersionState(kafkaVersionTopic, previousStoreVersionState -> {
      if (previousStoreVersionState != null) {
        if (previousStoreVersionState.topicSwitch == null) {
          LOGGER.info(
              "First time receiving a TopicSwitch message (new source topic: {}; "
                  + "rewind start time: {}; upstream start offset by source Kafka URL: {})",
              topicSwitch.sourceTopicName,
              topicSwitch.rewindStartTimestamp,
              upstreamStartOffsetByKafkaURL);
        } else {
          LOGGER.info(
              "Previous TopicSwitch message in metadata store (source topic: {}; rewind start time: {}; "
                  + "source kafka servers {}) will be replaced by the new TopicSwitch message (new source topic: {}; "
                  + "rewind start time: {}; upstream start offset by source Kafka URL: {})",
              previousStoreVersionState.topicSwitch.sourceTopicName,
              previousStoreVersionState.topicSwitch.rewindStartTimestamp,
              topicSwitch.sourceKafkaServers,
              topicSwitch.sourceTopicName,
              topicSwitch.rewindStartTimestamp,
              upstreamStartOffsetByKafkaURL);
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
                + partitionConsumptionState + " and upstreamStartOffsetByKafkaURL: " + upstreamStartOffsetByKafkaURL);
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
   */
  protected void updateOffsetsFromConsumerRecord(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      UpdateVersionTopicOffset updateVersionTopicOffsetFunction,
      UpdateUpstreamTopicOffset updateUpstreamTopicOffsetFunction,
      GetLastKnownUpstreamTopicOffset lastKnownUpstreamTopicOffsetSupplier,
      Supplier<String> sourceKafkaUrlSupplier) {

    // Only update the metadata if this replica should NOT produce to version topic.
    if (!shouldProduceToVersionTopic(partitionConsumptionState)) {
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
          final long previousUpstreamOffset = lastKnownUpstreamTopicOffsetSupplier.apply(sourceKafkaUrl, upstreamTopic);
          checkAndHandleUpstreamOffsetRewind(
              partitionConsumptionState,
              partitionConsumptionState.getOffsetRecord(),
              consumerRecord,
              newUpstreamOffset,
              previousUpstreamOffset);
          /**
           * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
           * to the true leader when the old leader doesn't stop producing.
           */
          updateUpstreamTopicOffsetFunction.apply(sourceKafkaUrl, upstreamTopic, newUpstreamOffset);
        }
        // update leader producer GUID
        partitionConsumptionState.setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
        if (kafkaValue.leaderMetadataFooter != null) {
          partitionConsumptionState.setLeaderHostId(kafkaValue.leaderMetadataFooter.hostName.toString());
        }
      }
    } else {
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
      String msg = consumerTaskId + " UpdateOffset: Produced record should not be null in LEADER for: "
          + consumerRecord.getTopicPartition();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.warn(msg);
      }
    }
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl) {
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
        () -> OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  protected void checkAndHandleUpstreamOffsetRewind(
      PartitionConsumptionState partitionConsumptionState,
      OffsetRecord offsetRecord,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      final long newUpstreamOffset,
      final long previousUpstreamOffset) {
    if (newUpstreamOffset >= previousUpstreamOffset) {
      return; // Rewind did not happen
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
          consumerTaskId + " partition %d received message with upstreamOffset: %d;"
              + " but recorded upstreamOffset is: %d. Received message producer GUID: %s; Recorded producer GUID: %s;"
              + " Received message producer host: %s; Recorded producer host: %s."
              + " Multiple leaders are producing. ",
          consumerRecord.getTopicPartition().getPartitionNumber(),
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
        AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
        switch (MessageType.valueOf(envelope)) {
          case PUT:
            // Issue an read to get the current value of the key
            byte[] actualValue =
                storageEngine.get(consumerRecord.getTopicPartition().getPartitionNumber(), key.getKey());
            if (actualValue != null) {
              int actualSchemaId = ByteUtils.readInt(actualValue, 0);
              Put put = (Put) envelope.payloadUnion;
              if (actualSchemaId == put.schemaId) {
                // continue if schema Id is the same
                if (ByteUtils.equals(
                    put.putValue.array(),
                    put.putValue.position(),
                    actualValue,
                    ValueRecord.SCHEMA_HEADER_LENGTH)) {
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
        LOGGER.warn("{} failed comparing the rewind message with the actual value in Venice", consumerTaskId, e);
      }

      if (lossy) {
        if (!partitionConsumptionState.isEndOfPushReceived()) {
          logMsg += Utils.NEW_LINE_CHAR;
          logMsg += "Failing the job because lossy rewind happens before receiving EndOfPush";
          LOGGER.error(logMsg);
          versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
          VeniceException e = new VeniceException(logMsg);
          statusReportAdapter.reportError(Collections.singletonList(partitionConsumptionState), logMsg, e);
          throw e;
        } else {
          logMsg += Utils.NEW_LINE_CHAR;
          logMsg += "Don't fail the job during streaming ingestion";
          LOGGER.error(logMsg);
          versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
        }
      } else {
        LOGGER.info(logMsg);
        versionedDIVStats.recordBenignLeaderOffsetRewind(storeName, versionNumber);
      }
    }
  }

  protected void produceToLocalKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceFunction,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs) {
    LeaderProducerCallback callback = createProducerCallback(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
    long sourceTopicOffset = consumerRecord.getOffset();
    LeaderMetadataWrapper leaderMetadataWrapper = new LeaderMetadataWrapper(sourceTopicOffset, kafkaClusterId);
    partitionConsumptionState.setLastLeaderPersistFuture(leaderProducedRecordContext.getPersistedToDBFuture());
    produceFunction.accept(callback, leaderMetadataWrapper);
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
     * ingestion, {@link CachedKafkaMetadataGetter} was introduced to get the latest offset periodically;
     * with this strategy, it is possible that partition could become 'ONLINE' at most
     * {@link CachedKafkaMetadataGetter#ttlMs} earlier.
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
      return cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), versionTopic, partition)
          - partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
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

  protected long measureRTOffsetLagForSingleRegion(
      String sourceRealTimeTopicKafkaURL,
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    return getLatestLeaderPersistedOffsetAndHybridTopicOffset(
        sourceRealTimeTopicKafkaURL,
        partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository),
        partitionConsumptionState,
        shouldLogLag);
  }

  protected long measureRTOffsetLagForMultiRegions(
      Set<String> sourceRealTimeTopicKafkaURLs,
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    throw new VeniceException(
        String.format(
            "%s Multi colo RT offset lag calculation is not supported for non Active-Active stores",
            consumerTaskId));
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

    if (pcs.isEndOfPushReceived() && !pcs.isLatchReleased()) {
      if (cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), versionTopic, partition) - 1 <= pcs
          .getLatestProcessedLocalVersionTopicOffset()) {
        statusReportAdapter.reportCatchUpVersionTopicOffsetLag(pcs);

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
          amplificationFactorAdapter
              .executePartitionConsumptionState(pcs.getUserPartition(), PartitionConsumptionState::lagHasCaughtUp);
          statusReportAdapter.reportCompleted(pcs, true);
        }
      }
    }
  }

  /**
   * For Leader/Follower model, the follower should have the same kind of check as the Online/Offline model;
   * for leader, it's possible that it consumers from real-time topic or GF topic.
   */
  @Override
  protected boolean shouldProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record, int subPartition) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
    if (partitionConsumptionState == null) {
      LOGGER.info(
          "Skipping message as partition is no longer actively subscribed. Topic: {} Partition Id: {}",
          kafkaVersionTopic,
          subPartition);
      return false;
    }
    switch (partitionConsumptionState.getLeaderFollowerState()) {
      case LEADER:
        PubSubTopic currentLeaderTopic =
            partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
        if (partitionConsumptionState.consumeRemotely()
            && currentLeaderTopic.isVersionTopicOrStreamReprocessingTopic()) {
          if (partitionConsumptionState.skipKafkaMessage()) {
            String msg = "Skipping messages after EOP in remote version topic. Topic: " + kafkaVersionTopic
                + " Partition Id: " + subPartition;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              LOGGER.info(msg);
            }
            return false;
          }
          if (record.getKey().isControlMessage()) {
            ControlMessageType controlMessageType =
                ControlMessageType.valueOf((ControlMessage) record.getValue().payloadUnion);
            if (controlMessageType == END_OF_PUSH) {
              /**
               * The flag is turned on to avoid consuming unwanted messages after EOP in remote VT, such as SOBR. In
               * {@link LeaderFollowerStoreIngestionTask#checkLongRunningTaskState()}, once leader notices that EOP is
               * received, it will unsubscribe from the remote VT and turn off this flag. However, if data recovery is
               * in progress and the store is hybrid then we actually want to consume messages after EOP. In that case
               * remote TS will be skipped but with a different method.
               */
              if (!(isDataRecovery && isHybridMode())) {
                partitionConsumptionState.setSkipKafkaMessage(true);
              }
            }
          }
        }
        if (!record.getTopicPartition().getPubSubTopic().equals(currentLeaderTopic)) {
          String errorMsg = "Leader receives a Kafka record that doesn't belong to leader topic. Store version: "
              + this.kafkaVersionTopic + ", partition: " + partitionConsumptionState.getPartition() + ", leader topic: "
              + currentLeaderTopic + ", topic of incoming message: "
              + record.getTopicPartition().getPubSubTopic().getName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            LOGGER.error(errorMsg);
          }
          return false;
        }
        break;
      default:
        PubSubTopic pubSubTopic = record.getTopicPartition().getPubSubTopic();
        String topicName = pubSubTopic.getName();
        if (!versionTopic.equals(pubSubTopic)) {
          String errorMsg = consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderFollowerState()
              + "; partition: " + subPartition + "; Message retrieved from non version topic " + topicName;
          if (consumerHasSubscription(pubSubTopic, partitionConsumptionState)) {
            throw new VeniceMessageException(
                errorMsg + ". Throwing exception as the node still subscribes to " + topicName);
          }
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            LOGGER.error("{}. Skipping the message as the node does not subscribe to {}", errorMsg, topicName);
          }
          return false;
        }

        long lastOffset = partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset();
        if (lastOffset >= record.getOffset()) {
          String message = consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderFollowerState()
              + "; The record was already processed partition " + subPartition;
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
            LOGGER.info("{}; LastKnown {}; Current {}", message, lastOffset, record.getOffset());
          }
          return false;
        }
        break;
    }

    return super.shouldProcessRecord(record, subPartition);
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

    switch (partitionConsumptionState.getLeaderFollowerState()) {
      case LEADER:
        PubSubTopic currentLeaderTopic =
            partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
        if (!record.getTopicPartition().getPubSubTopic().equals(currentLeaderTopic)) {
          String errorMsg = "Leader receives a Kafka record that doesn't belong to leader topic. Store version: "
              + this.kafkaVersionTopic + ", partition: " + partitionConsumptionState.getPartition() + ", leader topic: "
              + currentLeaderTopic + ", topic of incoming message: " + currentLeaderTopic.getName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            LOGGER.error(errorMsg);
          }
          return false;
        }
        break;
      default:
        if (!record.getTopicPartition().getPubSubTopic().equals(this.versionTopic)) {
          String errorMsg = partitionConsumptionState.getLeaderFollowerState().toString()
              + " replica receives a Kafka record that doesn't belong to version topic. Store version: "
              + this.kafkaVersionTopic + ", partition: " + partitionConsumptionState.getPartition()
              + ", topic of incoming message: " + record.getTopicPartition().getPubSubTopic().getName();
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
      long producerConsumerLatencyMs,
      PartitionConsumptionState partitionConsumptionState) {
    if (isUserSystemStore()) {
      return;
    }
    if (isNativeReplicationEnabled) {
      // Emit latency metrics separately for leaders and followers
      boolean isLeader = partitionConsumptionState.getLeaderFollowerState().equals(LEADER);
      if (isLeader) {
        versionedDIVStats.recordLeaderLatencies(
            storeName,
            versionNumber,
            consumerTimestampMs,
            producerBrokerLatencyMs,
            brokerConsumerLatencyMs,
            producerConsumerLatencyMs);
      } else {
        versionedDIVStats.recordFollowerLatencies(
            storeName,
            versionNumber,
            consumerTimestampMs,
            producerBrokerLatencyMs,
            brokerConsumerLatencyMs,
            producerConsumerLatencyMs);
      }
    } else {
      super.recordWriterStats(
          consumerTimestampMs,
          producerBrokerLatencyMs,
          brokerConsumerLatencyMs,
          producerConsumerLatencyMs,
          partitionConsumptionState);
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

  private void recordRegionHybridConsumptionStats(
      int kafkaClusterId,
      int producedRecordSize,
      long upstreamOffset,
      long currentTimeMs) {
    if (kafkaClusterId >= 0) {
      versionedIngestionStats.recordRegionHybridConsumption(
          storeName,
          versionNumber,
          kafkaClusterId,
          producedRecordSize,
          upstreamOffset,
          currentTimeMs);
      hostLevelIngestionStats.recordTotalRegionHybridBytesConsumed(kafkaClusterId, producedRecordSize, currentTimeMs);
    }
  }

  /**
   * The goal of this function is to possibly produce the incoming kafka message consumed from local VT, remote VT, RT or SR topic to
   * local VT if needed. It's decided based on the function output of {@link #shouldProduceToVersionTopic} and message type.
   * It also perform any necessary additional computation operation such as for write-compute/update message.
   * It returns a boolean indicating if it was produced to kafka or not.
   *
   * This function should be called as one of the first steps in processing pipeline for all messages consumed from any kafka topic.
   *
   * The caller of this function should only process this {@param consumerRecord} further if the return is
   * {@link DelegateConsumerRecordResult#QUEUED_TO_DRAINER}.
   *
   * This function assumes {@link #shouldProcessRecord(PubSubMessage, int)} has been called which happens in
   * {@link StoreIngestionTask#produceToStoreBufferServiceOrKafka(Iterable, PubSubTopicPartition, String, int)}
   * before calling this and the it was decided that this record needs to be processed. It does not perform any
   * validation check on the PartitionConsumptionState object to keep the goal of the function simple and not overload.
   *
   * Also DIV validation is done here if the message is received from RT topic. For more info please see
   * please see {@literal StoreIngestionTask#internalProcessConsumerRecord(PubSubMessage, PartitionConsumptionState, LeaderProducedRecordContext, int, String, long)}
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
   *
   * @return a {@link DelegateConsumerRecordResult} indicating what to do with the record
   */
  @Override
  protected DelegateConsumerRecordResult delegateConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) {
    boolean produceToLocalKafka = false;
    try {
      KafkaKey kafkaKey = consumerRecord.getKey();
      KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
      /**
       * partitionConsumptionState must be in a valid state and no error reported. This is made sure by calling
       * {@link shouldProcessRecord} before processing any record.
       *
       * ^ This is no longer true because with shared consumer the partitionConsumptionState could have been removed
       * from unsubscribe action in the StoreIngestionTask thread. Today, when unsubscribing
       * {@link StoreIngestionTask.waitForAllMessageToBeProcessedFromTopicPartition} only ensure the buffer queue is
       * drained before unsubscribe. Records being processed by shared consumer may see invalid partitionConsumptionState.
       */
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
      if (partitionConsumptionState == null) {
        // The partition is likely unsubscribed, will skip these messages.
        return DelegateConsumerRecordResult.SKIPPED_MESSAGE;
      }
      produceToLocalKafka = shouldProduceToVersionTopic(partitionConsumptionState);
      // UPDATE message is only expected in LEADER which must be produced to kafka.
      MessageType msgType = MessageType.valueOf(kafkaValue);
      if (msgType == MessageType.UPDATE && !produceToLocalKafka) {
        throw new VeniceMessageException(
            consumerTaskId + " hasProducedToKafka: Received UPDATE message in non-leader for: "
                + consumerRecord.getTopicPartition() + " Offset " + consumerRecord.getOffset());
      }

      /**
       * return early if it needs not be produced to local VT such as cases like
       * (i) it's a follower or (ii) leader is consuming from VT
       */
      if (!produceToLocalKafka) {
        return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
      }

      // If we are here the message must be produced to local kafka or silently consumed.
      LeaderProducedRecordContext leaderProducedRecordContext;

      validateRecordBeforeProducingToLocalKafka(consumerRecord, partitionConsumptionState, kafkaUrl, kafkaClusterId);

      if (consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
        recordRegionHybridConsumptionStats(
            kafkaClusterId,
            consumerRecord.getPayloadSize(),
            consumerRecord.getOffset(),
            currentTimeForMetricsMs);
        updateLatestInMemoryLeaderConsumedRTOffset(partitionConsumptionState, kafkaUrl, consumerRecord.getOffset());
      }

      /**
       * Just to note this code is getting executed in Leader only. Leader DIV check progress is always ahead of the
       * actual data persisted on disk. Leader DIV check results will not be persisted on disk.
       */
      boolean isEndOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
      try {
        /**
         * TODO: An improvement can be made to fail all future versions for fatal DIV exceptions after EOP.
         */
        validateMessage(
            this.kafkaDataIntegrityValidatorForLeaders,
            consumerRecord,
            isEndOfPushReceived,
            partitionConsumptionState);
        versionedDIVStats.recordSuccessMsg(storeName, versionNumber);
      } catch (FatalDataValidationException e) {
        if (!isEndOfPushReceived) {
          throw e;
        }
      } catch (DuplicateDataException e) {
        /**
         * Skip duplicated messages; leader must not produce duplicated messages from RT to VT, because leader will
         * override the DIV info for messages from RT; as a result, both leaders and followers will persisted duplicated
         * messages to disk, and potentially rewind a k/v pair to an old value.
         */
        divErrorMetricCallback.accept(e);
        LOGGER.debug("{} : Skipping a duplicate record at offset: {}", consumerTaskId, consumerRecord.getOffset());
        return DelegateConsumerRecordResult.DUPLICATE_MESSAGE;
      }

      if (kafkaKey.isControlMessage()) {
        boolean producedFinally = true;
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
        leaderProducedRecordContext = LeaderProducedRecordContext
            .newControlMessageRecord(kafkaClusterId, consumerRecord.getOffset(), kafkaKey.getKey(), controlMessage);
        switch (controlMessageType) {
          case START_OF_PUSH:
            /**
             * N.B.: This is expected to be the first time time we call {@link veniceWriter#get()}. During this time
             *       since startOfPush has not been processed yet, {@link StoreVersionState} is not created yet (unless
             *       this is a server restart scenario). So the {@link com.linkedin.venice.writer.VeniceWriter#isChunkingEnabled} field
             *       will not be set correctly at this point. This is fine as chunking is mostly not applicable for control messages.
             *       This chunking flag for the veniceWriter will happen be set correctly in
             *       {@link StoreIngestionTask#processStartOfPush(ControlMessage, int, long, PartitionConsumptionState)},
             *       which will be called when this message gets processed in drainer thread after successfully producing
             *       to kafka.
             *
             * Note update: the first time we call {@link veniceWriter#get()} is different in various use cases:
             * 1. For hybrid store with L/F enabled, the first time a VeniceWriter is created is after leader switches to RT and
             *    consumes the first message; potential message type: SOS, EOS, data message.
             * 2. For store version generated by stream reprocessing push type, the first time is after leader switches to
             *    SR topic and consumes the first message; potential message type: SOS, EOS, data message (consider server restart).
             * 3. For store with native replication enabled, the first time is after leader switches to remote topic and start
             *    consumes the first message; potential message type: SOS, EOS, SOP, EOP, data message (consider server restart).
             */
          case END_OF_PUSH:
            /**
             * Simply produce this EOP to local VT. It will be processed in order in the drainer queue later
             * after successfully producing to kafka.
             */
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get()
                    .put(
                        consumerRecord.getKey(),
                        consumerRecord.getValue(),
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper),
                subPartition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingRecordTimestampNs);
            break;
          case START_OF_SEGMENT:
          case END_OF_SEGMENT:
            /**
             * SOS and EOS will be produced to the local version topic with DIV pass-through mode by leader in the following cases:
             * 1. SOS and EOS are from stream-reprocessing topics (use cases: stream-reprocessing)
             * 2. SOS and EOS are from version topics in a remote fabric (use cases: native replication for remote fabrics)
             *
             * SOS and EOS will not be produced to local version topic in the following cases:
             * 1. SOS and EOS are from real-time topics (use cases: hybrid ingestion, incremental push to RT)
             * 2. SOS and EOS are from version topics in local fabric, which has 2 different scenarios:
             *    i.  native replication is enabled, but the current fabric is the source fabric (use cases: native repl for source fabric)
             *    ii. native replication is not enabled; it doesn't matter whether current replica is leader or follower,
             *        messages from local VT doesn't need to be reproduced into local VT again (use cases: local batch consumption,
             *        incremental push to VT)
             */
            if (!consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()) {
              produceToLocalKafka(
                  consumerRecord,
                  partitionConsumptionState,
                  leaderProducedRecordContext,
                  (callback, leaderMetadataWrapper) -> veniceWriter.get()
                      .put(
                          consumerRecord.getKey(),
                          consumerRecord.getValue(),
                          callback,
                          consumerRecord.getTopicPartition().getPartitionNumber(),
                          leaderMetadataWrapper),
                  subPartition,
                  kafkaUrl,
                  kafkaClusterId,
                  beforeProcessingRecordTimestampNs);
            } else {
              /**
               * Based on current design handling this case (specially EOS) is tricky as we don't produce the SOS/EOS
               * received from RT to local VT. But ideally EOS must be queued in-order (after all previous data message
               * PUT/UPDATE/DELETE) to drainer. But since the queueing of data message to drainer
               * happens in Kafka producer callback there is no way to queue this EOS to drainer in-order.
               *
               * Usually following processing in Leader for other control message.
               *    1. DIV:
               *    2. updateOffset:
               *    3. stats maintenance as in {@link StoreIngestionTask#processKafkaDataMessage(PubSubMessage, PartitionConsumptionState, LeaderProducedRecordContext)}
               *
               * For #1 Since we have moved the DIV validation in this function, We are good with DIV part which is the most critical one.
               * For #2 Leader will not update the offset for SOS/EOS. From Server restart point of view this is tolerable. This was the case in previous design also. So there is no change in behaviour.
               * For #3 stat counter update will not happen for SOS/EOS message. This should not be a big issue. If needed we can copy some of the stats maintenance
               *   work here.
               *
               * So in summary NO further processing is needed SOS/EOS received from RT topics. Just silently drop the message here.
               * We should not return false here.
               */
              producedFinally = false;
            }
            break;
          case START_OF_INCREMENTAL_PUSH:
          case END_OF_INCREMENTAL_PUSH:
            // For inc push to RT policy, the control msg is written in RT topic, we will need to calculate the
            // destination partition in VT correctly.
            int versionTopicPartitionToBeProduced = consumerRecord.getTopicPartition().getPubSubTopic().isRealTime()
                ? consumerRecord.getTopicPartition().getPartitionNumber() * amplificationFactor
                : consumerRecord.getTopicPartition().getPartitionNumber();
            /**
             * We are using {@link VeniceWriter#asyncSendControlMessage()} api instead of {@link VeniceWriter#put()} since we have
             * to calculate DIV for this message but keeping the ControlMessage content unchanged. {@link VeniceWriter#put()} does not
             * allow that.
             */
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get()
                    .asyncSendControlMessage(
                        controlMessage,
                        versionTopicPartitionToBeProduced,
                        new HashMap<>(),
                        callback,
                        leaderMetadataWrapper),
                subPartition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingRecordTimestampNs);
            break;
          case TOPIC_SWITCH:
            /**
             * For TOPIC_SWITCH message we should use -1 as consumedOffset. This will ensure that it does not update the
             * setLeaderUpstreamOffset in:
             * {@link #updateOffsetsAsRemoteConsumeLeader(PartitionConsumptionState, LeaderProducedRecordContext, String, PubSubMessage, UpdateVersionTopicOffset, UpdateUpstreamTopicOffset)}
             * The leaderUpstreamOffset is set from the TS message config itself. We should not override it.
             */
            if (isDataRecovery && !partitionConsumptionState.isBatchOnly()) {
              // Ignore remote VT's TS message since we might need to consume more RT or incremental push data from VT
              // that's no longer in the local/remote RT due to retention.
              return DelegateConsumerRecordResult.SKIPPED_MESSAGE;
            }
            leaderProducedRecordContext =
                LeaderProducedRecordContext.newControlMessageRecord(kafkaKey.getKey(), controlMessage);
            produceToLocalKafka(
                consumerRecord,
                partitionConsumptionState,
                leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get()
                    .asyncSendControlMessage(
                        controlMessage,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        new HashMap<>(),
                        callback,
                        DEFAULT_LEADER_METADATA_WRAPPER),
                subPartition,
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingRecordTimestampNs);
            break;
          case VERSION_SWAP:
            return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
          default:
            // do nothing
            break;
        }
        if (!isSegmentControlMsg(controlMessageType)) {
          LOGGER.info(
              "{} hasProducedToKafka: {}; ControlMessage: {}; topicPartition: {}; offset: {}",
              consumerTaskId,
              producedFinally,
              controlMessageType.name(),
              consumerRecord.getTopicPartition(),
              consumerRecord.getOffset());
        }
      } else if (kafkaValue == null) {
        throw new VeniceMessageException(
            consumerTaskId + " hasProducedToKafka: Given null Venice Message. TopicPartition: "
                + consumerRecord.getTopicPartition() + " Offset " + consumerRecord.getOffset());
      } else {
        // This function may modify the original record in KME and it is unsafe to use the payload from KME directly
        // after this call.
        processMessageAndMaybeProduceToKafka(
            consumerRecord,
            partitionConsumptionState,
            subPartition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs,
            currentTimeForMetricsMs);
      }
      return DelegateConsumerRecordResult.PRODUCED_TO_KAFKA;
    } catch (Exception e) {
      throw new VeniceException(
          consumerTaskId + " hasProducedToKafka: exception for message received from: "
              + consumerRecord.getTopicPartition() + ", Offset: " + consumerRecord.getOffset() + ". Bubbling up.",
          e);
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
    final long WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED = MINUTES.toMillis(1); // 1 min

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
            "Got interrupted while waiting for the last queued record to be persisted for: {}. "
                + "Will throw the interrupt exception.",
            topicPartition,
            e);
        throw e;
      } catch (Exception e) {
        LOGGER.error(
            "Got exception while waiting for the last queued record to be persisted for: {}. Will swallow.",
            topicPartition,
            e);
      }
      Future<Void> lastFuture = null;
      try {
        lastFuture = partitionConsumptionState.getLastLeaderPersistFuture();
        if (lastFuture != null) {
          long synchronizeStartTimeInNS = System.nanoTime();
          lastFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
          hostLevelIngestionStats
              .recordLeaderProducerSynchronizeLatency(LatencyUtils.getLatencyInMS(synchronizeStartTimeInNS));
        }
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Got interrupted while waiting for the last leader producer future for: {}. "
                + "No data loss. Will throw the interrupt exception.",
            topicPartition,
            e);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
        throw e;
      } catch (TimeoutException e) {
        LOGGER.error(
            "Timeout on waiting for the last leader producer future for: {}. No data loss. Will swallow.",
            topicPartition,
            e);
        lastFuture.cancel(true);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      } catch (Exception e) {
        LOGGER.error(
            "Got exception while waiting for the latest producer future to be completed for: {}. Will swallow.",
            topicPartition,
            e);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        // No need to fail the push job; just record the failure.
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      }
    }
  }

  /**
   * Checks before producing local version topic.
   *
   * Extend this function when there is new check needed.
   */
  private void validateRecordBeforeProducingToLocalKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId) {
    // Check whether the message is from local version topic; leader shouldn't consume from local VT and then produce
    // back to VT again
    if (kafkaClusterId == localKafkaClusterId
        && consumerRecord.getTopicPartition().getPubSubTopic().equals(this.versionTopic)
        && kafkaUrl.equals(this.localKafkaServer)) {
      // N.B.: Ideally, the first two conditions should be sufficient, but for some reasons, in certain tests, the
      // third condition also ends up being necessary. In any case, doing the cluster ID check should be a
      // fast short-circuit in normal cases.
      try {
        int partitionId = partitionConsumptionState.getPartition();
        setIngestionException(
            partitionId,
            new VeniceException(
                "Store version " + this.kafkaVersionTopic + " partition " + partitionId
                    + " is consuming from local version topic and producing back to local version topic"
                    + ", kafkaClusterId = " + kafkaClusterId + ", kafkaUrl = " + kafkaUrl + ", this.localKafkaServer = "
                    + this.localKafkaServer));
      } catch (VeniceException offerToQueueException) {
        setLastStoreIngestionException(offerToQueueException);
      }
    }
  }

  // calculate the the replication once per partition, checking Leader instance will make sure we calculate it just once
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

    long replicationLag = partitionConsumptionStateMap.values()
        .stream()
        .filter(BATCH_REPLICATION_LAG_FILTER)
        // the lag is (latest VT offset in remote kafka - latest VT offset in local kafka)
        .mapToLong((pcs) -> {
          PubSubTopic currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
          if (currentLeaderTopic == null) {
            currentLeaderTopic = versionTopic;
          }

          String sourceKafkaURL = getSourceKafkaUrlForOffsetLagMeasurement(pcs);
          // Consumer might not existed after the consumption state is created, but before attaching the corresponding
          // consumer.
          long offsetLagOptional = getPartitionOffsetLag(sourceKafkaURL, currentLeaderTopic, pcs.getUserPartition());
          if (offsetLagOptional >= 0) {
            return offsetLagOptional;
          }
          // Fall back to use the old way
          return (cachedKafkaMetadataGetter.getOffset(
              getTopicManager(nativeReplicationSourceVersionTopicKafkaURL),
              currentLeaderTopic,
              pcs.getPartition()) - 1)
              - (cachedKafkaMetadataGetter
                  .getOffset(getTopicManager(localKafkaServer), currentLeaderTopic, pcs.getPartition()) - 1);
        })
        .sum();

    return minZeroLag(replicationLag);
  }

  public static final Predicate<? super PartitionConsumptionState> LEADER_OFFSET_LAG_FILTER =
      pcs -> pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> BATCH_LEADER_OFFSET_LAG_FILTER =
      pcs -> !pcs.isEndOfPushReceived() && pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> HYBRID_LEADER_OFFSET_LAG_FILTER =
      pcs -> pcs.isEndOfPushReceived() && pcs.isHybrid() && pcs.getLeaderFollowerState().equals(LEADER);

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
          long offsetLagOptional = getPartitionOffsetLag(kafkaSourceAddress, currentLeaderTopic, pcs.getPartition());
          if (offsetLagOptional >= 0) {
            return offsetLagOptional;
          }

          // Fall back to calculate offset lag in the original approach
          if (currentLeaderTopic.isRealTime()) {
            return this.measureHybridOffsetLag(pcs, false);
          } else {
            return (cachedKafkaMetadataGetter
                .getOffset(getTopicManager(kafkaSourceAddress), currentLeaderTopic, pcs.getPartition()) - 1)
                - pcs.getLatestProcessedLocalVersionTopicOffset();
          }
        })
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
          // Consumer might not existed after the consumption state is created, but before attaching the corresponding
          // consumer.
          long offsetLagOptional = getPartitionOffsetLag(localKafkaServer, versionTopic, pcs.getPartition());
          if (offsetLagOptional >= 0) {
            return offsetLagOptional;
          }
          // Fall back to calculate offset lag in the old way
          return (cachedKafkaMetadataGetter
              .getOffset(getTopicManager(localKafkaServer), versionTopic, pcs.getPartition()) - 1)
              - pcs.getLatestProcessedLocalVersionTopicOffset();
        })
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

  protected void updateLatestInMemoryLeaderConsumedRTOffset(
      PartitionConsumptionState pcs,
      String ignoredKafkaUrl,
      long offset) {
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
    veniceWriter.ifPresent(vw -> vw.closePartition(partitionId));
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

  protected int getSubPartitionId(byte[] key, PubSubTopicPartition topicPartition) {
    return amplificationFactor != 1 && topicPartition.getPubSubTopic().isRealTime()
        ? venicePartitioner.getPartitionId(key, subPartitionCount)
        : topicPartition.getPartitionNumber();
  }

  /**
   * Compresses data in a bytebuffer when consuming from rt as a leader node and compression is enabled for the store
   * version for which we're consuming data.
   *
   * @param partition which partition we're acting on so as to determine the PartitionConsumptionState
   * @param data the data that we might compress
   * @return a bytebuffer that's either the original bytebuffer or a new one depending on if we compressed it.
   */
  protected ByteBuffer maybeCompressData(
      int partition,
      ByteBuffer data,
      PartitionConsumptionState partitionConsumptionState) {
    // To handle delete operations
    if (data == null) {
      return null;
    }
    if (shouldCompressData(partitionConsumptionState)) {
      try {
        // We need to expand the front of the returned bytebuffer to make room for schema header insertion
        return compressor.get().compress(data, ByteUtils.SIZE_OF_INT);
      } catch (IOException e) {
        // throw a loud exception if something goes wrong here
        throw new RuntimeException(
            String.format(
                "Failed to compress value in venice writer! Aborting write! partition: %d, leader topic: %s, compressor: %s",
                partition,
                partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository),
                compressor.getClass().getName()),
            e);
      }
    }
    return data;
  }

  protected boolean shouldCompressData(PartitionConsumptionState partitionConsumptionState) {
    if (!isLeader(partitionConsumptionState)) {
      return false; // Not leader, don't compress
    }
    PubSubTopic leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic(pubSubTopicRepository);
    if (!realTimeTopic.equals(leaderTopic)) {
      return false; // We're consuming from version topic (don't compress it)
    }
    return !compressionStrategy.equals(CompressionStrategy.NO_OP);
  }

  protected void processMessageAndMaybeProduceToKafka(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) {
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    byte[] keyBytes = kafkaKey.getKey();
    MessageType msgType = MessageType.valueOf(kafkaValue.messageType);
    switch (msgType) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        put.putValue = maybeCompressData(
            consumerRecord.getTopicPartition().getPartitionNumber(),
            put.putValue,
            partitionConsumptionState);
        ByteBuffer putValue = put.putValue;

        /**
         * For WC enabled stores update the transient record map with the latest {key,value}. This is needed only for messages
         * received from RT. Messages received from VT have been persisted to disk already before switching to RT topic.
         */
        if (isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived()) {
          partitionConsumptionState.setTransientRecord(
              kafkaClusterId,
              consumerRecord.getOffset(),
              keyBytes,
              putValue.array(),
              putValue.position(),
              putValue.remaining(),
              put.schemaId,
              null);
        }

        LeaderProducedRecordContext leaderProducedRecordContext =
            LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, put);
        produceToLocalKafka(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            (callback, leaderMetadataWrapper) -> {
              /**
               * 1. Unfortunately, Kafka does not support fancy array manipulation via {@link ByteBuffer} or otherwise,
               * so we may be forced to do a copy here, if the backing array of the {@link putValue} has padding,
               * which is the case when using {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer}.
               * Since this is in a closure, it is not guaranteed to be invoked.
               *
               * The {@link OnlineOfflineStoreIngestionTask}, which ignores this closure, will not pay this price.
               *
               * Conversely, the {@link LeaderFollowerStoreIngestionTask}, which does invoke it, will.
               *
               * TODO: Evaluate holistically what is the best way to optimize GC for the L/F case.
               *
               * 2. Enable venice writer "pass-through" mode if we haven't received EOP yet. In pass through mode,
               * Leader will reuse upstream producer metadata. This would secures the correctness of DIV states in
               * followers when the leadership failover happens.
               */

              if (!partitionConsumptionState.isEndOfPushReceived()) {
                veniceWriter.get()
                    .put(
                        kafkaKey,
                        kafkaValue,
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper);
              } else {
                /**
                 * When amplificationFactor != 1 and it is a leaderSubPartition consuming from the Real-time topic,
                 * VeniceWriter will run VenicePartitioner inside to decide which subPartition of Kafka VT to write to.
                 * For details about how VenicePartitioner find the correct subPartition,
                 * please check {@link com.linkedin.venice.partitioner.UserPartitionAwarePartitioner}
                 */
                veniceWriter.get()
                    .put(keyBytes, ByteUtils.extractByteArray(putValue), put.schemaId, callback, leaderMetadataWrapper);
              }
            },
            subPartition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
        break;

      case UPDATE:
        handleUpdateRequest(
            (Update) kafkaValue.payloadUnion,
            keyBytes,
            consumerRecord,
            kafkaUrl,
            kafkaClusterId,
            partitionConsumptionState,
            beforeProcessingRecordTimestampNs);
        break;

      case DELETE:
        /**
         * For WC enabled stores update the transient record map with the latest {key,null} for similar reason as mentioned in PUT above.
         */
        if (isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived()) {
          partitionConsumptionState.setTransientRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, -1, null);
        }
        leaderProducedRecordContext =
            LeaderProducedRecordContext.newDeleteRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, null);
        produceToLocalKafka(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            (callback, leaderMetadataWrapper) -> {
              /**
               * DIV pass-through for DELETE messages before EOP.
               */
              if (!partitionConsumptionState.isEndOfPushReceived()) {
                veniceWriter.get()
                    .delete(
                        kafkaKey,
                        kafkaValue,
                        callback,
                        consumerRecord.getTopicPartition().getPartitionNumber(),
                        leaderMetadataWrapper);
              } else {
                veniceWriter.get().delete(keyBytes, callback, leaderMetadataWrapper);
              }
            },
            subPartition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingRecordTimestampNs);
        break;

      default:
        throw new VeniceMessageException(
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
  }

  /**
   *  1. Currently, we support chunking only for messages produced on VT topic during batch part of the ingestion
   *     for hybrid stores. Chunking is NOT supported for messages produced to RT topics during streaming ingestion.
   *
   *     So the assumption here is that the PUT/UPDATE messages stored in transientRecord should always be a full value
   *     (non chunked). Decoding should succeed using the simplified API
   *     {@link ChunkingAdapter#constructValue}
   *
   *  2. We always use the latest value schema to deserialize stored value bytes.
   *  3. We always use the partial update schema with an ID combination of the latest value schema ID + update schema ID
   *     to deserialize the incoming Update request payload bytes.
   *
   *  The reason for 2 and 3 is that we depend on the fact that the latest value schema must be a superset schema
   *  that contains all value fields that ever existed in a store value schema. So, always using a superset schema
   *  as the reader schema avoids data loss where the serialized bytes contain data for a field, however, the
   *  deserialized record does not contain that field because the reader schema does not contain that field.
   */
  private void handleUpdateRequest(
      Update update,
      byte[] keyBytes,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      String kafkaUrl,
      int kafkaClusterId,
      PartitionConsumptionState partitionConsumptionState,
      long beforeProcessingRecordTimestampNs) {

    final int subPartition = partitionConsumptionState.getPartition();
    final int readerValueSchemaId;
    final int readerUpdateProtocolVersion;
    if (isIngestingSystemStore()) {
      readerValueSchemaId = schemaRepository.getSupersetOrLatestValueSchema(storeName).getId();
      readerUpdateProtocolVersion = schemaRepository.getLatestDerivedSchema(storeName, readerValueSchemaId).getId();
    } else {
      SchemaEntry supersetSchemaEntry = schemaRepository.getSupersetSchema(storeName);
      if (supersetSchemaEntry == null) {
        throw new IllegalStateException("Cannot find superset schema for store: " + storeName);
      }
      readerValueSchemaId = supersetSchemaEntry.getId();
      readerUpdateProtocolVersion = update.updateSchemaId;
    }
    ChunkedValueManifestContainer valueManifestContainer = new ChunkedValueManifestContainer();
    final GenericRecord currValue = readStoredValueRecord(
        partitionConsumptionState,
        keyBytes,
        readerValueSchemaId,
        consumerRecord.getTopicPartition(),
        valueManifestContainer);

    final byte[] updatedValueBytes;
    final ChunkedValueManifest oldValueManifest = valueManifestContainer.getManifest();

    try {
      long writeComputeStartTimeInNS = System.nanoTime();
      // Leader nodes are the only ones which process UPDATES, so it's valid to always compress and not call
      // 'maybeCompress'.
      updatedValueBytes = compressor.get()
          .compress(
              storeWriteComputeHandler.applyWriteCompute(
                  currValue,
                  update.schemaId,
                  readerValueSchemaId,
                  update.updateValue,
                  update.updateSchemaId,
                  readerUpdateProtocolVersion));
      hostLevelIngestionStats.recordWriteComputeUpdateLatency(LatencyUtils.getLatencyInMS(writeComputeStartTimeInNS));
    } catch (Exception e) {
      writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_UPDATE_FAILURE.code;
      throw new RuntimeException(e);
    }

    if (updatedValueBytes == null) {
      if (currValue != null) {
        throw new IllegalStateException(
            "Detect a situation where the current value exists and the Write Compute request"
                + "deletes the current value. It is unexpected because Write Compute only supports partial update and does "
                + "not support record value deletion.");
      } else {
        // No-op. The fact that currValue does not exist on the leader means currValue does not exist on the follower
        // either. So, there is no need to tell the follower replica to do anything.
      }
    } else {
      partitionConsumptionState.setTransientRecord(
          kafkaClusterId,
          consumerRecord.getOffset(),
          keyBytes,
          updatedValueBytes,
          0,
          updatedValueBytes.length,
          readerValueSchemaId,
          null);

      ByteBuffer updateValueWithSchemaId =
          ByteUtils.prependIntHeaderToByteBuffer(ByteBuffer.wrap(updatedValueBytes), readerValueSchemaId, false);

      Put updatedPut = new Put();
      updatedPut.putValue = updateValueWithSchemaId;
      updatedPut.schemaId = readerValueSchemaId;

      LeaderProducedRecordContext leaderProducedRecordContext =
          LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.getOffset(), keyBytes, updatedPut);

      BiConsumer<ChunkAwareCallback, LeaderMetadataWrapper> produceFunction =
          (callback, leaderMetadataWrapper) -> veniceWriter.get()
              .put(
                  keyBytes,
                  updatedValueBytes,
                  readerValueSchemaId,
                  callback,
                  leaderMetadataWrapper,
                  APP_DEFAULT_LOGICAL_TS,
                  null,
                  oldValueManifest,
                  null);

      produceToLocalKafka(
          consumerRecord,
          partitionConsumptionState,
          leaderProducedRecordContext,
          produceFunction,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs);
    }
  }

  /**
   * Read the existing value. If a value for this key is found from the transient map then use that value, otherwise read
   * it from the storage engine.
   * @return {@link Optional#empty} if the value
   */
  private GenericRecord readStoredValueRecord(
      PartitionConsumptionState partitionConsumptionState,
      byte[] keyBytes,
      int readerValueSchemaID,
      PubSubTopicPartition topicPartition,
      ChunkedValueManifestContainer manifestContainer) {
    final GenericRecord currValue;
    PartitionConsumptionState.TransientRecord transientRecord = partitionConsumptionState.getTransientRecord(keyBytes);
    if (transientRecord == null) {
      try {
        long lookupStartTimeInNS = System.nanoTime();
        currValue = GenericRecordChunkingAdapter.INSTANCE.get(
            storageEngine,
            getSubPartitionId(keyBytes, topicPartition),
            ByteBuffer.wrap(keyBytes),
            isChunked,
            null,
            null,
            null,
            readerValueSchemaID,
            storeDeserializerCache,
            compressor.get(),
            manifestContainer);
        hostLevelIngestionStats.recordWriteComputeLookUpLatency(LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
      } catch (Exception e) {
        writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_DESERIALIZATION_FAILURE.code;
        throw e;
      }
    } else {
      hostLevelIngestionStats.recordWriteComputeCacheHitCount();
      // construct currValue from this transient record only if it's not null.
      if (transientRecord.getValue() != null) {
        try {
          currValue = GenericRecordChunkingAdapter.INSTANCE.constructValue(
              transientRecord.getValue(),
              transientRecord.getValueOffset(),
              transientRecord.getValueLen(),
              storeDeserializerCache.getDeserializer(transientRecord.getValueSchemaId(), readerValueSchemaID),
              compressor.get());
        } catch (Exception e) {
          writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_DESERIALIZATION_FAILURE.code;
          throw e;
        }
        if (manifestContainer != null) {
          manifestContainer.setManifest(transientRecord.getValueManifest());
        }

      } else {
        currValue = null;
      }
    }
    return currValue;
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
  protected long getLatestLeaderPersistedOffsetAndHybridTopicOffset(
      String sourceRealTimeTopicKafkaURL,
      PubSubTopic leaderTopic,
      PartitionConsumptionState pcs,
      boolean shouldLog) {
    return getLatestLeaderOffsetAndHybridTopicOffset(
        sourceRealTimeTopicKafkaURL,
        leaderTopic,
        pcs,
        this::getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement,
        shouldLog);
  }

  private boolean isIngestingSystemStore() {
    return VeniceSystemStoreUtils.isSystemStore(storeName);
  }

  private long getLatestLeaderOffsetAndHybridTopicOffset(
      String sourceRealTimeTopicKafkaURL,
      PubSubTopic leaderTopic,
      PartitionConsumptionState pcs,
      BiFunction<PartitionConsumptionState, String, Long> getLeaderLatestOffset,
      boolean shouldLog) {
    int partition = pcs.getPartition();
    long latestLeaderOffset;
    int partitionToGetLatestOffsetFor;
    if (amplificationFactor > 1) {
      /*
       * When amplificationFactor enabled, the RT topic and VT topics have different number of partition.
       * eg. if amplificationFactor == 10 and partitionCount == 2, the RT topic will have 2 partitions and VT topics
       * will have 20 partitions.
       *
       * No 1-to-1 mapping between the RT topic and VT topics partition, we can not calculate the offset difference.
       * To measure the offset difference between 2 types of topics, we go through latestLeaderOffset in corresponding
       * sub-partitions and pick up the maximum value which means picking up the offset of the sub-partition seeing the
       * most recent records in RT, then use this value to compare against the offset in the RT topic.
       */
      partitionToGetLatestOffsetFor = PartitionUtils.getUserPartition(partition, amplificationFactor);
      IntList subPartitions = PartitionUtils.getSubPartitions(partitionToGetLatestOffsetFor, amplificationFactor);
      latestLeaderOffset = -1;
      long upstreamOffset;
      PartitionConsumptionState subPartitionConsumptionState;
      for (int i = 0; i < subPartitions.size(); i++) {
        subPartitionConsumptionState = partitionConsumptionStateMap.get(subPartitions.getInt(i));
        if (subPartitionConsumptionState != null && subPartitionConsumptionState.getOffsetRecord() != null) {
          upstreamOffset = getLeaderLatestOffset.apply(subPartitionConsumptionState, sourceRealTimeTopicKafkaURL);
          if (upstreamOffset > latestLeaderOffset) {
            latestLeaderOffset = upstreamOffset;
          }
        }
      }
    } else {
      partitionToGetLatestOffsetFor = partition;
      latestLeaderOffset = getLeaderLatestOffset.apply(pcs, sourceRealTimeTopicKafkaURL);
    }

    long offsetFromConsumer =
        getPartitionLatestOffset(sourceRealTimeTopicKafkaURL, leaderTopic, partitionToGetLatestOffsetFor);

    long lastOffsetInRealTimeTopic = offsetFromConsumer >= 0
        ? offsetFromConsumer
        : cachedKafkaMetadataGetter
            .getOffset(getTopicManager(sourceRealTimeTopicKafkaURL), leaderTopic, partitionToGetLatestOffsetFor);

    if (latestLeaderOffset == -1) {
      // If leader hasn't consumed anything yet we should use the value of 0 to calculate the exact offset lag.
      latestLeaderOffset = 0;
    }
    long lag = lastOffsetInRealTimeTopic - latestLeaderOffset;

    // Here we handle the case where the topic is actually empty,
    // if consumerLag is a positive number then that means there is an existing offset that we've consumed to and a
    // bigger offset out there somewhere. Meaning that there are at least two messages in the topic and it's not empty
    long consumerLag = getPartitionOffsetLag(sourceRealTimeTopicKafkaURL, leaderTopic, partitionToGetLatestOffsetFor);
    if (consumerLag <= 0) {
      // We don't have a positive consumer lag, but this could be because we haven't polled.
      // So as a final check to determine if the topic is empty, we check
      // if the end offset is the same as the beginning
      long earliestOffset = cachedKafkaMetadataGetter.getEarliestOffset(
          getTopicManager(sourceRealTimeTopicKafkaURL),
          new PubSubTopicPartitionImpl(leaderTopic, partitionToGetLatestOffsetFor));
      if (earliestOffset == lastOffsetInRealTimeTopic - 1) {
        lag = 0;
      }
    }

    if (shouldLog) {
      LOGGER.info(
          "{} partition {} RT lag offset for {} is: Latest RT offset [{}] - persisted offset [{}] = Lag [{}]",
          consumerTaskId,
          pcs.getPartition(),
          sourceRealTimeTopicKafkaURL,
          lastOffsetInRealTimeTopic,
          latestLeaderOffset,
          lag);
    }
    return lag;
  }

  @Override
  protected void processVersionSwapMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {

    // Iterate through list of views for the store and process the control message.
    for (VeniceViewWriter viewWriter: viewWriters.values()) {
      // TODO: at some point, we should do this on more or all control messages potentially as we add more view types
      viewWriter.processControlMessage(controlMessage, partition, partitionConsumptionState, this.versionNumber);
    }
  }

  protected LeaderProducerCallback createProducerCallback(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return new LeaderProducerCallback(
        this,
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
  }

  protected Lazy<VeniceWriter<byte[], byte[], byte[]>> getVeniceWriter() {
    return veniceWriter;
  }

  // test method
  protected void addPartitionConsumptionState(Integer partition, PartitionConsumptionState pcs) {
    partitionConsumptionStateMap.put(partition, pcs);
  }
}
