package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.venice.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Lazy;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
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
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.*;
import static com.linkedin.venice.writer.VeniceWriter.*;
import static java.util.concurrent.TimeUnit.*;


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
  private static final Logger logger = LogManager.getLogger(LeaderFollowerStoreIngestionTask.class);

  /**
   * The new leader will stay inactive (not switch to any new topic or produce anything) for
   * some time after seeing the last messages in version topic.
   */
  private final long newLeaderInactiveTime;

  private final StoreIngestionWriteComputeProcessor ingestionTaskWriteComputeHandler;

  private final boolean isNativeReplicationEnabled;
  private final String nativeReplicationSourceVersionTopicKafkaURL;

  private final VeniceWriterFactory veniceWriterFactory;

  /**
   * Leader must maintain producer DIV states separate from drainers, because leader is always ahead of drainer;
   * if leader and drainer share the same DIV validator, leader will pollute the data in shared DIV validator;
   * in other words, if leader shares the same DIV validator with drainer, leader will move the DIV info ahead of the
   * actual persisted data.
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

  protected final StorageEngineBackedCompressorFactory compressorFactory;

  protected final Map<Integer, String> kafkaClusterIdToUrlMap;

  protected final Map<String, Integer> kafkaClusterUrlToIdMap;

  private long dataRecoveryCompletionTimeLagThresholdInMs = 0;

  public LeaderFollowerStoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      StorageEngineBackedCompressorFactory compressorFactory) {
    super(builder,
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
     * We are going to apply fast leader failover for {@link com.linkedin.venice.common.VeniceSystemStoreType#META_STORE}
     * since it is time sensitive, and if the split-brain problem happens in prod, we could design a way to periodically
     * produce snapshot to the meta system store to make correction in the future.
     */
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
      newLeaderInactiveTime = serverConfig.getServerSystemStorePromotionToLeaderReplicaDelayMs();
    } else {
      newLeaderInactiveTime = serverConfig.getServerPromotionToLeaderReplicaDelayMs();
    }
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    this.ingestionTaskWriteComputeHandler = new StoreIngestionWriteComputeProcessor(storeName, schemaRepository, mergeRecordHelper);

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
          .get(serverConfig.getKafkaClusterAliasToIdMap()
              .get(version.getDataRecoveryVersionConfig().getDataRecoverySourceFabric()));
      if (nativeReplicationSourceVersionTopicKafkaURL == null) {
        throw new VeniceException("Unable to get data recovery source kafka url from the provided source fabric:"
            + version.getDataRecoveryVersionConfig().getDataRecoverySourceFabric());
      }
      isDataRecovery = true;
      if (isHybridMode()) {
        dataRecoveryCompletionTimeLagThresholdInMs = TopicManager.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN / 2;
        logger.info("Data recovery info for topic: " + getVersionTopic() + ", source kafka url: "
            + nativeReplicationSourceVersionTopicKafkaURL + ", time lag threshold for completion: "
            + dataRecoveryCompletionTimeLagThresholdInMs);
      }
    }
    logger.info("Native replication source version topic kafka url set to: "
        + nativeReplicationSourceVersionTopicKafkaURL + " for topic: " + getVersionTopic());

    this.compressorFactory = compressorFactory;

    this.veniceWriterFactory = builder.getVeniceWriterFactory();
    this.veniceWriter = Lazy.of(() -> {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
      if (storeVersionState.isPresent()) {
        return veniceWriterFactory.createBasicVeniceWriter(kafkaVersionTopic, storeVersionState.get().chunked, venicePartitioner,
            Optional.of(storeVersionPartitionCount * amplificationFactor));
      } else {
        /**
         * In general, a partition in version topic follows this pattern:
         * {Start_of_Segment, Start_of_Push, End_of_Segment, Start_of_Segment, data..., End_of_Segment, Start_of_Segment, End_of_Push, End_of_Segment}
         * Therefore, in native replication where leader needs to producer all messages it consumes from remote, the first
         * message that leader consumes is not SOP, in this case, leader doesn't know whether chunking is enabled.
         *
         * Notice that the pattern is different in stream reprocessing which contains a lot more segments and is also
         * different in some test cases which reuse the same VeniceWriter.
         */
        return veniceWriterFactory.createBasicVeniceWriter(kafkaVersionTopic, venicePartitioner, Optional.of(storeVersionPartitionCount * amplificationFactor));
      }
    });

    this.kafkaClusterIdToUrlMap = serverConfig.getKafkaClusterIdToUrlMap();
    this.kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();

    this.kafkaDataIntegrityValidatorForLeaders = new KafkaDataIntegrityValidator(kafkaVersionTopic);
  }

  @Override
  protected void closeProducers() {
    veniceWriter.ifPresent(VeniceWriter::close);
  }

  /**
   * Close a DIV segment for a version topic partition.
   */
  private void endSegment(int partition) {
    // If the VeniceWriter doesn't exist, then no need to end any segment, and this function becomes a no-op
    veniceWriter.ifPresent(vw -> vw.endSegment(partition, true));
  }

  @Override
  public synchronized void promoteToLeader(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    amplificationAdapter.promoteToLeader(topic, partitionId, checker);
  }

  @Override
  public synchronized void demoteToStandby(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    throwIfNotRunning();
    amplificationAdapter.demoteToStandby(topic, partitionId, checker);
  }

  @Override
  protected void processConsumerAction(ConsumerAction message) throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
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
          logger.info("State transition from STANDBY to LEADER is skipped for topic " + topic + " partition " + partition
              + ", because Helix has assigned another role to this replica.");
          return;
        }

        PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
          logger.info("State transition from STANDBY to LEADER is skipped for topic " + topic + " partition " + partition
              + ", because this replica is the leader already.");
          return;
        }
        Store store = storeRepository.getStoreOrThrow(storeName);
        if (store.isMigrationDuplicateStore()) {
          partitionConsumptionState.setLeaderFollowerState(PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER);
          logger.info(consumerTaskId + " for partition " + partition + " is paused transition from STANDBY to LEADER");
        } else {
          // Mark this partition in the middle of STANDBY to LEADER transition
          partitionConsumptionState.setLeaderFollowerState(IN_TRANSITION_FROM_STANDBY_TO_LEADER);

          logger.info(consumerTaskId + " for partition " + partition + " is in transition from STANDBY to LEADER");
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
          logger.info("State transition from LEADER to STANDBY is skipped for topic " + topic + " partition " + partition
              + ", because Helix has assigned another role to this replica.");
          return;
        }

        partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)) {
          logger.info("State transition from LEADER to STANDBY is skipped for topic " + topic + " partition " + partition
              + ", because this replica is a follower already.");
          return;
        }

        /**
         * 1. If leader(itself) was consuming from local VT previously, just set the state as STANDBY for this partition;
         * 2. otherwise, leader would unsubscribe from the previous feed topic (real-time topic, grandfathering
         *    transient topic or remote VT); then drain all the messages from the feed topic, which would produce the
         *    corresponding result message to local VT; block on the callback of the final message that it needs to
         *    produce; finally the new follower will switch back to consume from local VT using the latest VT offset
         *    tracked by producer callback.
         */
        OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
        String leaderTopic = offsetRecord.getLeaderTopic();
        if (leaderTopic != null && (!topic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely())) {
          consumerUnSubscribe(leaderTopic, partitionConsumptionState);
          waitForAllMessageToBeProcessedFromTopicPartition(leaderTopic, partition, partitionConsumptionState);

          partitionConsumptionState.setConsumeRemotely(false);
          logger.info(consumerTaskId + " disabled remote consumption from topic " + leaderTopic + " partition " + partition);
          // Followers always consume local VT and should not skip kafka message
          partitionConsumptionState.setSkipKafkaMessage(false);
          // subscribe back to local VT/partition
          consumerSubscribe(topic, partitionConsumptionState.getSourceTopicPartition(topic),
              partitionConsumptionState.getLatestProcessedVersionTopicOffset(), localKafkaServer);

          logger.info(consumerTaskId + " demoted to standby for partition " + partition);
        }
        /**
         * As a leader, the node does not update leader topic when processing TS. When demoted to follower, it should
         * track the topic that leader will consume. Otherwise, for hybrid stores, if follower does not change leader
         * topic to RT, it will never become online because hybrid lag measurement will return a large value.
         */
        TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
        if (topicSwitch != null && !topicSwitch.sourceTopicName.toString().equals(offsetRecord.getLeaderTopic())) {
          offsetRecord.setLeaderTopic(topicSwitch.sourceTopicName.toString());
        }
        partitionConsumptionStateMap.get(partition).setLeaderFollowerState(STANDBY);
        /**
         * Close the writer to make sure the current segment is closed after the leader is demoted to standby.
         */
        endSegment(partition);
        break;
      default:
        processCommonConsumerAction(operation, topic, partition, message.getLeaderState());
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
   * same code path between regular push job, hybrid store and grandfathering job.
   *
   * @throws InterruptedException
   */
  @Override
  protected void checkLongRunningTaskState() throws InterruptedException{
    boolean pushTimeout = false;
    Set<Integer> timeoutPartitions = null;
    long checkStartTimeInNS = System.nanoTime();
    for (PartitionConsumptionState partitionConsumptionState : partitionConsumptionStateMap.values()) {
      final int partition = partitionConsumptionState.getPartition();

      /**
       * Check whether the push timeout
       */
      if (!partitionConsumptionState.isComplete()
          && LatencyUtils.getElapsedTimeInMs(partitionConsumptionState.getConsumptionStartTimeInMs()) > this.bootstrapTimeoutInMs) {
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
            logger.info(consumerTaskId + " became in transition to leader for partition " + partitionConsumptionState.getPartition());
          }
          break;

        case IN_TRANSITION_FROM_STANDBY_TO_LEADER:
          /**
           * Potential risk: it's possible that Kafka consumer would starve one of the partitions for a long
           * time even though there are new messages in it, so it's possible that the old leader is still producing
           * after waiting for 5 minutes; if it does happen, followers will detect upstream offset rewind by
           * a different producer GUID.
           */
          long lastTimestamp = getLastConsumedMessageTimestamp(kafkaVersionTopic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
            logger.info(consumerTaskId + " start promoting to leader for partition " + partition
                + " unsubscribing from current topic: " + kafkaVersionTopic);
            /**
             * There isn't any new message from the old leader for at least {@link newLeaderInactiveTime} minutes,
             * this replica can finally be promoted to leader.
             */
            // unsubscribe from previous topic/partition
            consumerUnSubscribe(kafkaVersionTopic, partitionConsumptionState);

            logger.info(consumerTaskId + " start promoting to leader for partition " + partition
                + ", unsubscribed from current topic: " + kafkaVersionTopic);
            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            if (null == offsetRecord.getLeaderTopic()) {
              /**
               * If this follower has processed a TS, the leader topic field should have been set. So, it must have been
               * consuming from version topic. Now it is becoming the leader. So the VT becomes its leader topic.
               */
              offsetRecord.setLeaderTopic(kafkaVersionTopic);
            }

            /**
             * When leader promotion actually happens and before leader switch to a new topic, restore the latest DIV check
             * results from drainer service to the DIV check validator that will be used in leader consumption thread
             */
            restoreProducerStatesForLeaderConsumption(partition);

            if (!amplificationAdapter.isLeaderSubPartition(partition) && partitionConsumptionState.isEndOfPushReceived()) {
              partitionConsumptionState.setLeaderFollowerState(STANDBY);
              consumerSubscribe(kafkaVersionTopic, partitionConsumptionState.getSourceTopicPartition(kafkaVersionTopic),
                  partitionConsumptionState.getLatestProcessedVersionTopicOffset(), localKafkaServer);
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
          String currentLeaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
          if (null == currentLeaderTopic) {
            String errorMsg = consumerTaskId + " Missing leader topic for actual leader. OffsetRecord: "
                + partitionConsumptionState.getOffsetRecord().toSimplifiedString();
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
          }

          /**
           * If LEADER is consuming remote VT or SR, EOP is already received, switch back to local fabrics.
           * TODO: We do not need to switch back to local fabrics at all when native replication is completely ramped up
           *   and push job directly produces prod cluster instead of parent corp cluster.
           */
          if (shouldLeaderSwitchToLocalConsumption(partitionConsumptionState)) {
            // Unsubscribe from remote Kafka topic, but keep the consumer in cache.
            consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
            // If remote consumption flag is false, existing messages for the partition in the drainer queue should be processed before that
            waitForAllMessageToBeProcessedFromTopicPartition(currentLeaderTopic, partitionConsumptionState.getPartition(), partitionConsumptionState);

            partitionConsumptionState.setConsumeRemotely(false);
            logger.info(consumerTaskId + " disabled remote consumption from topic " + currentLeaderTopic + " partition " + partition);
            /**
             * The flag is turned on in {@link LeaderFollowerStoreIngestionTask#shouldProcessRecord} avoid consuming
             * unwanted messages after EOP in remote VT, such as SOBR. Now that the leader switches to consume locally,
             * it should not skip any message.
             */
            partitionConsumptionState.setSkipKafkaMessage(false);
            // Subscribe to local Kafka topic
            consumerSubscribe(
                    currentLeaderTopic,
                    partitionConsumptionState.getSourceTopicPartition(currentLeaderTopic),
                    partitionConsumptionState.getLatestProcessedVersionTopicOffset(),
                    localKafkaServer
            );
          }

          if (!amplificationAdapter.isLeaderSubPartition(partition) && partitionConsumptionState.isEndOfPushReceived()) {
            consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);
            partitionConsumptionState.setConsumeRemotely(false);
            partitionConsumptionState.setLeaderFollowerState(STANDBY);
            consumerSubscribe(kafkaVersionTopic, partitionConsumptionState.getSourceTopicPartition(kafkaVersionTopic), partitionConsumptionState.getLatestProcessedVersionTopicOffset(), localKafkaServer);
            break;
          }

          TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
          if (null == topicSwitch || currentLeaderTopic.equals(topicSwitch.sourceTopicName.toString())) {
            break;
          }

          /**
           * Otherwise, execute the TopicSwitch message stored in metadata store if one of the below conditions is true:
           * 1. it has been 5 minutes since the last update in the current topic
           * 2. leader is consuming SR topic right now and TS wants leader to switch to another topic.
           */
          lastTimestamp = getLastConsumedMessageTimestamp(currentLeaderTopic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime || switchAwayFromStreamReprocessingTopic(currentLeaderTopic, topicSwitch)) {
            leaderExecuteTopicSwitch(partitionConsumptionState, topicSwitch);
          }
          break;

        case STANDBY:
          // no long running task for follower
          break;
      }
    }
    if (emitMetrics.get()) {
      storeIngestionStats.recordCheckLongRunningTasksLatency(storeName, LatencyUtils.getLatencyInMS(checkStartTimeInNS));
    }

    if (pushTimeout) {
      // Timeout
      String errorMsg =
          "After waiting " + TimeUnit.MILLISECONDS.toHours(this.bootstrapTimeoutInMs) + " hours, resource:" + storeName + " partitions:"
              + timeoutPartitions + " still can not complete ingestion.";
      logger.error(errorMsg);
      throw new VeniceTimeoutException(errorMsg);
    }
  }

  protected void startConsumingAsLeaderInTransitionFromStandby(PartitionConsumptionState partitionConsumptionState) {
    if (partitionConsumptionState.getLeaderFollowerState() != IN_TRANSITION_FROM_STANDBY_TO_LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s", IN_TRANSITION_FROM_STANDBY_TO_LEADER, partitionConsumptionState));
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
      if (null == nativeReplicationSourceVersionTopicKafkaURL || nativeReplicationSourceVersionTopicKafkaURL.isEmpty()) {
        throw new VeniceException("Native replication is enabled but remote source address is not found");
      }
      if (shouldNewLeaderSwitchToRemoteConsumption(partitionConsumptionState)) {
        partitionConsumptionState.setConsumeRemotely(true);
        logger.info(consumerTaskId + " enabled remote consumption from topic " + offsetRecord.getLeaderTopic() + " partition " + partition);
      }
    }

    Set<String> leaderSourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    if (leaderSourceKafkaURLs.size() != 1) {
      throw new VeniceException("In L/F mode, expect only one leader source Kafka URL. Got: " + leaderSourceKafkaURLs);
    }

    partitionConsumptionState.setLeaderFollowerState(LEADER);
    final String leaderTopic = offsetRecord.getLeaderTopic();
    final long leaderStartOffset = partitionConsumptionState.getLeaderOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
    logger.info(String.format("%s is promoted to leader for partition %d and it is going to start consuming from " +
            "topic %s at offset %d",
        consumerTaskId, partition, leaderTopic, leaderStartOffset));

    // subscribe to the new upstream
    String leaderSourceKafkaURL = leaderSourceKafkaURLs.iterator().next();
    consumerSubscribe(
        leaderTopic,
        partitionConsumptionState.getSourceTopicPartition(leaderTopic),
        leaderStartOffset,
        leaderSourceKafkaURL
    );

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState, Collections.singletonMap(leaderSourceKafkaURL, leaderStartOffset));

    logger.info(String.format("%s, as a leader, started consuming from topic %s partition %d at offset %d",
        consumerTaskId, offsetRecord.getLeaderTopic(), partition, leaderStartOffset));
  }

  private boolean switchAwayFromStreamReprocessingTopic(String currentLeaderTopic, TopicSwitch topicSwitch) {
    return Version.isStreamReprocessingTopic(currentLeaderTopic) && !Version.isStreamReprocessingTopic(topicSwitch.sourceTopicName.toString());
  }

  protected void leaderExecuteTopicSwitch(PartitionConsumptionState partitionConsumptionState, TopicSwitch topicSwitch) {
    if (partitionConsumptionState.getLeaderFollowerState() != LEADER) {
      throw new VeniceException(String.format("Expect state %s but got %s",
          LEADER, partitionConsumptionState.toString()
      ));
    }
    if (topicSwitch.sourceKafkaServers.size() != 1) {
      throw new VeniceException("In the L/F mode, expect only one source Kafka URL in Topic Switch control message. " +
              "But got: " + topicSwitch.sourceKafkaServers);
    }

    // leader switch local or remote topic, depending on the sourceKafkaServers specified in TS
    final int partition = partitionConsumptionState.getPartition();
    final String currentLeaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
    final String newSourceKafkaServer = topicSwitch.sourceKafkaServers.get(0).toString();
    final String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    long upstreamStartOffset = partitionConsumptionState.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);

    if (upstreamStartOffset < 0) {
      if (topicSwitch.rewindStartTimestamp > 0) {
        upstreamStartOffset = getTopicPartitionOffsetByKafkaURL(
                newSourceKafkaServer,
                newSourceTopicName,
                partitionConsumptionState.getSourceTopicPartition(newSourceTopicName),
                topicSwitch.rewindStartTimestamp
        );
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
            currentLeaderTopic, newSourceTopicName, partition)
    );

    // subscribe to the new upstream
    if (isNativeReplicationEnabled && !newSourceKafkaServer.equals(localKafkaServer)) {
      partitionConsumptionState.setConsumeRemotely(true);
      logger.info(consumerTaskId + " enabled remote consumption from topic " + newSourceTopicName + " partition " + partitionConsumptionState.getSourceTopicPartition(newSourceTopicName));
    }
    partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopicName);
    partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(
        OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY,
        upstreamStartOffset
    );

    Set<String> sourceKafkaURLs = getConsumptionSourceKafkaAddress(partitionConsumptionState);
    if (sourceKafkaURLs.size() != 1) {
      throw new VeniceException("In L/F mode, expect only one leader source Kafka URL. Got: " + sourceKafkaURLs);
    }
    String sourceKafkaURL = sourceKafkaURLs.iterator().next();
    consumerSubscribe(
            newSourceTopicName,
            partitionConsumptionState.getSourceTopicPartition(newSourceTopicName),
            upstreamStartOffset,
            sourceKafkaURL
    );

    syncConsumedUpstreamRTOffsetMapIfNeeded(partitionConsumptionState,Collections.singletonMap(sourceKafkaURL, upstreamStartOffset));

    logger.info(consumerTaskId + " leader successfully switch feed topic from " + currentLeaderTopic + " to "
        + newSourceTopicName + " offset " + upstreamStartOffset + " partition " + partition);

    // In case new topic is empty and leader can never become online
    defaultReadyToServeChecker.apply(partitionConsumptionState);
  }

  protected void syncConsumedUpstreamRTOffsetMapIfNeeded(PartitionConsumptionState pcs, Map<String, Long> upstreamStartOffsetByKafkaURL) {
    // Update in-memory consumedUpstreamRTOffsetMap in case no RT record is consumed after the subscription
    final String leaderTopic = pcs.getOffsetRecord().getLeaderTopic();
    if (leaderTopic != null && Version.isRealTimeTopic(leaderTopic)) {
      upstreamStartOffsetByKafkaURL.forEach((kafkaURL, upstreamStartOffset) -> {
        if (upstreamStartOffset >
            getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(pcs, kafkaURL)) {
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
      logger.error(errorMsg, e);
      versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
      reportStatusAdapter.reportError(Collections.singletonList(partitionConsumptionState), errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  protected long getTopicPartitionOffsetByKafkaURL(
          CharSequence kafkaURL,
          String topicName,
          int topicPartition,
          long rewindStartTimestamp
  ) {
    long topicPartitionOffset = getTopicManager(kafkaURL.toString()).getPartitionOffsetByTime(topicName, topicPartition, rewindStartTimestamp);
    /**
     * {@link com.linkedin.venice.kafka.TopicManager#getPartitionOffsetByTime} will always return the next offset
     * to consume, but {@link com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer#subscribe} is always
     * seeking the next offset, so we will deduct 1 from the returned offset here.
     */
    return topicPartitionOffset - 1;
  }

  @Override
  protected Set<String> getConsumptionSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    if (partitionConsumptionState.consumeRemotely()) {
      if (Version.isRealTimeTopic(partitionConsumptionState.getOffsetRecord().getLeaderTopic())) {
        Set<String> realTimeDataSourceKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
        if (!realTimeDataSourceKafkaURLs.isEmpty()) {
          return realTimeDataSourceKafkaURLs;
        } else {
          throw new VeniceException("Expect RT Kafka URL when leader topic is a real-time topic. Got: " + partitionConsumptionState);
        }
      } else {
        return Collections.singleton(nativeReplicationSourceVersionTopicKafkaURL);
      }
    }
    return Collections.singleton(localKafkaServer);
  }

  @Override
  protected Set<String> getRealTimeDataSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    if (!isNativeReplicationEnabled) {
      return Collections.singleton(localKafkaServer);
    }
    TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
    if (topicSwitch == null || topicSwitch.sourceKafkaServers == null || topicSwitch.sourceKafkaServers.isEmpty()) {
      return Collections.emptySet();
    }
    return topicSwitch.sourceKafkaServers.stream().map(CharSequence::toString).collect(Collectors.toSet());
  }

  /**
   * This method get the timestamp of the "last" message in topic/partition; notice that when the function
   * returns, new messages can be appended to the partition already, so it's not guaranteed that this timestamp
   * is from the last message.
   */
  private long getLastConsumedMessageTimestamp(String topic, int partition) throws InterruptedException {

    /**
     * Ingestion thread would update the last consumed message timestamp for the corresponding partition.
     */
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    long lastConsumedMessageTimestamp = partitionConsumptionState.getLatestMessageConsumptionTimestampInMs();

    return lastConsumedMessageTimestamp;
  }

  protected boolean shouldNewLeaderSwitchToRemoteConsumption(PartitionConsumptionState partitionConsumptionState) {
    return isConsumingFromRemoteVersionTopic(partitionConsumptionState) || isLeaderConsumingRemoteRealTimeTopic(partitionConsumptionState);
  }

  private boolean isConsumingFromRemoteVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    return !partitionConsumptionState.isEndOfPushReceived()
            && !isCurrentVersion.getAsBoolean()
            // Do not enable remote consumption for the source fabric leader. Otherwise, it will produce extra messages.
            && !Objects.equals(nativeReplicationSourceVersionTopicKafkaURL, localKafkaServer);
  }

  private boolean isLeaderConsumingRemoteRealTimeTopic(PartitionConsumptionState partitionConsumptionState) {
    if (!Version.isRealTimeTopic(partitionConsumptionState.getOffsetRecord().getLeaderTopic())) {
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
   * Incremental push is enabled, the policy is PUSH_TO_VERSION_TOPIC, write compute is disabled. OR the version is
   * hybrid or incremental push enabled and data recovery is in progress.
   */
  private boolean shouldLeaderSwitchToLocalConsumption(PartitionConsumptionState partitionConsumptionState)
      throws InterruptedException {
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
    return partitionConsumptionState.consumeRemotely()
        && partitionConsumptionState.isEndOfPushReceived()
        && Version.isVersionTopicOrStreamReprocessingTopic(partitionConsumptionState.getOffsetRecord().getLeaderTopic())
        && (!isDataRecovery || partitionConsumptionState.isDataRecoveryCompleted())
        && !(partitionConsumptionState.isIncrementalPushEnabled() && partitionConsumptionState.getIncrementalPushPolicy()
            .equals(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC) && !isWriteComputationEnabled);
  }

  private void checkAndUpdateDataRecoveryStatusOfHybridStore(PartitionConsumptionState partitionConsumptionState)
      throws InterruptedException {
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
      if (amplificationAdapter != null) {
        latestConsumedProducerTimestamp = getLatestConsumedProducerTimestampWithSubPartition(latestConsumedProducerTimestamp,
            partitionConsumptionState);
      }
      if (LatencyUtils.getElapsedTimeInMs(latestConsumedProducerTimestamp) < dataRecoveryCompletionTimeLagThresholdInMs) {
        logger.info("Data recovery completed for topic: " + kafkaVersionTopic + " partition: "
            + partitionConsumptionState.getPartition() + " upon consuming records with producer timestamp of "
            + latestConsumedProducerTimestamp + " which is within the data recovery completion lag threshold of "
            + dataRecoveryCompletionTimeLagThresholdInMs + " ms");
        isDataRecoveryCompleted = true;
      }
    }
    if (!isDataRecoveryCompleted) {
      long dataRecoverySourceVTEndOffset = cachedKafkaMetadataGetter
          .getOffset(topicManagerRepository.getTopicManager(nativeReplicationSourceVersionTopicKafkaURL), kafkaVersionTopic,
              partitionConsumptionState.getPartition());

      // If the last few records in source VT is old then we can also complete data recovery if the leader idles and we
      // have reached/passed the end offset of the VT (around the time when ingestion is started for that partition).
      long localVTOffsetProgress = partitionConsumptionState.getLatestProcessedVersionTopicOffset();
      // -2 because getOffset returns the next available offset (-1) and we are skipping the remote TS message (-1).
      boolean isAtEndOfSourceVT = dataRecoverySourceVTEndOffset - 2 <= localVTOffsetProgress;
      long lastTimestamp = getLastConsumedMessageTimestamp(kafkaVersionTopic, partitionConsumptionState.getPartition());
      if (isAtEndOfSourceVT && LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
        logger.info("Data recovery completed for topic: " + kafkaVersionTopic + " partition: "
            + partitionConsumptionState.getPartition() + " upon exceeding leader inactive time of "
            + newLeaderInactiveTime + " ms");
        isDataRecoveryCompleted = true;
      }
    }

    if (isDataRecoveryCompleted) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      reportStatusAdapter.reportDataRecoveryCompleted(partitionConsumptionState);
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
    String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
    return (!kafkaVersionTopic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely());
  }

  protected boolean isLeader(PartitionConsumptionState partitionConsumptionState) {
    return Objects.equals(partitionConsumptionState.getLeaderFollowerState(), LEADER);
  }

  @Override
  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
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
      throw new VeniceException("More than one Kafka server urls in TopicSwitch control message, "
          + "TopicSwitch.sourceKafkaServers: " + kafkaServerUrls);
    }
    reportStatusAdapter.reportTopicSwitchReceived(partitionConsumptionState);
    String sourceKafkaURL = kafkaServerUrls.get(0).toString();

    // Venice servers calculate the start offset based on start timestamp
    String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    long upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
    // Since DaVinci clients might not have network ACLs to remote RT, they will skip upstream start offset calculation.
    if (!isDaVinciClient && topicSwitch.rewindStartTimestamp > 0) {
      int newSourceTopicPartition = partitionConsumptionState.getSourceTopicPartition(newSourceTopicName);
      upstreamStartOffset = getTopicManager(sourceKafkaURL).getPartitionOffsetByTime(newSourceTopicName, newSourceTopicPartition, topicSwitch.rewindStartTimestamp);
      if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
        upstreamStartOffset -= 1;
      }
    }

    syncTopicSwitchToIngestionMetadataService(
        topicSwitch,
        partitionConsumptionState,
        Collections.singletonMap(sourceKafkaURL, upstreamStartOffset)
    );

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
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(
          OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY,
          upstreamStartOffset
      );
    } else {
      /**
       * For follower, just keep track of what leader is doing now.
       */
      partitionConsumptionState.getOffsetRecord().setLeaderTopic(newSourceTopicName);
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(
          OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY,
          upstreamStartOffset
      );

      /**
       * We need to measure offset lag here for follower; if real-time topic is empty and never gets any new message,
       * follower replica will never become online.
       *
       * If we measure lag here for follower, follower might become online faster than leader in extreme case:
       * Real time topic for that partition is empty or the rewind start offset is very closed to the end, followers
       * calculate the lag of the leader and decides the lag is small enough.
       */
      this.defaultReadyToServeChecker.apply(partitionConsumptionState);
    }
  }

  protected void syncTopicSwitchToIngestionMetadataService(
      TopicSwitch topicSwitch,
      PartitionConsumptionState partitionConsumptionState,
      Map<String, Long> upstreamStartOffsetByKafkaURL
  ) {
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (storeVersionState.isPresent()) {
      String newTopicSwitchLogging = "TopicSwitch message (new source topic:" + topicSwitch.sourceTopicName
          + "; rewind start time:" + topicSwitch.rewindStartTimestamp + "; upstream start offset by source Kafka URL: "
          + upstreamStartOffsetByKafkaURL + ")";

      if (null == storeVersionState.get().topicSwitch) {
        logger.info("First time receiving a " + newTopicSwitchLogging);
      } else {
        logger.info("Previous TopicSwitch message in metadata store (source topic:" + storeVersionState.get().topicSwitch.sourceTopicName
            + "; rewind start time:" + storeVersionState.get().topicSwitch.rewindStartTimestamp + "; source kafka servers "
            + topicSwitch.sourceKafkaServers + ") will be replaced" + " by the new " + newTopicSwitchLogging);
      }
      storeVersionState.get().topicSwitch = topicSwitch;
      // Sync latest store version level metadata to disk
      storageMetadataService.put(kafkaVersionTopic, storeVersionState.get());

      // Put TopicSwitch message into in-memory state to avoid poking metadata store
      partitionConsumptionState.setTopicSwitch(topicSwitch);
    } else {
      throw new VeniceException("Unexpected: received some " + ControlMessageType.TOPIC_SWITCH.name() +
          " control message in a topic where we have not yet received a " +
          ControlMessageType.START_OF_PUSH.name() + " control message.");
    }
  }

  /**
   * A helper function to update the checkpoint offsets in {@link OffsetRecord} or the latest in-memory offsets processed
   * by drainers in {@link PartitionConsumptionState}.
   *
   * When using this helper function to update the checkpoint offsets in {@link OffsetRecord}:
   * "updateVersionTopicOffsetFunction" should try to update the VT offset in {@link OffsetRecord}
   * "updateRealtimeTopicOffsetFunction" should try to update the upstream offset map in {@link OffsetRecord}
   *
   * When using this helper function to update the latest in-memory offsets processed by drainers in {@link PartitionConsumptionState}:
   * "updateVersionTopicOffsetFunction" should try to update the VT offset in {@link PartitionConsumptionState}
   * "updateRealtimeTopicOffsetFunction" should try to update the latest processed upstream offset map in {@link PartitionConsumptionState}
   *
   * In LeaderFollowerStoreIngestionTask, "sourceKafkaUrlSupplier" should always return {@link OffsetRecord#NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY};
   * in ActiveActiveStoreIngestionTask, "sourceKafkaUrlSupplier" should return the actual source Kafka url of the "consumerRecordWrapper"
   */
  protected void updateOffsets(PartitionConsumptionState partitionConsumptionState,
                               VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
                               LeaderProducedRecordContext leaderProducedRecordContext,
                               UpdateVersionTopicOffset updateVersionTopicOffsetFunction,
                               UpdateRealtimeTopicOffset updateRealtimeTopicOffsetFunction,
                               GetLastKnownRealtimeTopicOffset lastKnownRealtimeTopicOffsetSupplier,
                               Supplier<String> sourceKafkaUrlSupplier) {

    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    // Only update the metadata if this replica should NOT produce to version topic.
    if (!shouldProduceToVersionTopic(partitionConsumptionState)) {
      /**
       * If either (1) this is a follower replica or (2) this is a leader replica who is consuming from version topic
       * in a local Kafka cluster, we can update the offset metadata in offset record right after consuming a message;
       * otherwise, if the leader is consuming from real-time topic or grandfathering topic, it should update offset
       * metadata after successfully produce a corresponding message.
       */
      KafkaMessageEnvelope kafkaValue = consumerRecord.value();
      updateVersionTopicOffsetFunction.apply(consumerRecord.offset());

      // DaVinci clients don't need to maintain leader production states
      if (!isDaVinciClient) {
        // also update the leader topic offset using the upstream offset in ProducerMetadata
        if (kafkaValue.producerMetadata.upstreamOffset >= 0 || (kafkaValue.leaderMetadataFooter != null
            && kafkaValue.leaderMetadataFooter.upstreamOffset >= 0)) {
          final String sourceKafkaUrl = sourceKafkaUrlSupplier.get();
          final long newUpstreamOffset =
              kafkaValue.leaderMetadataFooter == null ? kafkaValue.producerMetadata.upstreamOffset : kafkaValue.leaderMetadataFooter.upstreamOffset;

          final long previousUpstreamOffset = lastKnownRealtimeTopicOffsetSupplier.apply(sourceKafkaUrl);
          checkAndHandleUpstreamOffsetRewind(partitionConsumptionState, partitionConsumptionState.getOffsetRecord(), consumerRecordWrapper.consumerRecord(), newUpstreamOffset, previousUpstreamOffset);
          /**
           * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
           * to the true leader when the old leader doesn't stop producing.
           */
          updateRealtimeTopicOffsetFunction.apply(sourceKafkaUrl, newUpstreamOffset);
        }
        // update leader producer GUID
        partitionConsumptionState.getOffsetRecord().setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
        if (kafkaValue.leaderMetadataFooter != null) {
          partitionConsumptionState.getOffsetRecord().setLeaderHostId(kafkaValue.leaderMetadataFooter.hostName.toString());
        }
      }
    } else {
      updateOffsetsAsRemoteConsumeLeader(leaderProducedRecordContext, sourceKafkaUrlSupplier.get(),
          consumerRecord, updateVersionTopicOffsetFunction, updateRealtimeTopicOffsetFunction);
    }
  }

  @Override
  protected void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {
    updateOffsets(partitionConsumptionState,
                  consumerRecordWrapper,
                  leaderProducedRecordContext,
                  (versionTopicOffset)
                      -> offsetRecord.setCheckpointLocalVersionTopicOffset(versionTopicOffset),
                  (sourceKafkaUrl, realtimeTopicOffset)
                      -> offsetRecord.setLeaderUpstreamOffset(sourceKafkaUrl, realtimeTopicOffset),
                  (sourceKafkaUrl)
                      -> offsetRecord.getUpstreamOffset(sourceKafkaUrl),
                  () -> OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  private void updateOffsetsAsRemoteConsumeLeader(
      LeaderProducedRecordContext leaderProducedRecordContext,
      String upstreamKafkaURL,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      UpdateVersionTopicOffset updateVersionTopicOffsetFunction,
      UpdateRealtimeTopicOffset updateRealtimeTopicOffsetFunction
  ) {
    // Leader will only update the offset from leaderProducedRecordContext in VT.
    if (leaderProducedRecordContext != null) {
      /**
       * producedOffset and consumedOffset both are set to -1 when producing individual chunks
       * see {@link LeaderProducerMessageCallback#onCompletion(RecordMetadata, Exception)}
       */
      if (leaderProducedRecordContext.getProducedOffset() >= 0) {
        updateVersionTopicOffsetFunction.apply(leaderProducedRecordContext.getProducedOffset());
      }

      if (leaderProducedRecordContext.getConsumedOffset() >= 0) {
        updateRealtimeTopicOffsetFunction.apply(upstreamKafkaURL, leaderProducedRecordContext.getConsumedOffset());
      }
    } else {
      //Ideally this should never happen.
      String msg = consumerTaskId + " UpdateOffset: Produced record should not be null in LEADER. topic: " + consumerRecord.topic() + " Partition "
          + consumerRecord.partition();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.warn(msg);
      }
    }
  }

  @Override
  protected void updateLatestInMemoryProcessedOffset(PartitionConsumptionState partitionConsumptionState,
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, LeaderProducedRecordContext leaderProducedRecordContext) {
    updateOffsets(partitionConsumptionState,
                  consumerRecordWrapper,
                  leaderProducedRecordContext,
                  (versionTopicOffset)
                      -> partitionConsumptionState.updateLatestProcessedVersionTopicOffset(versionTopicOffset),
                  (sourceKafkaUrl, realtimeTopicOffset)
                      -> partitionConsumptionState.updateLatestProcessedUpstreamRTOffset(sourceKafkaUrl, realtimeTopicOffset),
                  (sourceKafkaUrl)
                      -> partitionConsumptionState.getLatestProcessedUpstreamRTOffset(sourceKafkaUrl),
                  () -> OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  protected void checkAndHandleUpstreamOffsetRewind(
      PartitionConsumptionState partitionConsumptionState,
      OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      final long newUpstreamOffset,
      final long previousUpstreamOffset
  ) {
    if (newUpstreamOffset >= previousUpstreamOffset) {
      return; // Rewind did not happen
    }

    /**
     * If upstream offset is rewound and it's from a different producer, we encounter a split-brain
     * issue (multiple leaders producing to the same partition at the same time)
     *
     * The condition is a little messy here. This is due to the fact that we have 2 mechanisms to detect the issue.
     * 1. (old) we identify a Venice writer by checking message's GUID.
     * 2. We identify a Venice writer by checking message's "leaderMetadataFooter.hostName".
     *
     * We would need the second mechanism because once "pass-through" message reproducing is enabled (and it's the
     * enabled by default in latest code base), leader will re-use the same GUID as the one that's passed from the
     * upstream message.
     *
     * TODO:Remove old condition check once every SN is bumped to have "pass-through" mode enabled.
     */
    final KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    if ((offsetRecord.getLeaderGUID() != null && !kafkaValue.producerMetadata.producerGUID.equals(offsetRecord.getLeaderGUID()))
        || (kafkaValue.leaderMetadataFooter != null && offsetRecord.getLeaderHostId() != null
        && !kafkaValue.leaderMetadataFooter.hostName.toString().equals(offsetRecord.getLeaderHostId()))) {
      /**
       * Check whether the data inside rewind message is the same the data inside storage engine; if so,
       * we don't consider it as lossy rewind; otherwise, report potentially lossy upstream rewind.
       *
       * Fail the job if it's lossy and it's during the GF job (before END_OF_PUSH received);
       * otherwise, don't fail the push job, it's streaming ingestion now so it's serving online traffic already.
       */
      String logMsg = String.format(consumerTaskId + " partition %d received message with upstreamOffset: %d;"
              + " but recorded upstreamOffset is: %d. Recorded producer GUID: %s; Received message producer GUID: %s; "
              + "Received message producer host: %s. Multiple leaders are producing. ",
          consumerRecord.partition(),
          newUpstreamOffset,
          previousUpstreamOffset,
          GuidUtils.getHexFromGuid(offsetRecord.getLeaderGUID()),
          GuidUtils.getHexFromGuid(kafkaValue.producerMetadata.producerGUID),
          kafkaValue.leaderMetadataFooter == null ? "unknown" : kafkaValue.leaderMetadataFooter.hostName.toString()
      );

      boolean lossy = true;
      try {
        KafkaKey key = consumerRecord.key();
        KafkaMessageEnvelope envelope = consumerRecord.value();
        AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
        switch (MessageType.valueOf(envelope)) {
          case PUT:
            // Issue an read to get the current value of the key
            byte[] actualValue = storageEngine.get(consumerRecord.partition(), key.getKey(), true);
            if (actualValue != null) {
              int actualSchemaId = ByteUtils.readInt(actualValue, 0);
              Put put = (Put) envelope.payloadUnion;
              if (actualSchemaId == put.schemaId) {
                // continue if schema Id is the same
                if (ByteUtils.equals(put.putValue.array(), put.putValue.position(), actualValue,
                    ValueRecord.SCHEMA_HEADER_LENGTH)) {
                  lossy = false;
                  logMsg += Utils.NEW_LINE_CHAR;
                  logMsg += "But this rewound PUT is not lossy because the data in the rewind message is the same as the data inside Venice";
                }
              }
            }
            break;
          case DELETE:
            /**
             * Lossy if the key/value pair is added back to the storage engine after the first DELETE message.
             */
            actualValue = storageEngine.get(consumerRecord.partition(), key.getKey(), true);
            if (actualValue == null) {
              lossy = false;
              logMsg += Utils.NEW_LINE_CHAR;
              logMsg += "But this rewound DELETE is not lossy because the data in the rewind message is deleted already";
            }
            break;
          default:
            // Consider lossy for both control message and PartialUpdate
            break;
        }
      } catch (Exception e) {
        logger.warn(consumerTaskId + " failed comparing the rewind message with the actual value in Venice", e);
      }

      if (lossy) {
        if (!partitionConsumptionState.isEndOfPushReceived()) {
          logMsg += Utils.NEW_LINE_CHAR;
          logMsg += "Failing the job because lossy rewind happens before receiving EndOfPush";
          logger.error(logMsg);
          versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
          VeniceException e = new VeniceException(logMsg);
          reportStatusAdapter.reportError(Collections.singletonList(partitionConsumptionState), logMsg, e);
          throw e;
        } else {
          logMsg += Utils.NEW_LINE_CHAR;
          logMsg += "Don't fail the job during streaming ingestion";
          logger.error(logMsg);
          versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
        }
      } else {
        logger.info(logMsg);
        versionedDIVStats.recordBenignLeaderOffsetRewind(storeName, versionNumber);
      }
    }
  }

  protected void produceToLocalKafka(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState, LeaderProducedRecordContext leaderProducedRecordContext, ProduceToTopic produceFunction) {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    int partition = consumerRecord.partition();
    String leaderTopic = consumerRecord.topic();
    long sourceTopicOffset = consumerRecord.offset();
    int sourceKafkaClusterId = kafkaClusterUrlToIdMap.getOrDefault(consumerRecordWrapper.kafkaUrl(), -1);
    LeaderMetadataWrapper leaderMetadataWrapper = new LeaderMetadataWrapper(sourceTopicOffset, sourceKafkaClusterId);
    LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(this, consumerRecordWrapper, partitionConsumptionState, leaderTopic,
        kafkaVersionTopic, partition, versionedDIVStats, logger, leaderProducedRecordContext, System.nanoTime());
    partitionConsumptionState.setLastLeaderPersistFuture(leaderProducedRecordContext.getPersistedToDBFuture());
    produceFunction.apply(callback, leaderMetadataWrapper);
  }

  @Override
  protected boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState) {
    TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
    if (topicSwitch == null) {
      return false;
    }
    if (topicSwitch.sourceKafkaServers.size() != 1) {
      throw new VeniceException("Expect only one source Kafka URLs in Topic Switch. Got: " + topicSwitch.sourceKafkaServers);
    }
    return Version.isRealTimeTopic(topicSwitch.sourceTopicName.toString());
  }

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
    String leaderTopic = offsetRecord.getLeaderTopic();
    if (null == leaderTopic || !Version.isRealTimeTopic(leaderTopic)) {
      /**
       * 1. Usually there is a batch-push or empty push for the hybrid store before replaying messages from real-time
       *    topic; since we need to wait for at least 5 minutes of inactivity since the last successful consumed message
       *    before promoting a replica to leader, the leader topic metadata may not be initialized yet (the first time
       *    when we initialize the leader topic is either when a replica is promoted to leader successfully or encounter
       *    TopicSwitch control message.), so leader topic can be null during the 5 minutes inactivity.
       * 2. It's also possible that the replica is promoted to leader already but haven't received the TopicSwitch
       *    command from controllers to start consuming from real-time topic (for example, grandfathering Samza job has
       *    finished producing the batch input to the transient grandfathering topic, but user haven't sent END_OF_PUSH
       *    so controllers haven't sent TopicSwitch).
       */
      return Long.MAX_VALUE;
    }

    // Since DaVinci clients run in follower only mode, use local VT to compute hybrid lag.
    if (isDaVinciClient) {
      return cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), kafkaVersionTopic, partition)
          - partitionConsumptionState.getLatestProcessedVersionTopicOffset();
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

  protected long measureRTOffsetLagForSingleRegion(String sourceRealTimeTopicKafkaURL,
      PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    Pair<Long, Long> hybridLagPair =
        amplificationAdapter.getLatestLeaderPersistedOffsetAndHybridTopicOffset(sourceRealTimeTopicKafkaURL,
            partitionConsumptionState.getOffsetRecord().getLeaderTopic(), partitionConsumptionState);
    long latestLeaderOffset = hybridLagPair.getFirst();
    long lastOffsetInRealTimeTopic = hybridLagPair.getSecond();
    long lag = lastOffsetInRealTimeTopic - latestLeaderOffset;
    if (shouldLogLag) {
      logger.info(String.format("%s partition %d real-time buffer lag offset for region: %s is: "
              + "(Last RT offset [%d] - Last leader consumed offset [%d]) = Lag [%d]", consumerTaskId,
          partitionConsumptionState.getPartition(), sourceRealTimeTopicKafkaURL, lastOffsetInRealTimeTopic,
          latestLeaderOffset, lag));
    }
    return lag;
  }

  protected long measureRTOffsetLagForMultiRegions(Set<String> sourceRealTimeTopicKafkaURLs,
      PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag) {
    throw new VeniceException(String.format("%s Multi colo RT offset lag calculation is not supported for non Active-Active stores", consumerTaskId));
  }

  @Override
  protected void reportIfCatchUpBaseTopicOffset(PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();

    if (pcs.isEndOfPushReceived() && !pcs.isLatchReleased()) {
      if (cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), kafkaVersionTopic, partition) - 1 <= pcs.getLatestProcessedVersionTopicOffset()) {
        reportStatusAdapter.reportCatchUpBaseTopicOffsetLag(pcs);

        /**
         * Relax to report completion
         *
         * There is a safe guard latch that is optionally replaced during Offline to Follower
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
          reportStatusAdapter.reportCompleted(pcs, true);
        }
      }
    }
  }

  /**
   * For Leader/Follower model, the follower should have the same kind of check as the Online/Offline model;
   * for leader, it's possible that it consumers from real-time topic or GF topic.
   */
  @Override
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    int subPartition = PartitionUtils.getSubPartition(record.topic(), record.partition(), amplificationFactor);
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
    if (null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + kafkaVersionTopic + " Partition Id: " + subPartition);
      return false;
    }
    switch (partitionConsumptionState.getLeaderFollowerState()) {
      case LEADER:
        if (partitionConsumptionState.consumeRemotely()
            && Version.isVersionTopicOrStreamReprocessingTopic(partitionConsumptionState.getOffsetRecord().getLeaderTopic())
            && !(partitionConsumptionState.isIncrementalPushEnabled() && partitionConsumptionState.getIncrementalPushPolicy()
                .equals(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC) && !isWriteComputationEnabled)) {
          if (partitionConsumptionState.skipKafkaMessage()) {
            String msg = "Skipping messages after EOP in remote version topic. Topic: " + kafkaVersionTopic + " Partition Id: " + subPartition;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              logger.info(msg);
            }
            return false;
          }
          if (record.key().isControlMessage()) {
            ControlMessageType controlMessageType = ControlMessageType.valueOf((ControlMessage)record.value().payloadUnion);
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
        String currentLeaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
        if (!record.topic().equals(currentLeaderTopic)) {
          String errorMsg = "Leader receives a Kafka record that doesn't belong to leader topic. Store version: " + this.kafkaVersionTopic
              + ", partition: " + partitionConsumptionState.getPartition() + ", leader topic: " + currentLeaderTopic
              + ", topic of incoming message: " + record.topic();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            logger.error(errorMsg);
          }
          return false;
        }
        break;
      default:
        String recordTopic = record.topic();
        if (!kafkaVersionTopic.equals(recordTopic)) {
          throw new VeniceMessageException(
              consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderFollowerState() + "; partition: " + subPartition
                  + "; Message retrieved from different topic. Expected " + this.kafkaVersionTopic + " Actual " + recordTopic);
        }

        long lastOffset = partitionConsumptionState.getLatestProcessedVersionTopicOffset();
        if (lastOffset >= record.offset()) {
          logger.info(
              consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderFollowerState()
                  + "; The record was already processed Partition" + subPartition + " LastKnown " + lastOffset + " Current " + record.offset());
          return false;
        }
        break;
    }

    return super.shouldProcessRecord(record);
  }

  /**
   * Additional safeguards in Leader/Follower ingestion:
   * 1. Check whether the incoming messages are from the expected source topics
   */
  @Override
  protected boolean shouldPersistRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record,
      PartitionConsumptionState partitionConsumptionState) {
    if (!super.shouldPersistRecord(record, partitionConsumptionState)) {
      return false;
    }

    switch (partitionConsumptionState.getLeaderFollowerState()) {
      case LEADER:
        String currentLeaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
        if (!record.topic().equals(currentLeaderTopic)) {
          String errorMsg = "Leader receives a Kafka record that doesn't belong to leader topic. Store version: " + this.kafkaVersionTopic
              + ", partition: " + partitionConsumptionState.getPartition() + ", leader topic: " + currentLeaderTopic
              + ", topic of incoming message: " + record.topic();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            logger.error(errorMsg);
          }
          return false;
        }
        break;
      default:
        if (!record.topic().equals(this.kafkaVersionTopic)) {
          String errorMsg = partitionConsumptionState.getLeaderFollowerState().toString() + " replica receives a Kafka record that doesn't belong to version topic. Store version: "
              + this.kafkaVersionTopic + ", partition: " + partitionConsumptionState.getPartition() + ", topic of incoming message: " + record.topic();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsg)) {
            logger.error(errorMsg);
          }
          return false;
        }
        break;
    }
    return true;
  }

  @Override
  protected void recordWriterStats(long producerTimestampMs, long brokerTimestampMs, long consumerTimestampMs,
      PartitionConsumptionState partitionConsumptionState) {
    if (isNativeReplicationEnabled) {
      // Emit latency metrics separately for leaders and followers
      long producerBrokerLatencyMs = Math.max(brokerTimestampMs - producerTimestampMs, 0);
      long brokerConsumerLatencyMs = Math.max(consumerTimestampMs - brokerTimestampMs, 0);
      long producerConsumerLatencyMs = Math.max(consumerTimestampMs - producerTimestampMs, 0);
      boolean isLeader = partitionConsumptionState.getLeaderFollowerState().equals(LEADER);
      if (isLeader) {
        versionedDIVStats.recordProducerSourceBrokerLatencyMs(storeName, versionNumber, producerBrokerLatencyMs);
        versionedDIVStats.recordSourceBrokerLeaderConsumerLatencyMs(storeName, versionNumber, brokerConsumerLatencyMs);
        versionedDIVStats.recordProducerLeaderConsumerLatencyMs(storeName, versionNumber, producerConsumerLatencyMs);
      } else {
        versionedDIVStats.recordProducerLocalBrokerLatencyMs(storeName, versionNumber, producerBrokerLatencyMs);
        versionedDIVStats.recordLocalBrokerFollowerConsumerLatencyMs(storeName, versionNumber, brokerConsumerLatencyMs);
        versionedDIVStats.recordProducerFollowerConsumerLatencyMs(storeName, versionNumber, producerConsumerLatencyMs);
      }
    } else {
      super.recordWriterStats(producerTimestampMs, brokerTimestampMs, consumerTimestampMs, partitionConsumptionState);
    }
  }

  @Override
  protected void recordProcessedRecordStats(PartitionConsumptionState partitionConsumptionState, int processedRecordSize, int processedRecordNum) {
    if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
      versionedStorageIngestionStats.recordLeaderBytesConsumed(storeName, versionNumber, processedRecordSize);
      versionedStorageIngestionStats.recordLeaderRecordsConsumed(storeName, versionNumber, processedRecordNum);
      storeIngestionStats.recordTotalLeaderBytesConsumed(processedRecordSize);
      storeIngestionStats.recordTotalLeaderRecordsConsumed(processedRecordNum);
    } else {
      versionedStorageIngestionStats.recordFollowerBytesConsumed(storeName, versionNumber, processedRecordSize);
      versionedStorageIngestionStats.recordFollowerRecordsConsumed(storeName, versionNumber, processedRecordNum);
      storeIngestionStats.recordTotalFollowerBytesConsumed(processedRecordSize);
      storeIngestionStats.recordTotalFollowerRecordsConsumed(processedRecordNum);
    }
  }

  private void recordProducerStats(int producedRecordSize, int producedRecordNum) {
    versionedStorageIngestionStats.recordLeaderBytesProduced(storeName, versionNumber, producedRecordSize);
    versionedStorageIngestionStats.recordLeaderRecordsProduced(storeName, versionNumber, producedRecordNum);
    storeIngestionStats.recordTotalLeaderBytesProduced(producedRecordSize);
    storeIngestionStats.recordTotalLeaderRecordsProduced(producedRecordNum);
  }

  private void recordFabricHybridConsumptionStats(String kafkaUrl, int producedRecordSize, int partitionId, long upstreamOffset) {
    if (kafkaClusterUrlToIdMap.containsKey(kafkaUrl)) {
      int regionId = kafkaClusterUrlToIdMap.get(kafkaUrl);
      versionedStorageIngestionStats.recordRegionHybridBytesConsumed(storeName, versionNumber, producedRecordSize, regionId);
      versionedStorageIngestionStats.recordRegionHybridRecordsConsumed(storeName, versionNumber, 1, regionId);
      versionedStorageIngestionStats.recordRegionHybridAvgConsumedOffset(storeName, versionNumber, upstreamOffset, regionId);
      storeIngestionStats.recordTotalRegionHybridBytesConsumed(regionId, producedRecordSize);
      storeIngestionStats.recordTotalRegionHybridRecordsConsumed(regionId, 1);
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
   * The caller {@link StoreIngestionTask#produceToStoreBufferServiceOrKafka(Iterable, boolean)} of this function should
   * not process this consumerRecord any further if it was produced to kafka (returns true),
   * Otherwise it should continue to process the message as it would.
   *
   * This function assumes {@link #shouldProcessRecord(ConsumerRecord)} has been called which happens in
   * {@link StoreIngestionTask#produceToStoreBufferServiceOrKafka(Iterable, boolean)} before calling this and the it was decided that
   * this record needs to be processed. It does not perform any validation check on the PartitionConsumptionState object
   * to keep the goal of the function simple and not overload.
   *
   * Also DIV validation is done here if the message is received from RT topic. For more info please see
   * please see {@literal StoreIngestionTask#internalProcessConsumerRecord(VeniceConsumerRecordWrapper, PartitionConsumptionState, ProducedRecord)}
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this function.
   *
   * @param consumerRecordWrapper
   * @return true if the message was produced to kafka, Otherwise false.
   */
  @Override
  protected DelegateConsumerRecordResult delegateConsumerRecord(
      VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper) {
    // if record is from a RT topic, we select partitionConsumptionState of leaderSubPartition
    // to record the consuming status
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    int subPartition = PartitionUtils.getSubPartition(consumerRecord.topic(), consumerRecord.partition(), amplificationFactor);
    boolean produceToLocalKafka = false;
    try {
      KafkaKey kafkaKey = consumerRecord.key();
      KafkaMessageEnvelope kafkaValue = consumerRecord.value();

      /**
       * partitionConsumptionState must be in a valid state and no error reported. This is made sure by calling
       * {@link shouldProcessRecord} before processing any record.
       */
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
      produceToLocalKafka = shouldProduceToVersionTopic(partitionConsumptionState);
      //UPDATE message is only expected in LEADER which must be produced to kafka.
      MessageType msgType = MessageType.valueOf(kafkaValue);
      if (msgType == MessageType.UPDATE && !produceToLocalKafka) {
        throw new VeniceMessageException(
            consumerTaskId + " hasProducedToKafka: Received UPDATE message in non-leader. Topic: "
                + consumerRecord.topic() + " Partition " + consumerRecord.partition() + " Offset "
                + consumerRecord.offset());
      }

      /**
       * return early if it needs not be produced to local VT such as cases like
       * (i) it's a follower or (ii) leader is consuming from VT
       */
      if (!produceToLocalKafka) {
        return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
      }

      //If we are here the message must be produced to local kafka or silently consumed.
      LeaderProducedRecordContext leaderProducedRecordContext;

      validateRecordBeforeProducingToLocalKafka(consumerRecordWrapper, partitionConsumptionState);

      if (Version.isRealTimeTopic(consumerRecord.topic())) {
        recordFabricHybridConsumptionStats(consumerRecordWrapper.kafkaUrl(),
                          consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize(),
                                           consumerRecord.partition(),
                                           consumerRecord.offset());
        updateLatestInMemoryLeaderConsumedRTOffset(partitionConsumptionState, consumerRecordWrapper.kafkaUrl(), consumerRecord.offset());
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
        validateMessage(this.kafkaDataIntegrityValidatorForLeaders, consumerRecord, isEndOfPushReceived);
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
        divErrorMetricCallback.get().execute(e);
        if (logger.isDebugEnabled()) {
          logger.debug(consumerTaskId + " : Skipping a duplicate record at offset: " + consumerRecord.offset());
        }
        return DelegateConsumerRecordResult.DUPLICATE_MESSAGE;
      }

      if (kafkaKey.isControlMessage()) {
        boolean producedFinally = true;
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
        leaderProducedRecordContext = LeaderProducedRecordContext.newControlMessageRecord(
            consumerRecordWrapper.kafkaUrl(),
            consumerRecord.offset(),
            kafkaKey.getKey(),
            controlMessage
        );
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
            produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get().put(consumerRecord.key(), consumerRecord.value(),
                    callback, consumerRecord.partition(), leaderMetadataWrapper));
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
            if (!Version.isRealTimeTopic(consumerRecord.topic())) {
              produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
                  (callback, leaderMetadataWrapper) -> veniceWriter.get().put(consumerRecord.key(), consumerRecord.value(),
                      callback, consumerRecord.partition(), leaderMetadataWrapper));
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
               *    3. stats maintenance as in {@link StoreIngestionTask#processVeniceMessage(VeniceConsumerRecordWrapper, PartitionConsumptionState, LeaderProducedRecordContext)}
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
          case START_OF_BUFFER_REPLAY:
            //this msg coming here is not possible;
            throw new VeniceMessageException(
                consumerTaskId + " hasProducedToKafka: Received SOBR in L/F mode. Topic: " + consumerRecord.topic()
                    + " Partition " + consumerRecord.partition() + " Offset " + consumerRecord.offset());
          case START_OF_INCREMENTAL_PUSH:
          case END_OF_INCREMENTAL_PUSH:
            // For inc push to RT policy, the control msg is written in RT topic, we will need to calculate the destination partition in VT correctly.
            int versionTopicPartitionToBeProduced = Version.isRealTimeTopic(consumerRecord.topic()) ? consumerRecord.partition() * amplificationFactor : consumerRecord.partition();
            /**
             * We are using {@link VeniceWriter#asyncSendControlMessage()} api instead of {@link VeniceWriter#put()} since we have
             * to calculate DIV for this message but keeping the ControlMessage content unchanged. {@link VeniceWriter#put()} does not
             * allow that.
             */
            produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get().asyncSendControlMessage(controlMessage, versionTopicPartitionToBeProduced,
                    new HashMap<>(), callback, leaderMetadataWrapper));
            break;
          case TOPIC_SWITCH:
            /**
             * For TOPIC_SWITCH message we should use -1 as consumedOffset. This will ensure that it does not update the
             * setLeaderUpstreamOffset in {@link updateOffset()}.
             * The leaderUpstreamOffset is set from the TS message config itself. We should not override it.
             */
            if (isDataRecovery && !partitionConsumptionState.isBatchOnly()) {
              // Ignore remote VT's TS message since we might need to consume more RT or incremental push data from VT
              // that's no longer in the local/remote RT due to retention.
              return DelegateConsumerRecordResult.SKIPPED_MESSAGE;
            }
            leaderProducedRecordContext = LeaderProducedRecordContext.newControlMessageRecord(null, -1, kafkaKey.getKey(), controlMessage);
            produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
                (callback, leaderMetadataWrapper) -> veniceWriter.get().asyncSendControlMessage(controlMessage, consumerRecord.partition(),
                    new HashMap<>(), callback, DEFAULT_LEADER_METADATA_WRAPPER));
            break;
        }
        if (!isSegmentControlMsg(controlMessageType)) {
          if (producedFinally) {
            logger.info(consumerTaskId + " hasProducedToKafka: YES. ControlMessage: " + controlMessageType.name()
                + ", received from  Topic: " + consumerRecord.topic() + " Partition: " + consumerRecord.partition() + " Offset: "
                + consumerRecord.offset());
          } else {
            logger.info(consumerTaskId + " hasProducedToKafka: NO. ControlMessage: " + controlMessageType.name()
                + ", received from  Topic: " + consumerRecord.topic() + " Partition: " + consumerRecord.partition() + " Offset: "
                + consumerRecord.offset());
          }
        }
      } else if (null == kafkaValue) {
        throw new VeniceMessageException(
            consumerTaskId + " hasProducedToKafka: Given null Venice Message.  Topic: " + consumerRecord.topic()
                + " Partition " + consumerRecord.partition() + " Offset " + consumerRecord.offset());
      } else {
        // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
        processMessageAndMaybeProduceToKafka(consumerRecordWrapper, partitionConsumptionState, subPartition);
      }
      return DelegateConsumerRecordResult.PRODUCED_TO_KAFKA;
    } catch (Exception e) {
      throw new VeniceException(
          consumerTaskId + " hasProducedToKafka: exception for message received from  Topic: " + consumerRecord.topic()
              + " Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset() + ". Bubbling up.",
          e);
    }
  }

  /**
   * Besides draining messages in the drainer queue, wait for the last producer future.
   */
  @Override
  protected void waitForAllMessageToBeProcessedFromTopicPartition(String topic, int partition,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    super.waitForAllMessageToBeProcessedFromTopicPartition(topic, partition, partitionConsumptionState);
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
        CompletableFuture<Void> lastQueuedRecordPersistedFuture = partitionConsumptionState.getLastQueuedRecordPersistedFuture();
        if (lastQueuedRecordPersistedFuture != null) {
          lastQueuedRecordPersistedFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
        }
      } catch (InterruptedException e) {
        logger.warn("Got interrupted while waiting for the last queued record to be persisted for topic: " + topic
            + " partition: " + partition + ". Will throw the interrupt exception", e);
        throw e;
      } catch (Exception e) {
        logger.error("Got exception while waiting for the latest queued record future to be completed for topic: "
            + topic + " partition: " + partition, e);
      }
      Future<Void> lastFuture = null;
      try {
        lastFuture = partitionConsumptionState.getLastLeaderPersistFuture();
        if (lastFuture != null) {
          long synchronizeStartTimeInNS = System.nanoTime();
          lastFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
          storeIngestionStats.recordLeaderProducerSynchronizeLatency(storeName, LatencyUtils.getLatencyInMS(synchronizeStartTimeInNS));
        }
      } catch (InterruptedException e) {
        logger.warn("Got interrupted while waiting for the last leader producer future for topic: " + topic
            + " partition: " + partition + ". No data loss. Will throw the interrupt exception", e);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
        throw e;
      } catch (TimeoutException e) {
        logger.error("Timeout on waiting for the last leader producer future for topic: " + topic
            + " partition: " + partition + ". No data loss.", e);
        lastFuture.cancel(true);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      } catch (Exception e) {
        logger.error(
            "Got exception while waiting for the latest producer future to be completed for topic: " + topic + " partition: " + partition, e);
        partitionConsumptionState.setLastLeaderPersistFuture(null);
        //No need to fail the push job; just record the failure.
        versionedDIVStats.recordBenignLeaderProducerFailure(storeName, versionNumber);
      }
    }
  }

  /**
   * Checks before producing local version topic.
   *
   * Extend this function when there is new check needed.
   */
  private void validateRecordBeforeProducingToLocalKafka(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      PartitionConsumptionState partitionConsumptionState) {
    // Check whether the message is from local version topic; leader shouldn't consume from local VT and then produce back to VT again
    if (consumerRecordWrapper.kafkaUrl().equals(this.localKafkaServer) && consumerRecordWrapper.consumerRecord().topic().equals(this.kafkaVersionTopic)) {
      try {
        int partitionId = partitionConsumptionState.getPartition();
        offerConsumerException(new VeniceException("Store version " + this.kafkaVersionTopic + " partition " + partitionId
            + " is consuming from local version topic and producing back to local version topic"), partitionId);
      } catch (VeniceException offerToQueueException) {
        setLastStoreIngestionException(offerToQueueException);
      }
    }
  }

  //calculate the the replication once per partition, checking Leader instance will make sure we calculate it just once per partition.
  private static final Predicate<? super PartitionConsumptionState> BATCH_REPLICATION_LAG_FILTER =
      pcs -> !pcs.isEndOfPushReceived() && pcs.consumeRemotely() && pcs.getLeaderFollowerState().equals(LEADER);

  private long getReplicationLag(Predicate<? super PartitionConsumptionState> partitionConsumptionStateFilter) {
    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!svs.isPresent()) {
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

    long replicationLag = partitionConsumptionStateMap.values().stream().filter(partitionConsumptionStateFilter)
        //the lag is (latest VT offset in remote kafka - latest VT offset in local kafka)
        .mapToLong((pcs) -> {
          String currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic();
          if (currentLeaderTopic == null || currentLeaderTopic.isEmpty()) {
            currentLeaderTopic = kafkaVersionTopic;
          }

          String sourceKafkaURL = getSourceKafkaUrlForOffsetLagMeasurement(pcs);
          KafkaConsumerWrapper kafkaConsumer = consumerMap.get(sourceKafkaURL);
          // Consumer might not existed in the map after the consumption state is created, but before attaching the
          // corresponding consumer in consumerMap.
          if (kafkaConsumer != null) {
            Optional<Long> offsetLagOptional = kafkaConsumer.getOffsetLag(currentLeaderTopic, pcs.getPartition());
            if (offsetLagOptional.isPresent()) {
              return offsetLagOptional.get();
            }
          }
          // Fall back to use the old way
          return (cachedKafkaMetadataGetter.getOffset(getTopicManager(nativeReplicationSourceVersionTopicKafkaURL), currentLeaderTopic, pcs.getPartition()) - 1)
              - (cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), currentLeaderTopic, pcs.getPartition()) - 1);
        }).sum();

    return minZeroLag(replicationLag);
  }

  @Override
  public long getBatchReplicationLag() {
    return getReplicationLag(BATCH_REPLICATION_LAG_FILTER);
  }

  public static final Predicate<? super PartitionConsumptionState> LEADER_OFFSET_LAG_FILTER = pcs -> pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> BATCH_LEADER_OFFSET_LAG_FILTER = pcs ->
      !pcs.isEndOfPushReceived() && pcs.getLeaderFollowerState().equals(LEADER);
  private static final Predicate<? super PartitionConsumptionState> HYBRID_LEADER_OFFSET_LAG_FILTER = pcs ->
      pcs.isEndOfPushReceived() && pcs.isHybrid() && pcs.getLeaderFollowerState().equals(LEADER);

  private long getLeaderOffsetLag(Predicate<? super PartitionConsumptionState> partitionConsumptionStateFilter) {

    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!svs.isPresent()) {
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
        //the lag is (latest VT offset - consumed VT offset)
        .mapToLong((pcs) -> {
          String currentLeaderTopic = pcs.getOffsetRecord().getLeaderTopic();
          if (currentLeaderTopic == null || currentLeaderTopic.isEmpty()) {
            currentLeaderTopic = kafkaVersionTopic;
          }
          final String kafkaSourceAddress = getSourceKafkaUrlForOffsetLagMeasurement(pcs);
          KafkaConsumerWrapper kafkaConsumer = consumerMap.get(kafkaSourceAddress);
          // Consumer might not existed in the map after the consumption state is created, but before attaching the
          // corresponding consumer in consumerMap.
          if (kafkaConsumer != null) {
            Optional<Long> offsetLagOptional = kafkaConsumer.getOffsetLag(currentLeaderTopic, pcs.getPartition());
            if (offsetLagOptional.isPresent()) {
              return offsetLagOptional.get();
            }
          }

          // Fall back to calculate offset lag in the original approach
          if (Version.isRealTimeTopic(currentLeaderTopic)) {
            // Since partition count in RT : partition count in VT = 1 : AMP, we will need amplification factor adaptor to calculate the offset for every subPartitions.
            Pair<Long, Long> hybridOffsetPair = amplificationAdapter.getLatestLeaderConsumedOffsetAndHybridTopicOffset(kafkaSourceAddress, currentLeaderTopic, pcs);
            return (hybridOffsetPair.getSecond() - 1) - hybridOffsetPair.getFirst();
          } else {
            return (cachedKafkaMetadataGetter.getOffset(getTopicManager(kafkaSourceAddress), currentLeaderTopic, pcs.getPartition()) - 1)
                - pcs.getLatestProcessedVersionTopicOffset();
          }
        }).sum();

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

  private final Predicate<? super PartitionConsumptionState> FOLLOWER_OFFSET_LAG_FILTER = pcs ->
      pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1  && !pcs.getLeaderFollowerState().equals(LEADER);
  private final Predicate<? super PartitionConsumptionState> BATCH_FOLLOWER_OFFSET_LAG_FILTER = pcs ->
      !pcs.isEndOfPushReceived() && pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1  && !pcs.getLeaderFollowerState().equals(LEADER);
  private final Predicate<? super PartitionConsumptionState> HYBRID_FOLLOWER_OFFSET_LAG_FILTER = pcs ->
      pcs.isEndOfPushReceived() && pcs.isHybrid() && pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY) != -1  && !pcs.getLeaderFollowerState().equals(LEADER);

  private long getFollowerOffsetLag(Predicate<? super PartitionConsumptionState> partitionConsumptionStateFilter) {
    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!svs.isPresent()) {
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

    long offsetLag = partitionConsumptionStateMap.values().stream()
        //only calculate followers who have received EOP since before that, both leaders and followers
        //consume from VT
        .filter(partitionConsumptionStateFilter)
        //the lag is (latest VT offset - consumed VT offset)
        .mapToLong((pcs) -> {
          KafkaConsumerWrapper kafkaConsumer = consumerMap.get(localKafkaServer);
          // Consumer might not existed in the map after the consumption state is created, but before attaching the
          // corresponding consumer in consumerMap.
          if (kafkaConsumer != null) {
            Optional<Long> offsetLagOptional = kafkaConsumer.getOffsetLag(kafkaVersionTopic, pcs.getPartition());
            if (offsetLagOptional.isPresent()) {
              return offsetLagOptional.get();
            }
          }
          // Fall back to calculate offset lag in the old way
          return (cachedKafkaMetadataGetter.getOffset(getTopicManager(localKafkaServer), kafkaVersionTopic, pcs.getPartition()) - 1)
                  - pcs.getLatestProcessedVersionTopicOffset();
        }).sum();

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
        throw new VeniceException(String.format("Expect source Kafka URLs contains local Kafka URL. Got local " +
                "Kafka URL %s and source Kafka URLs %s", localKafkaServer, sourceKafkaURLs));
      }
    }
    return sourceKafkaURL;
  }

  /**
   * For L/F or NR, there is only one entry in upstreamOffsetMap whose key is NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY.
   * Return the value of the entry.
   */
  @Override
  protected long getLatestPersistedUpstreamOffsetForHybridOffsetLagMeasurement(PartitionConsumptionState pcs, String ignoredUpstreamKafkaUrl) {
    return pcs.getLatestProcessedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  /**
   * For regular L/F stores without A/A enabled, there is always only one real-time source.
   */
  @Override
  protected long getLatestConsumedUpstreamOffsetForHybridOffsetLagMeasurement(PartitionConsumptionState pcs, String ignoredKafkaUrl) {
    return pcs.getLeaderConsumedUpstreamRTOffset(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY);
  }

  @Override
  protected void updateLatestInMemoryLeaderConsumedRTOffset(PartitionConsumptionState pcs, String ignoredKafkaUrl, long offset) {
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
    Map<String, KafkaConsumerWrapper> consumerByKafkaURL = getConsumerByKafkaURL(partitionConsumptionState);
    String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();

    consumerByKafkaURL.values().forEach(consumer -> {
      if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER) && leaderTopic != null) {
        consumer.unSubscribe(leaderTopic, partitionConsumptionState.getPartition());
      } else {
        consumer.unSubscribe(kafkaVersionTopic, partitionConsumptionState.getPartition());
      }
    });
    /**
     * Leader of the user partition should close all subPartitions it is producing to.
     */
    veniceWriter.ifPresent(vw-> vw.closePartition(partitionConsumptionState.getPartition()));
  }

  @Override
  public int getWriteComputeErrorCode() {
    return writeComputeFailureCode;
  }

  protected int getSubPartitionId(byte[] key, String topic, int partition) {
    return amplificationFactor != 1 && Version.isRealTimeTopic(topic) ?
        venicePartitioner.getPartitionId(key, subPartitionCount) : partition;
  }

  protected void processMessageAndMaybeProduceToKafka(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, PartitionConsumptionState partitionConsumptionState, int subPartition) {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = consumerRecordWrapper.consumerRecord();
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    byte[] keyBytes = kafkaKey.getKey();
    MessageType msgType = MessageType.valueOf(kafkaValue.messageType);
    boolean isChunkedTopic = storageMetadataService.isStoreVersionChunked(kafkaVersionTopic);
    switch (msgType) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        ByteBuffer putValue = put.putValue;

        /**
         * For WC enabled stores update the transient record map with the latest {key,value}. This is needed only for messages
         * received from RT. Messages received from VT have been persisted to disk already before switching to RT topic.
         */
        if (isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived()) {
          partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, putValue.array(),
              putValue.position(), putValue.remaining(), put.schemaId, null);
        }

        LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext.newPutRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, put);
        produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
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
                return veniceWriter.get().put(kafkaKey, kafkaValue, callback, consumerRecord.partition(), leaderMetadataWrapper);
              }

              /**
               * When amplificationFactor != 1 and it is a leaderSubPartition consuming from the Real-time topic,
               * VeniceWriter will run VenicePartitioner inside to decide which subPartition of Kafka VT to write to.
               * For details about how VenicePartitioner find the correct subPartition,
               * please check {@link com.linkedin.venice.partitioner.UserPartitionAwarePartitioner}
               */
              return veniceWriter.get().put(keyBytes, ByteUtils.extractByteArray(putValue), put.schemaId, callback,
                  leaderMetadataWrapper);
            });
        break;

      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        final int valueSchemaId = update.schemaId;
        final int writeComputeSchemaId = update.updateSchemaId;
        final GenericRecord originalValue;

        /**
         *  Few Notes:
         *  1. Currently we support chunking only for messages produced on VT topic during batch part of the ingestion
         *     for hybrid stores. Chunking is NOT supported for messages produced to RT topics during streaming ingestion.
         *     Also we dont support compression at all for hybrid store.
         *     So the assumption here is that the PUT/UPDATE messages stored in transientRecord should always be a full value
         *     (non chunked) and non-compressed. Decoding should succeed using the the simplified API
         *     {@link ChunkingAdapter#constructValue(int, byte[], int, boolean, ReadOnlySchemaRepository, String)}
         *
         *  2. It's debatable if we'd like to the latest value schema or the value schema associated in the UPDATE message
         *     as the reader schema. Either one has pro and cons. The former is appealing when the schema has been devolved
         *     but hasn't been caught up by the write-compute pipeline. It prevents records which are originally serialized
         *     by the later schema get "reverted" to the old schema. However, this can go the opposite way as well. If the
         *     users register a "bad" schema unintentionally. The "latest value schema" approach will force the value
         *     record being upgraded even if the latest value schema hasn't been used by users yet.
         *
         *     As for now, we're using the value schema associated in the UPDATE message as the reader schema.
         */

        // Find the existing value. If a value for this key is found from the transient map then use that value, otherwise get it from DB.
        PartitionConsumptionState.TransientRecord transientRecord = partitionConsumptionState.getTransientRecord(keyBytes);
        if (transientRecord == null) {
          try {
            long lookupStartTimeInNS = System.nanoTime();
            originalValue = GenericRecordChunkingAdapter.INSTANCE.get(
                storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic), valueSchemaId,
                getSubPartitionId(keyBytes, consumerRecord.topic(), consumerRecord.partition()),
                ByteBuffer.wrap(keyBytes), isChunkedTopic, null, null, null,
                storageMetadataService.getStoreVersionCompressionStrategy(kafkaVersionTopic),
                serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName, compressorFactory, true);
            storeIngestionStats.recordWriteComputeLookUpLatency(storeName,
                LatencyUtils.getLatencyInMS(lookupStartTimeInNS));
          } catch (Exception e) {
            writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_DESERIALIZATION_FAILURE.code;
            throw e;
          }
        } else {
          storeIngestionStats.recordWriteComputeCacheHitCount(storeName);
          //construct originalValue from this transient record only if it's not null.
          if (transientRecord.getValue() != null) {
            try {
              originalValue = GenericRecordChunkingAdapter.INSTANCE.constructValue(transientRecord.getValueSchemaId(),
                  valueSchemaId, transientRecord.getValue(), transientRecord.getValueOffset(),
                  transientRecord.getValueLen(), serverConfig.isComputeFastAvroEnabled(), schemaRepository, storeName);
            } catch (Exception e) {
              writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_DESERIALIZATION_FAILURE.code;
              throw e;
            }
          } else {
            originalValue = null;
          }
        }

        //compute.
        final byte[] updatedValueBytes;
        try {
          long writeComputeStartTimeInNS = System.nanoTime();
          updatedValueBytes =
              ingestionTaskWriteComputeHandler.getUpdatedValueBytes(originalValue, update.updateValue,
                  valueSchemaId, writeComputeSchemaId);
          storeIngestionStats.recordWriteComputeUpdateLatency(storeName, LatencyUtils.getLatencyInMS(writeComputeStartTimeInNS));
        } catch (Exception e) {
          writeComputeFailureCode = StatsErrorCode.WRITE_COMPUTE_UPDATE_FAILURE.code;
          throw e;
        }

        //finally produce and update the transient record map.
        if (updatedValueBytes == null) {
          partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, -1, null);
          leaderProducedRecordContext = LeaderProducedRecordContext.newDeleteRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, null);
          produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
              (callback, leaderMetadataWrapper) -> veniceWriter.get().delete(keyBytes, callback, leaderMetadataWrapper));
        } else {
          partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, updatedValueBytes, 0,
              updatedValueBytes.length, valueSchemaId, null);

          ByteBuffer updateValueWithSchemaId = ByteUtils.prependIntHeaderToByteBuffer(ByteBuffer.wrap(updatedValueBytes), valueSchemaId, false);

          Put updatedPut = new Put();
          updatedPut.putValue = updateValueWithSchemaId;
          updatedPut.schemaId = valueSchemaId;

          byte[] updatedKeyBytes = keyBytes;
          if (isChunkedTopic) {
            // Samza VeniceWriter doesn't handle chunking config properly. It reads chunking config
            // from user's input instead of getting it from store's metadata repo. This causes SN
            // to der-se of keys a couple of times.
            updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(keyBytes);
          }
          leaderProducedRecordContext = LeaderProducedRecordContext.newPutRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), updatedKeyBytes, updatedPut);
          produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
              (callback, leaderMetadataWrapper) -> veniceWriter.get().put(keyBytes, updatedValueBytes, valueSchemaId,
                  callback, leaderMetadataWrapper));
        }
        break;

      case DELETE:
        /**
         * For WC enabled stores update the transient record map with the latest {key,null} for similar reason as mentioned in PUT above.
         */
        if (isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived()) {
          partitionConsumptionState.setTransientRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, -1, null);
        }
        leaderProducedRecordContext = LeaderProducedRecordContext.newDeleteRecord(consumerRecordWrapper.kafkaUrl(), consumerRecord.offset(), keyBytes, null);
        produceToLocalKafka(consumerRecordWrapper, partitionConsumptionState, leaderProducedRecordContext,
            (callback, leaderMetadataWrapper) -> {
              /**
               * DIV pass-through for DELETE messages before EOP.
               */
              if (!partitionConsumptionState.isEndOfPushReceived()) {
                return veniceWriter.get().delete(kafkaKey, kafkaValue, callback, consumerRecord.partition(), leaderMetadataWrapper);
              }
              return veniceWriter.get().delete(keyBytes, callback, leaderMetadataWrapper);
            });
        break;

      default:
        throw new VeniceMessageException(consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
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
  interface UpdateRealtimeTopicOffset {
    void apply(String sourceKafkaUrl, long realtimeTopicOffset);
  }

  /**
   * A function to get last known realtime topic offset.
   */
  @FunctionalInterface
  interface GetLastKnownRealtimeTopicOffset {
    long apply(String sourceKafkaUrl);
  }

  private class LeaderProducerMessageCallback implements ChunkAwareCallback {
    private StoreIngestionTask ingestionTask;
    private final VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecordWrapper;
    private final PartitionConsumptionState partitionConsumptionState;
    private final String leaderTopic;
    private final String versionTopic;
    private final int partition;
    private final AggVersionedDIVStats versionedDIVStats;
    private final Logger logger;
    private final LeaderProducedRecordContext leaderProducedRecordContext;
    private final long produceTimeNs;

    /**
     * The three mutable fields below are determined by the {@link com.linkedin.venice.writer.VeniceWriter},
     * which populates them via {@link ChunkAwareCallback#setChunkingInfo(byte[], ByteBuffer[], ChunkedValueManifest)}.
     *
     */
    private byte[] key = null;
    private ChunkedValueManifest chunkedValueManifest = null;
    private ByteBuffer[] chunks = null;

    public LeaderProducerMessageCallback(StoreIngestionTask ingestionTask,
        VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> sourceConsumerRecordWrapper,
        PartitionConsumptionState partitionConsumptionState,
        String leaderTopic, String versionTopic, int partition, AggVersionedDIVStats versionedDIVStats, Logger logger,
        LeaderProducedRecordContext leaderProducedRecordContext, long produceTimeNs) {
      this.ingestionTask = ingestionTask;
      this.sourceConsumerRecordWrapper = sourceConsumerRecordWrapper;
      this.partitionConsumptionState = partitionConsumptionState;
      this.leaderTopic = leaderTopic;
      this.versionTopic = versionTopic;
      this.partition = partition;
      this.versionedDIVStats = versionedDIVStats;
      this.logger = logger;
      this.leaderProducedRecordContext = leaderProducedRecordContext;
      this.produceTimeNs = produceTimeNs;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        logger.error("Leader failed to send out message to version topic when consuming " + leaderTopic + " partition "
            + partition, e);
        String storeName = Version.parseStoreFromKafkaTopicName(versionTopic);
        int version = Version.parseVersionFromKafkaTopicName(versionTopic);
        versionedDIVStats.recordLeaderProducerFailure(storeName, version);
      } else {
        // recordMetadata.partition() represents the partition being written by VeniceWriter
        // partitionConsumptionState.getPartition() is leaderSubPartition
        // when leaderSubPartition != recordMetadata.partition(), local StorageEngine will be written by
        // followers consuming from VTs. So it is safe to skip adding the record to leader's StorageBufferService
        if (partitionConsumptionState.getLeaderFollowerState() == LEADER
            && recordMetadata.partition() != partitionConsumptionState.getPartition()) {
          leaderProducedRecordContext.completePersistedToDBFuture(null);
          return;
        }
        /**
         * performs some sanity checks for chunks.
         * key may be null in case of producing control messages with direct api's like
         * {@link VeniceWriter#SendControlMessage} or {@link VeniceWriter#asyncSendControlMessage}
         */
        if (chunkedValueManifest != null) {
          if (null == chunks) {
            throw new IllegalStateException("chunking info not initialized.");
          } else if (chunkedValueManifest.keysWithChunkIdSuffix.size() != chunks.length) {
            throw new IllegalStateException("chunkedValueManifest.keysWithChunkIdSuffix is not in sync with chunks.");
          }
        }

        //record just the time it took for this callback to be invoked before we do further processing here such as queuing to drainer.
        //this indicates how much time kafka took to deliver the message to broker.
        versionedDIVStats.recordLeaderProducerCompletionTime(storeName, versionNumber, LatencyUtils.getLatencyInMS(produceTimeNs));

        int producedRecordNum = 0;
        int producedRecordSize = 0;
        //produce to drainer buffer service for further processing.
        try {
          /**
           * queue the leaderProducedRecordContext to drainer service as is in case the value was not chunked.
           * Otherwise queue the chunks and manifest individually to drainer service.
           */
          if (chunkedValueManifest == null) {
            //update the keyBytes for the ProducedRecord in case it was changed due to isChunkingEnabled flag in VeniceWriter.
            if (key != null) {
              leaderProducedRecordContext.setKeyBytes(key);
            }
            leaderProducedRecordContext.setProducedOffset(recordMetadata.offset());
            ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, leaderProducedRecordContext);

            producedRecordNum++;
            producedRecordSize = Math.max(0, recordMetadata.serializedKeySize()) + Math.max(0, recordMetadata.serializedValueSize());
          } else {
            int schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
            for (int i = 0; i < chunkedValueManifest.keysWithChunkIdSuffix.size(); i++) {
              ByteBuffer chunkKey = chunkedValueManifest.keysWithChunkIdSuffix.get(i);
              ByteBuffer chunkValue = chunks[i];

              Put chunkPut = new Put();
              chunkPut.putValue = chunkValue;
              chunkPut.schemaId = schemaId;

              LeaderProducedRecordContext
                  producedRecordForChunk = LeaderProducedRecordContext.newPutRecord(null, -1, ByteUtils.extractByteArray(chunkKey), chunkPut);
              producedRecordForChunk.setProducedOffset(-1);
              ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, producedRecordForChunk);

              producedRecordNum++;
              producedRecordSize += chunkKey.remaining() + chunkValue.remaining();
            }

            //produce the manifest inside the top-level key
            schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
            ByteBuffer manifest = ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(versionTopic, chunkedValueManifest));
            /**
             * The byte[] coming out of the {@link CHUNKED_VALUE_MANIFEST_SERIALIZER} is padded in front, so
             * that the put to the storage engine can avoid a copy, but we need to set the position to skip
             * the padding in order for this trick to work.
             */
            manifest.position(ValueRecord.SCHEMA_HEADER_LENGTH);

            Put manifestPut = new Put();
            manifestPut.putValue = manifest;
            manifestPut.schemaId = schemaId;

            LeaderProducedRecordContext producedRecordForManifest = LeaderProducedRecordContext.newPutRecordWithFuture(
                leaderProducedRecordContext.getConsumedKafkaUrl(),
                leaderProducedRecordContext.getConsumedOffset(),
                key, manifestPut, leaderProducedRecordContext.getPersistedToDBFuture());
            producedRecordForManifest.setProducedOffset(recordMetadata.offset());
            ingestionTask.produceToStoreBufferService(sourceConsumerRecordWrapper, producedRecordForManifest);
            producedRecordNum++;
            producedRecordSize += key.length + manifest.remaining();
          }
          recordProducerStats(producedRecordSize, producedRecordNum);
        } catch (Exception oe) {
          boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
          logger.error(consumerTaskId + " received exception in kafka callback thread; EOP received: " + endOfPushReceived + " Topic: " + sourceConsumerRecordWrapper.consumerRecord().topic() + " Partition: "
              + sourceConsumerRecordWrapper.consumerRecord().partition() + ", Offset: " + sourceConsumerRecordWrapper.consumerRecord().offset() + " exception: ", oe);
          //If EOP is not received yet, set the ingestion task exception so that ingestion will fail eventually.
          if (!endOfPushReceived) {
            try {
              ingestionTask.offerProducerException(oe, partition);
            } catch (VeniceException offerToQueueException) {
              ingestionTask.setLastStoreIngestionException(offerToQueueException);
            }
          }
          if (oe instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(oe);
          }
        }
      }
    }

    @Override
    public void setChunkingInfo(byte[] key, ByteBuffer[] chunks, ChunkedValueManifest chunkedValueManifest) {
      this.key = key;
      this.chunkedValueManifest = chunkedValueManifest;
      this.chunks = chunks;
    }
  }
}
