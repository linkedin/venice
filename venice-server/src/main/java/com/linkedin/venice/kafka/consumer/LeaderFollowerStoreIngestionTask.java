package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import static com.linkedin.venice.kafka.consumer.LeaderFollowerStateType.*;
import static com.linkedin.venice.store.record.ValueRecord.*;
import static com.linkedin.venice.writer.VeniceWriter.*;


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
 *              consumed message timestamp metadata;
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
  private static final Logger logger = Logger.getLogger(LeaderFollowerStoreIngestionTask.class);
  private static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER = new ChunkedValueManifestSerializer(false);

  /**
   * The new leader will stay inactive (not switch to any new topic or produce anything) for
   * some time after seeing the last messages in version topic.
   */
  public final long newLeaderInactiveTime;

  public LeaderFollowerStoreIngestionTask(
      VeniceWriterFactory writerFactory,
      VeniceConsumerFactory consumerFactory,
      Properties kafkaConsumerProperties,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      Queue<VeniceNotifier> notifiers,
      EventThrottler bandwidthThrottler,
      EventThrottler recordsThrottler,
      ReadOnlySchemaRepository schemaRepo,
      ReadOnlyStoreRepository metadataRepo,
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
    super(writerFactory, consumerFactory, kafkaConsumerProperties, storageEngineRepository, storageMetadataService, notifiers,
        bandwidthThrottler, recordsThrottler, schemaRepo, metadataRepo, topicManager, storeIngestionStats, versionedDIVStats, storeBufferService,
        isCurrentVersion, hybridStoreConfig, isIncrementalPushEnabled, storeConfig, diskUsage, bufferReplayEnabledForHybrid, serverConfig);
    newLeaderInactiveTime = serverConfig.getServerPromotionToLeaderReplicaDelayMs();
  }

  @Override
  public synchronized void promoteToLeader(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    validateState();
    ConsumerAction newAction = new ConsumerAction(ConsumerActionType.STANDBY_TO_LEADER, topic, partitionId, nextSeqNum(), checker);
    consumerActionsQueue.add(newAction);
  }

  @Override
  public synchronized void demoteToStandby(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    validateState();
    ConsumerAction newAction = new ConsumerAction(ConsumerActionType.LEADER_TO_STANDBY, topic, partitionId, nextSeqNum(), checker);
    consumerActionsQueue.add(newAction);
  }

  @Override
  protected void processConsumerAction(ConsumerAction message)
      throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case STANDBY_TO_LEADER:
        LeaderFollowerParticipantModel.LeaderSessionIdChecker checker = message.getLeaderSessionIdChecker();
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
        if (partitionConsumptionState.getLeaderState().equals(LEADER)) {
          logger.info("State transition from STANDBY to LEADER is skipped for topic " + topic + " partition " + partition
              + ", because this replica is the leader already.");
          return;
        }

        // Mark this partition in the middle of STANDBY to LEADER transition
        partitionConsumptionState.setLeaderState(IN_TRANSITION_FROM_STANDBY_TO_LEADER);

        logger.info(consumerTaskId + " for partition " + partition + " is in transition from STANDBY to LEADER;\n"
            + partitionConsumptionState.getOffsetRecord().toDetailedString());
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
        if (partitionConsumptionState.getLeaderState().equals(STANDBY)) {
          logger.info("State transition from LEADER to STANDBY is skipped for topic " + topic + " partition " + partition
              + ", because this replica is a follower already.\n" + partitionConsumptionState.getOffsetRecord().toDetailedString());
          return;
        }

        /**
         * 1. If leader(itself) was consuming from VT previously, just set the state as STANDBY for this partition;
         * 2. otherwise, leader would unsubscribe from the previous feed topic (real-time topic or grandfathering
         *    transient topic); then drain all the messages from the feed topic, which would produce the corresponding
         *    result message to VT; block on the callback of the final message that it needs to produce; finally the new
         *    follower will switch back to consuming from VT using the latest VT offset tracked by producer callback.
         */
        OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
        String leaderTopic = offsetRecord.getLeaderTopic();
        long versionTopicOffset = offsetRecord.getOffset();
        if (!topic.equals(leaderTopic)) {
          consumer.unSubscribe(leaderTopic, partition);

          // finish consuming and producing all the things in queue
          storeBufferService.drainBufferedRecordsFromTopicPartition(leaderTopic, partition);

          // wait for the last callback to complete
          try {
            Future<RecordMetadata> lastFuture = partitionConsumptionState.getLastLeaderProduceFuture();
            if (lastFuture != null) {
              lastFuture.get();
            }
          } catch (Exception e) {
            logger.error("Failed to produce the last message to version topic before demoting to standby;"
                + " for partition: " + partition + "\n" + offsetRecord.toDetailedString(), e);
            /**
             * No need to fail the push job; new leader can safely resume from the failure checkpoint.
             */
            versionedDIVStats.recordLeaderProducerFailure(storeNameWithoutVersionInfo, storeVersion);
          }

          // subscribe back to VT/partition
          consumer.subscribe(topic, partition, versionTopicOffset);
          logger.info(consumerTaskId + " demoted to standby for partition " + partition + "\n" + offsetRecord.toDetailedString());
        }
        partitionConsumptionStateMap.get(partition).setLeaderState(STANDBY);
        break;
      default:
        processCommonConsumerAction(operation, topic, partition);
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
    for (PartitionConsumptionState partitionConsumptionState : partitionConsumptionStateMap.values()) {
      switch (partitionConsumptionState.getLeaderState()) {
        case IN_TRANSITION_FROM_STANDBY_TO_LEADER:
          int partition = partitionConsumptionState.getPartition();

          /**
           * If buffer replay is disabled, all replica are just consuming from version topic;
           * promote to leader immediately.
           */
          if (!bufferReplayEnabledForHybrid) {
            partitionConsumptionState.setLeaderState(LEADER);
            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            offsetRecord.setLeaderConsumptionState(this.topic, offsetRecord.getOffset());

            logger.info(consumerTaskId + " promoted to leader for partition " + partition);

            defaultReadyToServeChecker.apply(partitionConsumptionState);
            return;
          }

          long lastTimestamp = getLastConsumedMessageTimestamp(topic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
            /**
             * There isn't any new message from the old leader for at least {@link newLeaderInactiveTime} minutes,
             * this replica can finally be promoted to leader.
             */
            // unsubscribe from previous topic/partition
            consumer.unSubscribe(topic, partition);

            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            if (null == offsetRecord.getLeaderTopic()) {
              /**
               * This replica is ready to actually switch to LEADER role, but it has been consuming from version topic
               * all the time, so set the leader consumption state to its version topic consumption state.
               */
              offsetRecord.setLeaderConsumptionState(topic, offsetRecord.getOffset());
            }

            // subscribe to the new upstream
            consumer.subscribe(offsetRecord.getLeaderTopic(), partition, offsetRecord.getLeaderOffset());
            partitionConsumptionState.setLeaderState(LEADER);

            logger.info(consumerTaskId + " promoted to leader for partition " + partition
                + "; start consuming from " + offsetRecord.getLeaderTopic() + " offset " + offsetRecord.getLeaderOffset());

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
                + partitionConsumptionState.getOffsetRecord().toDetailedString();
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
          }

          TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
          if (null == topicSwitch || currentLeaderTopic.equals(topicSwitch.sourceTopicName.toString())) {
            continue;
          }

          /**
           * Otherwise, check whether it has been 5 minutes since the last update in the current topic
           *
           */
          String newSourceTopicName = topicSwitch.sourceTopicName.toString();
          partition = partitionConsumptionState.getPartition();
          lastTimestamp = getLastConsumedMessageTimestamp(currentLeaderTopic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
            // leader switch topic
            long upstreamStartOffset = partitionConsumptionState.getOffsetRecord().getLeaderOffset();
            if (upstreamStartOffset < 0) {
              if (topicSwitch.rewindStartTimestamp > 0) {
                upstreamStartOffset = topicManager.getOffsetByTime(newSourceTopicName, partition, topicSwitch.rewindStartTimestamp);
                if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
                  // subscribe will seek to the next offset
                  upstreamStartOffset -= 1;
                }
              } else {
                upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
              }
            }

            // unsubscribe the old source and subscribe to the new source
            consumer.unSubscribe(currentLeaderTopic, partition);

            // wait for the last callback to complete
            try {
              Future<RecordMetadata> lastFuture = partitionConsumptionState.getLastLeaderProduceFuture();
              if (lastFuture != null) {
                lastFuture.get();
              }
            } catch (Exception e) {
              String errorMsg = "Leader failed to produce the last message to version topic before switching "
                  + "feed topic from " + currentLeaderTopic+ " to " + newSourceTopicName + " partition: " + partition;
              logger.error(errorMsg, e);
              versionedDIVStats.recordLeaderProducerFailure(storeNameWithoutVersionInfo, storeVersion);
              notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), errorMsg, e);
              throw new VeniceException(errorMsg, e);
            }

            // subscribe to the new upstream
            partitionConsumptionState.getOffsetRecord().setLeaderConsumptionState(newSourceTopicName, upstreamStartOffset);
            consumer.subscribe(newSourceTopicName, partition, upstreamStartOffset);

            if (!topic.equals(currentLeaderTopic)) {
              /**
               * This is for Samza grandfathering.
               *
               * Use default upstream offset for callback so that leader won't update its own leader consumed offset after
               * producing the TS message.
               */
              LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, currentLeaderTopic,
                  topic, partition, DEFAULT_UPSTREAM_OFFSET, defaultReadyToServeChecker, Optional.empty(), versionedDIVStats, logger);
              /**
               * Put default upstream offset for TS message so that followers won't update the leader offset after seeing
               * the TS message.
               */
              ControlMessage controlMessage = new ControlMessage();
              controlMessage.controlMessageType = ControlMessageType.TOPIC_SWITCH.getValue();
              controlMessage.controlMessageUnion = topicSwitch;
              getVeniceWriter().sendControlMessage(controlMessage, partition, new HashMap<>(), callback, DEFAULT_UPSTREAM_OFFSET);
            }

            logger.info(consumerTaskId + " leader successfully switch feed topic from " + currentLeaderTopic + " to "
                + newSourceTopicName + " offset " + upstreamStartOffset + " partition " + partition);

            // In case new topic is empty and leader can never become online
            defaultReadyToServeChecker.apply(partitionConsumptionState);
          }
          break;
        case STANDBY:
          // no long running task for follower
          break;
      }
    }
  }

  /**
   * This method get the timestamp of the "last" message in topic/partition; notice that when the function
   * returns, new messages can be appended to the partition already, so it's not guaranteed that this timestamp
   * is from the last message.
   */
  private long getLastConsumedMessageTimestamp(String topic, int partition) throws InterruptedException {
    /**
     * Drain all the messages from the leaderTopic/partition in order to update
     * the latest consumed message timestamp.
     */
    storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);

    /**
     * {@link com.linkedin.venice.kafka.consumer.StoreBufferService.StoreBufferDrainer} would update
     * the last consumed message timestamp for the corresponding partition.
     */
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    long lastConsumedMessageTimestamp = partitionConsumptionState.getOffsetRecord().getProcessingTimeEpochMs();

    return lastConsumedMessageTimestamp;
  }

  /**
   * For the corresponding partition being tracked in `partitionConsumptionState`, if it's in LEADER state and it's
   * not consuming from version topic, it should produce the new message to version topic.
   *
   * If buffer replay is disable, all replicas will stick to version topic, no one is going to produce any message.
   */
  private boolean shouldProduceToVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    boolean isLeader = partitionConsumptionState.getLeaderState().equals(LEADER);
    String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
    return isLeader && !topic.equals(leaderTopic) && bufferReplayEnabledForHybrid;
  }

  /**
   * For regular hybrid store (batch ingestion from Hadoop and streaming ingestion from Samza), the control messages
   * won't be produced to the version topic because leader gets these control message inside version topic, or in other
   * words, `shouldProduceToVersionTopic` would return false.
   *
   * For grandfathering jobs, since the batch ingestion is from Samza and it will write to a transient topic (WIP),
   * leader should produce these messages to version topic so that followers understands that the push starts or ends.
   */
  @Override
  protected void processStartOfPush(ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    super.processStartOfPush(controlMessage, partition, offset, partitionConsumptionState);

    if (shouldProduceToVersionTopic(partitionConsumptionState)) {
      String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
      LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, leaderTopic,
          topic, partition, offset, defaultReadyToServeChecker, Optional.empty(), versionedDIVStats, logger);

      /**
       * N.B.: This is expected to be the first time time we call {@link #getVeniceWriter()}, which is important
       *       because that function relies on the Start of Push's flags having been set in the
       *       {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState}. The flag setting happens in
       *       {@link StoreIngestionTask#processStartOfPush(ControlMessage, int, long, PartitionConsumptionState)},
       *       which is called at the start of the current function.
       */
      getVeniceWriter().sendControlMessage(controlMessage, partition, new HashMap<>(), callback, offset);
    }
  }

  @Override
  protected void processEndOfPush(ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    super.processEndOfPush(controlMessage, partition, offset, partitionConsumptionState);

    if (shouldProduceToVersionTopic(partitionConsumptionState)) {
      String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
      LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, leaderTopic,
          topic, partition, offset, defaultReadyToServeChecker, Optional.empty(), versionedDIVStats, logger);
      getVeniceWriter().sendControlMessage(controlMessage, partition, new HashMap<>(), callback, offset);
    }
  }

  @Override
  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
    /**
     * Currently just check whether the sourceKafkaServers list inside TopicSwitch control message only contains
     * one Kafka server url and whether it's equal to the Kafka server url we use for consumer and producer.
     *
     * In the future, when we support consuming from one or multiple remote Kafka servers, we need to modify
     * the code here.
     */
    List<CharSequence> kafkaServerUrls = topicSwitch.sourceKafkaServers;
    if (kafkaServerUrls.size() != 1) {
      throw new VeniceException("More than one Kafka server urls in TopicSwitch control message, "
          + "TopicSwitch.sourceKafkaServers: " + kafkaServerUrls);
    }
     if (!serverConfig.getKafkaBootstrapServers().equals(kafkaServerUrls.get(0).toString())) {
       logger.warn("Kafka server url in TopicSwitch control message is different from the url in consumer");
       /**
        * Kafka is migrating the vip servers; we should stop comparing the kafka url used by controller with the url used by servers;
        * otherwise, we require a synchronized deployment between controllers and servers, which is almost infeasible.
        *
        * TODO: start checking the url after the Kafka VIP migration; or maybe don't check the url until Actice/Active replication.
        */
       // throw new VeniceException("Kafka server url in TopicSwitch control message is different from the url in consumer");
     }
    notificationDispatcher.reportTopicSwitchReceived(partitionConsumptionState);

    // Calculate the start offset based on start timestamp
    String newSourceTopicName = topicSwitch.sourceTopicName.toString();
    long upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
    if (topicSwitch.rewindStartTimestamp > 0) {
      upstreamStartOffset = topicManager.getOffsetByTime(newSourceTopicName, partition, topicSwitch.rewindStartTimestamp);
    }

    // Sync TopicSwitch message into metadata store
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
    if (storeVersionState.isPresent()) {
      String newTopicSwitchLogging = "TopicSwitch message (new source topic:" + topicSwitch.sourceTopicName
          + "; rewind start time:" + topicSwitch.rewindStartTimestamp + ")";
      if (null == storeVersionState.get().topicSwitch) {
        logger.info("First time receiving a " + newTopicSwitchLogging);
        storeVersionState.get().topicSwitch = topicSwitch;
      } else {
        logger.info("Previous TopicSwitch message in metadata store (source topic:" + storeVersionState.get().topicSwitch.sourceTopicName
            + "; rewind start time:" + storeVersionState.get().topicSwitch.rewindStartTimestamp + ") will be replaced"
            + " by the new " + newTopicSwitchLogging);
        storeVersionState.get().topicSwitch = topicSwitch;
      }
      // Sync latest store version level metadata to disk
      storageMetadataService.put(topic, storeVersionState.get());

      // Put TopicSwitch message into in-memory state to avoid poking metadata store
      partitionConsumptionState.setTopicSwitch(topicSwitch);
    } else {
      throw new VeniceException("Unexpected: received some " + ControlMessageType.TOPIC_SWITCH.name() +
          " control message in a topic where we have not yet received a " +
          ControlMessageType.START_OF_PUSH.name() + " control message.");
    }


    if (partitionConsumptionState.getLeaderState().equals(LEADER)) {
      /**
       * Leader shouldn't switch topic here (drainer thread), which would conflict with the ingestion thread which would
       * also access consumer.
       *
       * Besides, if there is re-balance, leader should finish consuming the everything in VT before switching topics;
       * there could be more than one TopicSwitch message in VT, we should honor the last one during re-balance; so
       * don't update the consumption state like leader topic or leader offset yet until actually switching topic
       */
      if (!bufferReplayEnabledForHybrid) {
        /**
         * If buffer replay is disabled, everyone sticks to the version topic
         * and only consumes from version topic.
         */
        logger.info(consumerTaskId + " receives TopicSwitch message: new source topic: " + newSourceTopicName
            + "; start offset: " + upstreamStartOffset + "; however buffer replay is disabled, consumer will stick to " + topic);
        partitionConsumptionState.getOffsetRecord().setLeaderConsumptionState(newSourceTopicName, upstreamStartOffset);
        return;
      }
      // Do not update leader consumption state until actually switching topic
    } else {
      /**
       * For follower, just keep track of what leader is doing now.
       */
      partitionConsumptionState.getOffsetRecord().setLeaderConsumptionState(newSourceTopicName, upstreamStartOffset);

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

  @Override
  protected void updateOffset(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    // Only update the metadata if this replica should NOT produce to version topic.
    if (!shouldProduceToVersionTopic(partitionConsumptionState)) {
      /**
       * If either (1) this is a follower replica or (2) this is a leader replica who is consuming from version topic,
       * we can update the offset metadata in offset record right after consuming a message; otherwise, if the leader
       * is consuming from real-time topic or grandfathering topic, it should update offset metadata after succeesfully
       * produce a corresponding message.
       */
      KafkaMessageEnvelope kafkaValue = consumerRecord.value();
      offsetRecord.setOffset(consumerRecord.offset());

      // also update the leader topic offset using the upstream offset in ProducerMetadata
      if (kafkaValue.producerMetadata.upstreamOffset >= 0) {
        long newUpstreamOffset = kafkaValue.producerMetadata.upstreamOffset;
        long previousUpstreamOffset = offsetRecord.getLeaderOffset();
        /**
         * If upstream offset is rewound and it's from a different producer, we encounter a split-brain
         * issue (multiple leaders producing to the same partition at the same time)
         */
        if (newUpstreamOffset < previousUpstreamOffset && offsetRecord.getLeaderGUID() != null
            && !kafkaValue.producerMetadata.producerGUID.equals(offsetRecord.getLeaderGUID())) {
          /**
           * Check whether the data inside rewind message is the same the data inside storage engine; if so,
           * we don't consider it as lossy rewind; otherwise, report potentially lossy upstream rewind.
           *
           * Fail the job if it's lossy and it's during the GF job (before END_OF_PUSH received);
           * otherwise, don't fail the push job, it's streaming ingestion now so it's serving online traffic already.
           */
          String logMsg = String.format(consumerTaskId + " received message with upstreamOffset: %ld;"
              + " but recorded upstreamOffset is: %ld. New GUID: %s; previous producer GUID: %s. "
              + "Multiple leaders are producing.", newUpstreamOffset, previousUpstreamOffset,
              GuidUtils.getHexFromGuid(kafkaValue.producerMetadata.producerGUID), GuidUtils.getHexFromGuid(offsetRecord.getLeaderGUID()));

          boolean lossy = true;
          try {
            KafkaKey key = consumerRecord.key();
            KafkaMessageEnvelope envelope = consumerRecord.value();
            AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(topic);
            switch (MessageType.valueOf(envelope)) {
              case PUT:
                // Issue an read to get the current value of the key
                byte[] actualValue = storageEngine.get(consumerRecord.partition(), key.getKey());
                if (actualValue != null) {
                  int actualSchemaId = ByteUtils.readInt(actualValue, 0);
                  Put put = (Put) envelope.payloadUnion;
                  if (actualSchemaId == put.schemaId) {
                    // continue if schema Id is the same
                    if (ByteUtils.equals(put.putValue.array(), put.putValue.position(), actualValue, SCHEMA_HEADER_LENGTH)) {
                      lossy = false;
                      logMsg += "\nBut this rewound PUT is not lossy because the data in the rewind message is the same as the data inside Venice";
                    }
                  }
                }
                break;
              case DELETE:
                /**
                 * Lossy if the key/value pair is added back to the storage engine after the first DELETE message.
                 */
                actualValue = storageEngine.get(consumerRecord.partition(), key.getKey());
                if (actualValue == null) {
                  lossy = false;
                  logMsg += "\nBut this rewound DELETE is not lossy because the data in the rewind message is deleted already";
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
              logMsg += "\nFailing the job because lossy rewind happens during Grandfathering job";
              logger.error(logMsg);
              versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeNameWithoutVersionInfo, storeVersion);
              VeniceException e = new VeniceException(logMsg);
              notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), logMsg, e);
              throw e;
            } else {
              logMsg += "\nDon't fail the job during streaming ingestion";
              logger.error(logMsg);
              versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeNameWithoutVersionInfo, storeVersion);
            }
          } else {
            logger.info(logMsg);
            versionedDIVStats.recordBenignLeaderOffsetRewind(storeNameWithoutVersionInfo, storeVersion);
          }
        }
        /**
         * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
         * to the true leader when the old leader doesn't stop producing.
         */
        offsetRecord.setLeaderTopicOffset(kafkaValue.producerMetadata.upstreamOffset);
      }
      // update leader producer GUID
      offsetRecord.setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
    }
  }

  @Override
  protected void produceAndWriteToDatabase(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      WriteToStorageEngine writeFunction, ProduceToTopic produceFunction) {
    int partition = consumerRecord.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    if (shouldProduceToVersionTopic(partitionConsumptionState)) {
      String leaderTopic = consumerRecord.topic();
      long sourceTopicOffset = consumerRecord.offset();
      LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, leaderTopic,
          topic, partition, sourceTopicOffset, defaultReadyToServeChecker, Optional.of(writeFunction), versionedDIVStats, logger);
      partitionConsumptionState.setLastLeaderProduceFuture(produceFunction.apply(callback, sourceTopicOffset));
    } else {
      // If (i) it's a follower or (ii) leader is consuming from VT, execute the write function immediately
      writeFunction.apply(consumerRecord.key().getKey());
    }
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
     * ingestion, {@link CachedLatestOffsetGetter} was introduced to get the latest offset periodically;
     * with this strategy, it is possible that partition could become 'ONLINE' at most
     * {@link CachedLatestOffsetGetter#ttlMs} earlier.
     */
    if (bufferReplayEnabledForHybrid) {
      String leaderTopic = offsetRecord.getLeaderTopic();
      if (null == leaderTopic || !Version.isRealTimeTopic(leaderTopic)) {
        /**
         * 1. Usually there is a batch-push or empty push for the bybrid store before replaying messages from real-time
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

      // leaderTopic is the real-time topic now
      long leaderOffset = offsetRecord.getLeaderOffset();
      long lastOffsetInRealTimeTopic = cachedLatestOffsetGetter.getOffset(leaderTopic, partition);

      long lag = lastOffsetInRealTimeTopic - leaderOffset;
      if (shouldLogLag) {
        logger.info(String.format("%s partition %d real-time buffer lag offset is: " + "(Last RT offset [%d] - Last leader consumed offset [%d]) = Lag [%d]",
            consumerTaskId, partition, lastOffsetInRealTimeTopic, leaderOffset, lag));
      }

      return lag;
    } else {
      /**
       * If buffer replay is disabled, all replicas are consuming from version topic, so check the lag in version topic.
       */
      long versionTopicConsumedOffset = offsetRecord.getOffset();
      long storeVersionTopicLatestOffset = cachedLatestOffsetGetter.getOffset(topic, partition);
      long lag = storeVersionTopicLatestOffset - versionTopicConsumedOffset;
      if (shouldLogLag) {
        logger.info(String.format("Store buffer replay was disabled, and %s partition %d lag offset is: (Last VT offset [%d] - Last VT consumed offset [%d]) = Lag [%d]",
            consumerTaskId, partition, storeVersionTopicLatestOffset, versionTopicConsumedOffset, lag));
      }
      return lag;
    }
  }

  /**
   * For Leader/Follower model, the follower should have the same kind of check as the Online/Offline model;
   * for leader, it's possible that it consumers from real-time topic or GF topic.
   */
  @Override
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if(null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + topic + " Partition Id: " + partitionId);
      return false;
    }

    if (!partitionConsumptionState.getLeaderState().equals(LEADER)) {
      String recordTopic = record.topic();
      if (!topic.equals(recordTopic)) {
        throw new VeniceMessageException(
            consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderState() + "; partition: " + partitionId
                + "; Message retrieved from different topic. Expected " + this.topic + " Actual " + recordTopic);
      }

      long lastOffset = partitionConsumptionState.getOffsetRecord().getOffset();
      if (lastOffset >= record.offset()) {
        logger.info(
            consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderState()
                + "; The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
        return false;
      }
    }

    return super.shouldProcessRecord(record);
  }

  private class LeaderProducerMessageCallback implements ChunkAwareCallback {
    private final PartitionConsumptionState partitionConsumptionState;
    private final String feedTopic;
    private final String versionTopic;
    private final int partition;
    private final long consumedOffset;
    private final ReadyToServeCheck readyToServeChecker;
    private final Optional<WriteToStorageEngine> writeFunction;
    private final AggVersionedDIVStats versionedDIVStats;
    private final Logger logger;

    /**
     * The three mutable fields below are determined by the {@link com.linkedin.venice.writer.VeniceWriter},
     * which populates them via {@link ChunkAwareCallback#setChunkingInfo(byte[], ByteBuffer[], ChunkedValueManifest)}.
     *
     * They should always be set for any message that ought to be persisted to the storage engine, i.e.: any callback
     * where the {@link #writeFunction} is present.
     */
    private byte[] key = null;
    private ChunkedValueManifest chunkedValueManifest = null;
    private ByteBuffer[] chunks = null;

    public LeaderProducerMessageCallback(PartitionConsumptionState consumptionState, String feedTopic, String versionTopic,
        int partition, long consumedOffset, ReadyToServeCheck readyToServeCheck, Optional<WriteToStorageEngine> writeFunction,
        AggVersionedDIVStats versionedDIVStats, Logger logger) {
      this.partitionConsumptionState = consumptionState;
      this.feedTopic = feedTopic;
      this.versionTopic = versionTopic;
      this.partition = partition;
      this.consumedOffset = consumedOffset;
      this.readyToServeChecker = readyToServeCheck;
      this.writeFunction = writeFunction;
      this.versionedDIVStats = versionedDIVStats;
      this.logger = logger;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        logger.error("Leader failed to send out message to version topic when consuming " + feedTopic + " partition " + partition, e);
        String storeName = Version.parseStoreFromKafkaTopicName(versionTopic);
        int version = Version.parseVersionFromKafkaTopicName(versionTopic);
        versionedDIVStats.recordLeaderProducerFailure(storeName, version);
      } else {
        /**
         * Leader would update the storage engine only after successfully producing the message.
         */
        if (writeFunction.isPresent()) {
          // Sanity check
          if (null == key) {
            throw new IllegalStateException("key not initialized.");
          }
          if (null == chunkedValueManifest) {
            // We apply the write function as is only if it is a small value
            writeFunction.get().apply(key);
          } else {
            /**
             * For large values which have been chunked, we instead write the chunks
             *
             * N.B.: Some of the write path implementation details of chunking are spread between here and
             * {@link com.linkedin.venice.writer.VeniceWriter#putLargeValue(byte[], byte[], int, Callback, int, long)},
             * which is not ideal from a maintenance standpoint.
             *
             * TODO: Coalesce write path chunking logic to a single class, following the pattern of {@link ChunkingAdapter}.
             */

            // Sanity checks
            if (null == chunks) {
              throw new IllegalStateException("chunking info not initialized.");
            } else if (chunkedValueManifest.keysWithChunkIdSuffix.size() != chunks.length) {
              throw new IllegalStateException("chunkedValueManifest.keysWithChunkIdSuffix is not in sync with chunks.");
            }

            // Write all chunks to the storage engine
            int schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
            for (int i = 0; i < chunkedValueManifest.keysWithChunkIdSuffix.size(); i++) {
              ByteBuffer chunkKey = chunkedValueManifest.keysWithChunkIdSuffix.get(i);
              ByteBuffer chunkValue = chunks[i];
              prependHeaderAndWriteToStorageEngine(versionTopic, partition, ByteUtils.extractByteArray(chunkKey), chunkValue, schemaId);
            }

            // Write the manifest inside the top-level key
            schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
            ByteBuffer manifest = ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize(versionTopic, chunkedValueManifest));
            /**
             * The byte[] coming out of the {@link CHUNKED_VALUE_MANIFEST_SERIALIZER} is padded in front, so
             * that the put to the storage engine can avoid a copy, but we need to set the position to skip
             * the padding in order for this trick to work.
             */
            manifest.position(SCHEMA_HEADER_LENGTH);
            prependHeaderAndWriteToStorageEngine(versionTopic, partition, key, manifest, schemaId);
          }
        }

        // leader should keep track of the latest offset of the version topic
        partitionConsumptionState.getOffsetRecord().setOffset(recordMetadata.offset());
        // update the leader consumption offset
        if (consumedOffset >= 0) {
          partitionConsumptionState.getOffsetRecord().setLeaderTopicOffset(consumedOffset);
        }
        /**
         * Check whether it's ready to serve after successfully produce a message.
         *
         * Reason behind adding check in Kafka producer callback:
         * Only check whether it's ready to serve when consuming a message is not enough, especially for leader replica;
         * for example, if the lag threshold is 100, leader has successfully consumed 1000 messages and successfully
         * produced 800 messages, so currently the lag is 200 because the offset metadata will only be updated after
         * successfully producing the messages; however, if the real-time topic doesn't receive any new messages since
         * then, "isReadyToServe" will never be checked again, even though later the leader has successfully produced
         * the rest 200 messages, so the leader replica will never be online.
         */
        readyToServeChecker.apply(partitionConsumptionState);
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
