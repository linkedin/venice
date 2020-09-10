package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.kafka.KafkaClientFactory;
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
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.writer.ChunkAwareCallback;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.CommonClientConfigs;
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
  private final long newLeaderInactiveTime;
  private final boolean isNativeReplicationEnabled;
  private final String nativeReplicationSourceAddress;

  /**
   * A set of boolean that check if partitions owned by this task have released the latch.
   * This is an optional field and is only used while ingesting a topic that currently
   * served the traffic. The set is initialized as an empty set and will add partition
   * numbers once the partition has caught up.
   *
   * See {@link LeaderFollowerParticipantModel} for the details why we need latch for
   * certain resources.
   */

  public LeaderFollowerStoreIngestionTask(
      VeniceWriterFactory writerFactory,
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
      ReadOnlyStoreRepository metadataRepo,
      TopicManager topicManager,
      AggStoreIngestionStats storeIngestionStats,
      AggVersionedDIVStats versionedDIVStats,
      AggVersionedStorageIngestionStats versionedStorageIngestionStats,
      StoreBufferService storeBufferService,
      BooleanSupplier isCurrentVersion,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean isIncrementalPushEnabled,
      VeniceStoreConfig storeConfig,
      DiskUsage diskUsage,
      RocksDBMemoryStats rocksDBMemoryStats,
      boolean bufferReplayEnabledForHybrid,
      AggKafkaConsumerService aggKafkaConsumerService,
      VeniceServerConfig serverConfig,
      boolean isNativeReplicationEnabled,
      String nativeReplicationSourceAddress,
      int partitionId,
      ExecutorService cacheWarmingThreadPool,
      long startReportingReadyToServeTimestamp) {
    super(
        writerFactory,
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
        metadataRepo,
        topicManager,
        storeIngestionStats,
        versionedDIVStats,
        versionedStorageIngestionStats,
        storeBufferService,
        isCurrentVersion,
        hybridStoreConfig,
        isIncrementalPushEnabled,
        storeConfig,
        diskUsage,
        rocksDBMemoryStats,
        bufferReplayEnabledForHybrid,
        aggKafkaConsumerService,
        serverConfig,
        partitionId,
        cacheWarmingThreadPool,
        startReportingReadyToServeTimestamp);
    newLeaderInactiveTime = serverConfig.getServerPromotionToLeaderReplicaDelayMs();
    this.isNativeReplicationEnabled = isNativeReplicationEnabled;
    this.nativeReplicationSourceAddress = nativeReplicationSourceAddress;
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
        if (leaderTopic != null && !topic.equals(leaderTopic)) {
          consumerUnSubscribe(leaderTopic, partitionConsumptionState);

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
            versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
          }

          partitionConsumptionState.setConsumeRemotely(false);
          logger.info(consumerTaskId + " disabled remote consumption for partition " + partition);
          // subscribe back to VT/partition
          consumerSubscribe(topic, partitionConsumptionState, versionTopicOffset);
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
    long checkStartTimeInNS = System.nanoTime();
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
            offsetRecord.setLeaderConsumptionState(this.kafkaVersionTopic, offsetRecord.getOffset());

            logger.info(consumerTaskId + " promoted to leader for partition " + partition);

            defaultReadyToServeChecker.apply(partitionConsumptionState);
            return;
          }

          /**
           * Potential risk: it's possible that Kafka consumer would starve one of the partitions for a long
           * time even though there are new messages in it, so it's possible that the old leader is still producing
           * after waiting for 5 minutes; if it does happen, followers will detect upstream offset rewind by
           * a different producer GUID.
           */
          long lastTimestamp = getLastConsumedMessageTimestamp(kafkaVersionTopic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime) {
            /**
             * There isn't any new message from the old leader for at least {@link newLeaderInactiveTime} minutes,
             * this replica can finally be promoted to leader.
             */
            // unsubscribe from previous topic/partition
            consumerUnSubscribe(kafkaVersionTopic, partitionConsumptionState);

            OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
            if (null == offsetRecord.getLeaderTopic()) {
              /**
               * This replica is ready to actually switch to LEADER role, but it has been consuming from version topic
               * all the time, so set the leader consumption state to its version topic consumption state.
               */
              offsetRecord.setLeaderConsumptionState(kafkaVersionTopic, offsetRecord.getOffset());
            }

            /**
             * When a leader replica is actually promoted to LEADER role and if native replication is enabled, there could
             * be 3 cases:
             * 1. Local fabric hasn't consumed anything from remote yet; in this case, EOP is not received, source topic
             * still exists, leader will rebuild the consumer with the proper remote Kafka bootstrap server url and start
             * consuming remotely;
             * 2. Local fabric hasn't finish the consumption, but the host is restarted or leadership is handed over; in
             * this case, EOP is also not received, leader will resume the consumption from remote at the specific offset
             * which is checkpointed in the leader offset metadata;
             * 3. A re-balance happens, leader is bootstrapping a new replica for a version that is already online; in this
             * case, source topic might be removed already and the {@link isCurrentVersion} flag should be true; so leader
             * doesn't need to switch to remote, all the data messages have been replicated to local fabric, leader just
             * needs to consume locally.
             */
            if (isNativeReplicationEnabled && !partitionConsumptionState.isEndOfPushReceived() && !isCurrentVersion.getAsBoolean()) {
              if (null == nativeReplicationSourceAddress || nativeReplicationSourceAddress.length() == 0) {
                throw new VeniceException("Native replication is enabled but remote source address is not found");
              }
              // Do not enable remote consumption for the source fabric leader. Otherwise, it will produce extra messages.
              if (!nativeReplicationSourceAddress.equals(this.kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))) {
                partitionConsumptionState.setConsumeRemotely(true);
                logger.info(consumerTaskId + " enabled remote consumption for partition " + partition);
                // Add a new consumer using remote bootstrap address
                getConsumer(partitionConsumptionState);
              }
            }

            // subscribe to the new upstream
            consumerSubscribe(offsetRecord.getLeaderTopic(), partitionConsumptionState, offsetRecord.getLeaderOffset());
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

          /**
           * If LEADER is consuming remotely and EOP is already received, switch back to local fabrics.
           * TODO: The logic needs to be updated in order to support native replication for incremental push and hybrid.
           */
          if (partitionConsumptionState.consumeRemotely() && partitionConsumptionState.isEndOfPushReceived()) {
            // Unsubscribe from remote Kafka topic, but keep the consumer in cache.
            consumerUnSubscribe(kafkaVersionTopic, partitionConsumptionState);
            partitionConsumptionState.setConsumeRemotely(false);
            logger.info(consumerTaskId + " disabled remote consumption for partition " + partitionConsumptionState.getPartition());
            // Subscribe to local Kafka topic
            consumerSubscribe(kafkaVersionTopic, partitionConsumptionState, partitionConsumptionState.getOffsetRecord().getOffset());
          }

          TopicSwitch topicSwitch = partitionConsumptionState.getTopicSwitch();
          if (null == topicSwitch || currentLeaderTopic.equals(topicSwitch.sourceTopicName.toString())) {
            continue;
          }

          /**
           * Otherwise, execute the TopicSwitch message stored in metadata store if one of the below conditions is true:
           * 1. it has been 5 minutes since the last update in the current topic
           * 2. leader is consuming SR topic right now and TS wants leader to switch to another topic.
           */
          String newSourceTopicName = topicSwitch.sourceTopicName.toString();
          partition = partitionConsumptionState.getPartition();
          lastTimestamp = getLastConsumedMessageTimestamp(currentLeaderTopic, partition);
          if (LatencyUtils.getElapsedTimeInMs(lastTimestamp) > newLeaderInactiveTime
              || (Version.isStreamReprocessingTopic(currentLeaderTopic) && !Version.isStreamReprocessingTopic(newSourceTopicName))) {
            // leader switch topic
            long upstreamStartOffset = partitionConsumptionState.getOffsetRecord().getUpstreamOffset();
            if (upstreamStartOffset < 0) {
              if (topicSwitch.rewindStartTimestamp > 0) {
                int newSourceTopicPartition = partition;
                if (Version.isRealTimeTopic(newSourceTopicName)) {
                  newSourceTopicPartition = partition / amplificationFactor;
                }
                upstreamStartOffset = topicManager.getOffsetByTime(newSourceTopicName, newSourceTopicPartition, topicSwitch.rewindStartTimestamp);
                if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
                  // subscribe will seek to the next offset
                  upstreamStartOffset -= 1;
                }
              } else {
                upstreamStartOffset = OffsetRecord.LOWEST_OFFSET;
              }
            }

            // unsubscribe the old source and subscribe to the new source
            consumerUnSubscribe(currentLeaderTopic, partitionConsumptionState);

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
              versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
              notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), errorMsg, e);
              throw new VeniceException(errorMsg, e);
            }
            // subscribe to the new upstream
            partitionConsumptionState.getOffsetRecord().setLeaderConsumptionState(newSourceTopicName, upstreamStartOffset);
            consumerSubscribe(newSourceTopicName, partitionConsumptionState, upstreamStartOffset);

            if (!kafkaVersionTopic.equals(currentLeaderTopic)) {
              /**
               * This is for Samza grandfathering.
               *
               * Use default upstream offset for callback so that leader won't update its own leader consumed offset after
               * producing the TS message.
               */
              LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, currentLeaderTopic,
                  kafkaVersionTopic, partition, DEFAULT_UPSTREAM_OFFSET, defaultReadyToServeChecker, Optional.empty(), versionedDIVStats, logger);
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
    if (emitMetrics.get()) {
      storeIngestionStats.recordCheckLongRunningTasksLatency(storeName, LatencyUtils.getLatencyInMS(checkStartTimeInNS));
    }
  }

  @Override
  protected String getBatchWriteSourceAddress() {
    return isNativeReplicationEnabled ? nativeReplicationSourceAddress : this.kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
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

  /**
   * For the corresponding partition being tracked in `partitionConsumptionState`, if it's in LEADER state and it's
   * not consuming from version topic, it should produce the new message to version topic; besides, if LEADER is
   * consuming remotely, it should also produce to local fabric.
   *
   * If buffer replay is disable, all replicas will stick to version topic, no one is going to produce any message.
   */
  private boolean shouldProduceToVersionTopic(PartitionConsumptionState partitionConsumptionState) {
    boolean isLeader = partitionConsumptionState.getLeaderState().equals(LEADER);
    String leaderTopic = partitionConsumptionState.getOffsetRecord().getLeaderTopic();
    return isLeader && bufferReplayEnabledForHybrid &&
        (!kafkaVersionTopic.equals(leaderTopic) || partitionConsumptionState.consumeRemotely());
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
  protected void processStartOfPush(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    super.processStartOfPush(consumerRecord, controlMessage, partition, offset, partitionConsumptionState);

    /**
     * N.B.: This is expected to be the first time time we call {@link #getVeniceWriter()}, which is important
     *       because that function relies on the Start of Push's flags having been set in the
     *       {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState}. The flag setting happens in
     *       {@link StoreIngestionTask#processStartOfPush(ControlMessage, int, long, PartitionConsumptionState)},
     *       which is called at the start of the current function.
     *
     * Note update: the first time we call {@link #getVeniceWriter()} is different in various use cases:
     * 1. For hybrid store with L/F enabled, the first time a VeniceWriter is created is after leader switches to RT and
     *    consumes the first message; potential message type: SOS, EOS, data message.
     * 2. For store version generated by stream reprocessing push type, the first time is after leader switches to
     *    SR topic and consumes the first message; potential message type: SOS, EOS, data message (consider server restart).
     * 3. For store with native replication enabled, the first time is after leader switches to remote topic and start
     *    consumes the first message; potential message type: SOS, EOS, SOP, EOP, data message (consider server restart).
     */
    produceAndWriteToDatabase(consumerRecord, partitionConsumptionState, WriteToStorageEngine.NO_OP, (callback, sourceTopicOffset) ->
        getVeniceWriter().put(consumerRecord.key(), consumerRecord.value(), callback, partition, sourceTopicOffset));

  }

  @Override
  protected void processEndOfPush(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    /**
     * DIV pass through mode for producing EOP message.
     */
    produceAndWriteToDatabase(consumerRecord, partitionConsumptionState, WriteToStorageEngine.NO_OP, (callback, sourceTopicOffset) ->
        getVeniceWriter().put(consumerRecord.key(), consumerRecord.value(), callback, partition, sourceTopicOffset));

    /**
     * Before ending the batch write and converting the storage engine to read-only mode, make sure that all the messages
     * have been writen into storage engine by waiting on the last producer future.
     */
    try {
      long synchronizeStartTimeInNS = System.nanoTime();
      Future<RecordMetadata> lastLeaderProduceFuture =
          partitionConsumptionStateMap.get(partition).getLastLeaderProduceFuture();
      if (lastLeaderProduceFuture != null) {
        lastLeaderProduceFuture.get();
      }
      storeIngestionStats.recordLeaderProducerSynchronizeLatency(storeName, LatencyUtils.getLatencyInMS(synchronizeStartTimeInNS));
    } catch (Exception e) {
      versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
    }
    // End batch write.
    super.processEndOfPush(consumerRecord, controlMessage, partition, offset, partitionConsumptionState);
  }

  /**
   * For incremental push in hybrid store, the control messages should be produced to the version topic because leader
   * gets these control message inside real time topic, or in other words, `shouldProduceToVersionTopic` would return true.
   */
  @Override
  protected void processStartOfIncrementalPush(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      ControlMessage startOfIncrementalPush, int partition, long upstreamOffset, PartitionConsumptionState partitionConsumptionState) {
    super.processStartOfIncrementalPush(consumerRecord, startOfIncrementalPush, partition, upstreamOffset, partitionConsumptionState);

    produceAndWriteToDatabase(consumerRecord, partitionConsumptionState, WriteToStorageEngine.NO_OP, (callback, sourceTopicOffset) ->
        getVeniceWriter().asyncSendControlMessage(startOfIncrementalPush, partition, new HashMap<>(), callback, sourceTopicOffset));
  }

  @Override
  protected void processEndOfIncrementalPush(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      ControlMessage endOfIncrementalPush, int partition, long upstreamOffset, PartitionConsumptionState partitionConsumptionState) {
    super.processEndOfIncrementalPush(consumerRecord, endOfIncrementalPush, partition, upstreamOffset, partitionConsumptionState);

    produceAndWriteToDatabase(consumerRecord, partitionConsumptionState, WriteToStorageEngine.NO_OP, (callback, sourceTopicOffset) ->
        getVeniceWriter().asyncSendControlMessage(endOfIncrementalPush, partition, new HashMap<>(), callback, sourceTopicOffset));
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
      int newSourceTopicPartition = partition;
      if (Version.isRealTimeTopic(newSourceTopicName)) {
        newSourceTopicPartition = partition / amplificationFactor;
      }
      upstreamStartOffset = topicManager.getOffsetByTime(newSourceTopicName, newSourceTopicPartition, topicSwitch.rewindStartTimestamp);
      if (upstreamStartOffset != OffsetRecord.LOWEST_OFFSET) {
        // subscribe will seek to the next offset
        upstreamStartOffset -= 1;
      }
    }

    // Sync TopicSwitch message into metadata store
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
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
      storageMetadataService.put(kafkaVersionTopic, storeVersionState.get());

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
       * don't update the consumption state like leader topic until actually switching topic. The leaderTopic field
       * should be used to track the topic that leader is actually consuming.
       */
      if (!bufferReplayEnabledForHybrid) {
        /**
         * If buffer replay is disabled, everyone sticks to the version topic
         * and only consumes from version topic.
         */
        logger.info(consumerTaskId + " receives TopicSwitch message: new source topic: " + newSourceTopicName
            + "; start offset: " + upstreamStartOffset + "; however buffer replay is disabled, consumer will stick to " + kafkaVersionTopic);
        partitionConsumptionState.getOffsetRecord().setLeaderConsumptionState(newSourceTopicName, upstreamStartOffset);
        return;
      }
      /**
       * Update the latest upstream offset.
       */
      partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(upstreamStartOffset);
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
      defaultReadyToServeChecker.apply(partitionConsumptionState);
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
      if (kafkaValue.producerMetadata.upstreamOffset >= 0
          || (kafkaValue.leaderMetadataFooter != null && kafkaValue.leaderMetadataFooter.upstreamOffset >= 0)) {

        long newUpstreamOffset =
            kafkaValue.leaderMetadataFooter != null ? kafkaValue.leaderMetadataFooter.upstreamOffset : kafkaValue.producerMetadata.upstreamOffset;

        long previousUpstreamOffset = offsetRecord.getUpstreamOffset();

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
        if (newUpstreamOffset < previousUpstreamOffset) {
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
                    + " but recorded upstreamOffset is: %d. New GUID: %s; previous producer GUID: %s."
                    + " Multiple leaders are producing.", consumerRecord.partition(), newUpstreamOffset,
                previousUpstreamOffset,
                GuidUtils.getHexFromGuid(kafkaValue.producerMetadata.producerGUID),
                GuidUtils.getHexFromGuid(offsetRecord.getLeaderGUID()));

          boolean lossy = true;
          try {
            KafkaKey key = consumerRecord.key();
            KafkaMessageEnvelope envelope = consumerRecord.value();
            AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
            switch (MessageType.valueOf(envelope)) {
              case PUT:
                // Issue an read to get the current value of the key
                byte[] actualValue = storageEngine.get(consumerRecord.partition(), key.getKey());
                if (actualValue != null) {
                  int actualSchemaId = ByteUtils.readInt(actualValue, 0);
                  Put put = (Put) envelope.payloadUnion;
                  if (actualSchemaId == put.schemaId) {
                    // continue if schema Id is the same
                    if (ByteUtils.equals(put.putValue.array(), put.putValue.position(), actualValue,
                        SCHEMA_HEADER_LENGTH)) {
                      lossy = false;
                      logMsg +=
                          "\nBut this rewound PUT is not lossy because the data in the rewind message is the same as the data inside Venice";
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
                  logMsg +=
                      "\nBut this rewound DELETE is not lossy because the data in the rewind message is deleted already";
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
                versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
                VeniceException e = new VeniceException(logMsg);
                notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), logMsg, e);
                throw e;
              } else {
                logMsg += "\nDon't fail the job during streaming ingestion";
                logger.error(logMsg);
                versionedDIVStats.recordPotentiallyLossyLeaderOffsetRewind(storeName, versionNumber);
              }
            } else {
              logger.info(logMsg);
              versionedDIVStats.recordBenignLeaderOffsetRewind(storeName, versionNumber);
            }
          }
        }
        /**
         * Keep updating the upstream offset no matter whether there is a rewind or not; rewind could happen
         * to the true leader when the old leader doesn't stop producing.
         */
        if (kafkaValue.leaderMetadataFooter != null) {
          offsetRecord.setLeaderUpstreamOffset(kafkaValue.leaderMetadataFooter.upstreamOffset);
        } else {
          offsetRecord.setLeaderUpstreamOffset(kafkaValue.producerMetadata.upstreamOffset);
        }
      }
      // update leader producer GUID
      offsetRecord.setLeaderGUID(kafkaValue.producerMetadata.producerGUID);
      if (kafkaValue.leaderMetadataFooter != null) {
        offsetRecord.setLeaderHostId(kafkaValue.leaderMetadataFooter.hostName.toString());
      }
    }
  }

  @Override
  protected void produceAndWriteToDatabase(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      PartitionConsumptionState partitionConsumptionState, WriteToStorageEngine writeFunction, ProduceToTopic produceFunction) {
    int partition = consumerRecord.partition();
    if (shouldProduceToVersionTopic(partitionConsumptionState)) {
      String leaderTopic = consumerRecord.topic();
      long sourceTopicOffset = consumerRecord.offset();
      LeaderProducerMessageCallback callback = new LeaderProducerMessageCallback(partitionConsumptionState, leaderTopic,
          kafkaVersionTopic, partition, sourceTopicOffset, defaultReadyToServeChecker, Optional.of(writeFunction), versionedDIVStats, logger);
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

      // leaderTopic is the real-time topic now
      long leaderOffset;
      long lastOffsetInRealTimeTopic;
      if (amplificationFactor != 1) {
        /**
         * When amplificationFactor enabled, the RT topic and VT topics have different number of partition.
         * eg. if amplificationFactor == 10 and partitionCount == 2,
         *     the RT topic will have 2 partitions and VT topics will have 20 partitions.
         * No 1-to-1 mapping between the RT topic and VT topics partition, we can not calculate the offset difference.
         * To measure the offset difference between 2 types of topics, we go through leaderOffset in corresponding
         * sub-partitions and pick up the maximum value which means picking up the offset of the sub-partition seeing the most recent records in RT,
         * then use this value to compare against the offset in the RT topic.
         */
        int userPartition = partition / amplificationFactor;
        lastOffsetInRealTimeTopic = cachedLatestOffsetGetter.getOffset(leaderTopic, userPartition);
        leaderOffset = -1;
        for (int subPartition : PartitionUtils.getSubPartitions(Collections.singleton(userPartition), amplificationFactor)) {
          if (partitionConsumptionStateMap.get(subPartition) != null
              && partitionConsumptionStateMap.get(subPartition).getOffsetRecord() != null
              && partitionConsumptionStateMap.get(subPartition).getOffsetRecord().getLeaderOffset() >= 0) {
            leaderOffset = Math.max(leaderOffset, partitionConsumptionStateMap.get(subPartition).getOffsetRecord().getUpstreamOffset());
          }
        }
      } else {
        leaderOffset = offsetRecord.getUpstreamOffset();
        lastOffsetInRealTimeTopic = cachedLatestOffsetGetter.getOffset(leaderTopic, partition);
      }
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
      long storeVersionTopicLatestOffset = cachedLatestOffsetGetter.getOffset(kafkaVersionTopic, partition);
      long lag = storeVersionTopicLatestOffset - versionTopicConsumedOffset;
      if (shouldLogLag) {
        logger.info(String.format("Store buffer replay was disabled, and %s partition %d lag offset is: (Last VT offset [%d] - Last VT consumed offset [%d]) = Lag [%d]",
            consumerTaskId, partition, storeVersionTopicLatestOffset, versionTopicConsumedOffset, lag));
      }
      return lag;
    }
  }

  @Override
  protected void reportIfCatchUpBaseTopicOffset(PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();

    if (pcs.isEndOfPushReceived() && !pcs.isLatchReleased()) {
      if (cachedLatestOffsetGetter.getOffset(kafkaVersionTopic, partition) - 1 <= pcs.getOffsetRecord().getOffset()) {
        notificationDispatcher.reportCatchUpBaseTopicOffsetLag(pcs);

        /**
         * Relax to report completion
         *
         * There is a safe guard latch that is optionally replaced during Offline to Follower
         * state transition in order to prevent "over-rebalancing". However, there is
         * still an edge case that could make Venice lose all of the Online SNs.
         *
         * 1. Helix rebalances the leader replica of a partition; old leader shuts down,
         * so no one is replicating data from RT topic to VT;
         * 2. New leader is block while transisting to follower; after consuming the end
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
          notificationDispatcher.reportCompleted(pcs);
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
    // if record is from a RT topic, we select partitionConsumptionState of leaderSubPartition
    // to record the consuming status
    int partitionId = Version.isRealTimeTopic(record.topic()) ?
        record.partition() * amplificationFactor :  record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if(null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + kafkaVersionTopic + " Partition Id: " + partitionId);
      return false;
    }

    if (!partitionConsumptionState.getLeaderState().equals(LEADER)) {
      String recordTopic = record.topic();
      if (!kafkaVersionTopic.equals(recordTopic)) {
        throw new VeniceMessageException(
            consumerTaskId + " Current L/F state:" + partitionConsumptionState.getLeaderState() + "; partition: " + partitionId
                + "; Message retrieved from different topic. Expected " + this.kafkaVersionTopic + " Actual " + recordTopic);
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

  @Override
  protected void recordWriterStats(long producerTimestampMs, long brokerTimestampMs, long consumerTimestampMs,
      PartitionConsumptionState partitionConsumptionState) {
    if (isNativeReplicationEnabled) {
      // Emit latency metrics separately for leaders and followers
      long producerBrokerLatencyMs = Math.max(brokerTimestampMs - producerTimestampMs, 0);
      long brokerConsumerLatencyMs = Math.max(consumerTimestampMs - brokerTimestampMs, 0);
      long producerConsumerLatencyMs = Math.max(consumerTimestampMs - producerTimestampMs, 0);
      boolean isLeader = partitionConsumptionState.getLeaderState().equals(LEADER);
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
          partitionConsumptionState.getOffsetRecord().setLeaderUpstreamOffset(consumedOffset);
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
