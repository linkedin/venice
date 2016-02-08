package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


/**
 * Venice Partition's State model to manage state transitions.
 *
 * The States along with their assumptions are as follows:
 * Offline (Initial State): The partition messages are not being consumed from Kafka.
 * Online (Ideal State): The partition messages are being consumed from Kafka.
 * Dropped (Implicit State): The partition has been unsubscribed from Kafka, the kafka offset for the partition
 *         has been reset to the beginning, Data related to Partition has been removed from local storage.
 * Error: A partition enters this state if there was some error while transitioning from one state to another.
 */

@StateModelInfo(initialState = State.OFFLINE_STATE, states = { State.ONLINE_STATE })
public class VenicePartitionStateModel extends StateModel {
  private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class.getName());

  private static final String STORE_PARTITION_NODE_DESCRIPTION_FORMAT = "%s-%d @ node %d";

  private final VeniceStoreConfig storeConfig;
  private final int partition;
  private final KafkaConsumerService kafkaConsumerService;
  private final StoreRepository storeRepository;
  private final String storePartitionNodeDescription;

  public VenicePartitionStateModel(KafkaConsumerService kafkaConsumerService, StoreRepository storeRepository,
      VeniceStoreConfig storeConfig, int partition) {

    this.storeConfig = storeConfig;
    this.partition = partition;
    this.storeRepository = storeRepository;
    this.kafkaConsumerService = kafkaConsumerService;
    this.storePartitionNodeDescription = String.format(STORE_PARTITION_NODE_DESCRIPTION_FORMAT,
        storeConfig.getStoreName(), partition, storeConfig.getNodeId());
  }

  /**
   * Handles OFFLINE->ONLINE transition. Subscribes to the partition as part of the transition.
   */
  @Transition(to = State.ONLINE_STATE, from = State.OFFLINE_STATE)
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    kafkaConsumerService.startConsumption(storeConfig, partition);
    AbstractStorageEngine storageEngine = storeRepository.getOrCreateLocalStorageEngine(storeConfig, partition);
    if (!storageEngine.containsPartition(partition)) {
      storageEngine.addStoragePartition(partition);
      kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
    }
    logger.info(storePartitionNodeDescription + " becomes ONLINE from OFFLINE.");
  }

  /**
   * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
   */
  @Transition(to = State.OFFLINE_STATE, from = State.ONLINE_STATE)
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    kafkaConsumerService.stopConsumption(storeConfig, partition);
    logger.info(storePartitionNodeDescription + " becomes OFFLINE from ONLINE.");
  }

  /**
   * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
   * offset for the partition as part of the transition.
   */
  @Transition(to = State.DROPPED_STATE, from = State.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
    logger.info(storePartitionNodeDescription + " becomes DROPPED from OFFLINE.");
  }

  /**
   * Handles ERROR->OFFLINE transition.
   */
  @Transition(to = State.OFFLINE_STATE, from = State.ERROR_STATE)
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    kafkaConsumerService.stopConsumption(storeConfig, partition);
    logger.info(storePartitionNodeDescription + " becomes OFFLINE from ERROR.");
  }

  /**
   * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribes the partition, removes partition's data
   * from local storage and clears the committed offset.
   */
  @Override
  @Transition(to = State.DROPPED_STATE, from = State.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    kafkaConsumerService.stopConsumption(storeConfig, partition);
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
    logger.warn(
        storePartitionNodeDescription + " becomes DROPPED from ERROR transition invoked with message: " + message
            + " and context: " + context);

  }

}