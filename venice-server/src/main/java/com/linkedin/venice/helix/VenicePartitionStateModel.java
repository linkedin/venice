package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
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

@StateModelInfo(initialState = "OFFLINE", states = {
    "ONLINE", "ERROR"
})

public class VenicePartitionStateModel extends TransitionHandler {
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
  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes ONLINE from OFFLINE.");
    kafkaConsumerService.startConsumption(storeConfig, partition);
  }

  /**
   * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
   */
  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes OFFLINE from ONLINE.");
    kafkaConsumerService.stopConsumption(storeConfig, partition);
  }

  /**
   * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
   * offset for the partition as part of the transition.
   */
  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes DROPPED from OFFLINE.");
    kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
  }

  /**
   * Handles ERROR->OFFLINE transition.
   */
  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes OFFLINE from ERROR.");
    try {
      kafkaConsumerService.stopConsumption(storeConfig, partition);
    } catch (VeniceException ex) {
      ex.printStackTrace();
      logger.warn(ex.getMessage());
    }
  }

  /**
   * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribes the partition, removes partition's data
   * from local storage and clears the committed offset.
   */
  @Override
  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    logger.warn(
        storePartitionNodeDescription + " becomes DROPPED from ERROR transition invoked with message: " + message
            + " and context: " + context);
    kafkaConsumerService.stopConsumption(storeConfig, partition);
    kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
  }

}