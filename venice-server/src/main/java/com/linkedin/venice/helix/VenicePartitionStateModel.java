package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.controller.State;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
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

@StateModelInfo(initialState = State.OFFLINE_STATE, states = { State.ONLINE_STATE })

public class VenicePartitionStateModel extends TransitionHandler {
  private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class.getName());

  private static final String STORE_PARTITION_NODE_DESCRIPTION_FORMAT = "%s-%d @ node %d";

  private static final long RETRY_SEEK_TO_BEGINNING_WAIT_TIME_MS = 1000;
  private static final int RETRY_SEEK_TO_BEGINNING_COUNT = 10;

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
    logger.info(storePartitionNodeDescription + " becomes ONLINE from OFFLINE.");
    kafkaConsumerService.startConsumption(storeConfig, partition);
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(storeConfig.getStoreName());
    if (!storageEngine.containsPartition(partition)) {
      storageEngine.addStoragePartition(partition);
      tryResetConsumptionOffset(storeConfig, partition);
    }
  }

  /**
   * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
   */
  @Transition(to = State.OFFLINE_STATE, from = State.ONLINE_STATE)
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes OFFLINE from ONLINE.");
    kafkaConsumerService.stopConsumption(storeConfig, partition);
  }

  /**
   * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
   * offset for the partition as part of the transition.
   */
  @Transition(to = State.DROPPED_STATE, from = State.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    logger.info(storePartitionNodeDescription + " becomes DROPPED from OFFLINE.");
    kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
  }

  /**
   * Handles ERROR->OFFLINE transition.
   */
  @Transition(to = State.OFFLINE_STATE, from = State.ERROR_STATE)
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
  @Transition(to = State.DROPPED_STATE, from = State.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    logger.warn(
        storePartitionNodeDescription + " becomes DROPPED from ERROR transition invoked with message: " + message
            + " and context: " + context);
    kafkaConsumerService.stopConsumption(storeConfig, partition);
    kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
    storeRepository.getLocalStorageEngine(storeConfig.getStoreName()).removePartition(partition);
  }

  /**
   * TODO: Get rid of this workaround. Once, the Kafka Clients have been fixed.
   * Retries resetConsumptionOffset for a partition.
   * Because of current Kafka limitations, a partition is only subscribed after a poll call is made.
   * In our case, after the subscribed is called, a consumptionTask is spawned which polls to consumer for messages.
   * @param storeConfig
   * @param partition
   */
  private void tryResetConsumptionOffset(VeniceStoreConfig storeConfig, int partition) {
    int attempt = 0;
    while (true) {
      try {
        logger.info(storePartitionNodeDescription + " trying resetting offset - Attempt: " + attempt);
        kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
        return;
      } catch (IllegalArgumentException ex) {
        if (++attempt >= RETRY_SEEK_TO_BEGINNING_COUNT) {
          throw ex;
        }
        try {
          Thread.sleep(RETRY_SEEK_TO_BEGINNING_WAIT_TIME_MS);
        } catch (InterruptedException e) {
          logger.error("Unable to sleep while retrying resetConsumptionOffset. ", e);
        }
      }
    }
  }
}