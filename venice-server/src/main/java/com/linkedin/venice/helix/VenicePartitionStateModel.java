package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.storage.StorageService;
import javax.validation.constraints.NotNull;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


/**
 * Venice Partition's State model to manage state transitions.
 * <p>
 * The States along with their assumptions are as follows: Offline (Initial State): The partition messages are not being
 * consumed from Kafka. Online (Ideal State): The partition messages are being consumed from Kafka. Dropped (Implicit
 * State): The partition has been unsubscribed from Kafka, the kafka offset for the partition has been reset to the
 * beginning, Data related to Partition has been removed from local storage. Error: A partition enters this state if
 * there was some error while transitioning from one state to another.
 */

@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.ONLINE_STATE})
public class VenicePartitionStateModel extends StateModel {
    private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class.getName());

    private static final String STORE_PARTITION_NODE_DESCRIPTION_FORMAT = "%s-%d @ node %d";

    private final VeniceStoreConfig storeConfig;
    private final int partition;
    private final KafkaConsumerService kafkaConsumerService;
    private final StorageService storageService;
    private final String storePartitionNodeDescription;

    public VenicePartitionStateModel(@NotNull KafkaConsumerService kafkaConsumerService,
            @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition) {
        this.storeConfig = storeConfig;
        this.partition = partition;
        this.storageService = storageService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.storePartitionNodeDescription = String
            .format(STORE_PARTITION_NODE_DESCRIPTION_FORMAT, storeConfig.getStoreName(), partition,
                storeConfig.getNodeId());
    }

    /**
     * Handles OFFLINE->ONLINE transition. Subscribes to the partition as part of the transition.
     */
    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        logEntry(HelixState.OFFLINE, HelixState.ONLINE, message, context);

        storageService.openStoreForNewPartition(storeConfig , partition);
        kafkaConsumerService.startConsumption(storeConfig, partition);
        logCompletion(HelixState.OFFLINE, HelixState.ONLINE, message, context);
    }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        logEntry(HelixState.ONLINE, HelixState.OFFLINE, message, context);
        kafkaConsumerService.stopConsumption(storeConfig, partition);
        logCompletion(HelixState.ONLINE, HelixState.OFFLINE, message, context);

    }

    private void removePartitionFromStore( ) {
        storageService.dropStorePartition(storeConfig, partition);
    }

    /**
     * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
     * offset for the partition as part of the transition.
     */
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        //TODO Add some control logic here to maintain storage engine to avoid mistake operations.
        logEntry(HelixState.OFFLINE, HelixState.DROPPED, message, context);
        removePartitionFromStore();
        logCompletion(HelixState.OFFLINE, HelixState.DROPPED, message, context);
    }

    /**
     * Handles ERROR->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        logEntry(HelixState.ERROR, HelixState.OFFLINE, message, context);
        kafkaConsumerService.stopConsumption(storeConfig, partition);
        logCompletion(HelixState.ERROR, HelixState.OFFLINE, message, context);
    }

    /**
     * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribes the partition, removes partition's data
     * from local storage and clears the committed offset.
     */
    @Override
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
        logEntry(HelixState.ERROR, HelixState.DROPPED, message, context);
        kafkaConsumerService.stopConsumption(storeConfig, partition);
        removePartitionFromStore();
        logCompletion(HelixState.ERROR, HelixState.DROPPED, message, context);
    }

    private void logEntry(HelixState from, HelixState to, Message message, NotificationContext context) {
        logger.info(storePartitionNodeDescription + " initiating transition from " + from.toString() + " to " + to
                .toString() + " Store " + storeConfig.getStoreName() + " Partition " + partition +
                " invoked with Message " + message + " and context " + context);
    }

    private void logCompletion(HelixState from, HelixState to, Message message, NotificationContext context) {
        logger.info(storePartitionNodeDescription + " completed transition from " + from.toString() + " to "
                + to.toString() + " Store " + storeConfig.getStoreName() + " Partition " + partition +
                " invoked with Message " + message + " and context " + context);
    }
}
