package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.storage.StorageService;
import javax.validation.constraints.NotNull;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
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

@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.ONLINE_STATE, HelixState.BOOTSTRAP_STATE})
public class VenicePartitionStateModel extends StateModel {
    private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class);

    private static final String STORE_PARTITION_DESCRIPTION_FORMAT = "%s-%d";

    private final VeniceStoreConfig storeConfig;
    private final int partition;
    private final KafkaConsumerService kafkaConsumerService;
    private final StorageService storageService;
    private final String storePartitionDescription;
    private final VeniceStateModelFactory.StateModelNotifier notifier;

    public VenicePartitionStateModel(@NotNull KafkaConsumerService kafkaConsumerService,
            @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition, VeniceStateModelFactory.StateModelNotifier notifer) {
        this.storeConfig = storeConfig;
        this.partition = partition;
        this.storageService = storageService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.storePartitionDescription = String
            .format(STORE_PARTITION_DESCRIPTION_FORMAT, storeConfig.getStoreName(), partition);
        this.notifier = notifer;
    }

    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
        logEntry(HelixState.BOOTSTRAP, HelixState.ONLINE, message, context);
        try {
            notifier.waitConsumptionCompleted(message.getResourceName(), partition);
        } catch (InterruptedException e) {
            String errorMsg =
                "Can not complete consumption for resource:" + message.getResourceName() + "par" + " partition:"
                    + partition;
            logger.error(errorMsg, e);
            // Please note, after throwing this exception, this node will become ERROR for this resource.
            throw new VeniceException(errorMsg, e);
        }
        logCompletion(HelixState.BOOTSTRAP, HelixState.ONLINE, message, context);
    }

    @Transition(to = HelixState.BOOTSTRAP_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
        logEntry(HelixState.OFFLINE, HelixState.BOOTSTRAP, message, context);
        // If given store and partition have already exist in this node, openStoreForNewPartition is idempotent so it
        // will not create them again.
        storageService.openStoreForNewPartition(storeConfig , partition);
        kafkaConsumerService.startConsumption(storeConfig, partition);
        notifier.startConsumption(message.getResourceName(), partition);
        logCompletion(HelixState.OFFLINE, HelixState.BOOTSTRAP, message, context);
       }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        logEntry(HelixState.ONLINE, HelixState.OFFLINE, message, context);
        kafkaConsumerService.stopConsumption(storeConfig, partition);
        logCompletion(HelixState.OFFLINE, HelixState.ONLINE, message, context);

    }

    private void removePartitionFromStore( ) {
        storageService.dropStorePartition(storeConfig, partition);
        kafkaConsumerService.resetConsumptionOffset(storeConfig, partition);
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
     * Stop the consumption once a replica become ERROR.
     */
    @Override
    public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
        logger.info(storePartitionDescription
            + " met an error during state transition. Stop the running consumption. Caused by:", error.getException());
        kafkaConsumerService.stopConsumption(storeConfig, partition);
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
     * Handles BOOTSTRAP->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
        logEntry(HelixState.BOOTSTRAP, HelixState.OFFLINE, message, context);
        // TODO stop is an async operation, we need to ensure that it's really stopped before state transition is completed.
        kafkaConsumerService.stopConsumption(storeConfig, partition);
        logCompletion(HelixState.BOOTSTRAP, HelixState.OFFLINE, message, context);
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
        logger.info(storePartitionDescription + " initiating transition from " + from.toString() + " to " + to
                .toString() + " Store" + storeConfig.getStoreName() + " Partition " + partition +
                " invoked with Message " + message + " and context " + context);
    }

    private void logCompletion(HelixState from, HelixState to, Message message, NotificationContext context) {
        logger.info(storePartitionDescription + " completed transition from " + from.toString() + " to "
                + to.toString() + " Store " + storeConfig.getStoreName() + " Partition " + partition +
                " invoked with Message " + message + " and context " + context);
    }
}
