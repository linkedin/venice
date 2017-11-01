package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
 *
 * The scope of this state model is one replica of one partition, hosted on one storage node.
 * <p>
 * The States along with their assumptions are as follows:
 *
 * - Offline (Initial State): The partition messages are not being consumed from Kafka.
 * - Bootstrap (Intermediary State): The partition messages are being consumed from Kafka.
 * - Online (Ideal State): The partition messages are fully consumed from Kafka and checksumming has succeeded.
 * - Dropped (Implicit State): The partition has been unsubscribed from Kafka, the kafka offset for the partition
 *   has been reset to the beginning, Data related to Partition has been removed from local storage.
 * - Error: A partition enters this state if there was some error while transitioning from one state to another.
 */

@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.ONLINE_STATE, HelixState.BOOTSTRAP_STATE})
public class VenicePartitionStateModel extends StateModel {
    private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class);

    private static final String STORE_PARTITION_DESCRIPTION_FORMAT = "%s-%d";

    private final VeniceStoreConfig storeConfig;
    private final int partition;
    private final StoreIngestionService storeIngestionService;
    private final StorageService storageService;
    private final String storePartitionDescription;
    private final VeniceStateModelFactory.StateModelNotifier notifier;
    private final Time time;

    public VenicePartitionStateModel(@NotNull StoreIngestionService storeIngestionService,
        @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition,
        VeniceStateModelFactory.StateModelNotifier notifier) {
        this(storeIngestionService, storageService, storeConfig, partition, notifier, new SystemTime());
    }

    public VenicePartitionStateModel(@NotNull StoreIngestionService storeIngestionService,
        @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition,
        VeniceStateModelFactory.StateModelNotifier notifier, Time time) {
        this.storeConfig = storeConfig;
        this.partition = partition;
        this.storageService = storageService;
        this.storeIngestionService = storeIngestionService;
        this.storePartitionDescription = String
            .format(STORE_PARTITION_DESCRIPTION_FORMAT, storeConfig.getStoreName(), partition);
        this.notifier = notifier;
        this.time = time;
    }

    private void executeStateTransition(Message message, NotificationContext context,
        Runnable handler) {
        String from = message.getFromState();
        String to = message.getToState();
        logEntry(from, to, message, context);
        // Change name to indicate which st is occupied this thread.
        Thread.currentThread()
            .setName("Helix-ST-" + message.getResourceName() + "-" + partition + "-" + from + "->" + to);
        try {
            handler.run();
            logCompletion(from, to, message, context);
        } finally {
            // Once st is terminated, change the name to indicate this thread will not be occupied by this st.
            Thread.currentThread().setName("Inactive ST thread.");
        }
    }

    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
        executeStateTransition(message, context, () -> {
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
        });
    }

    @Transition(to = HelixState.BOOTSTRAP_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
        executeStateTransition(message, context, () -> {
            // If given store and partition have already exist in this node, openStoreForNewPartition is idempotent so it
            // will not create them again.
            storageService.openStoreForNewPartition(storeConfig, partition);
            storeIngestionService.startConsumption(storeConfig, partition);
            notifier.startConsumption(message.getResourceName(), partition);
        });
    }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribes to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> {
            storeIngestionService.stopConsumption(storeConfig, partition);
        });
    }

    private void removePartitionFromStore() {
        try {
            /**
             * Since un-subscription is an asynchronous process, so we would like to make sure current partition is not
             * being consuming before dropping store partition.
             *
             * Otherwise, a {@link com.linkedin.venice.exceptions.PersistenceFailureException} could be thrown here:
             * {@link com.linkedin.venice.store.AbstractStorageEngine#put(Integer, byte[], byte[])}.
             */
            makeSurePartitionIsNotConsuming();
        } catch (Exception e) {
            logger.error("Met error while waiting for partition to stop consuming", e);
        }
        // Catch exception separately to ensure reset consumption offset would be executed for sure.
        try {
            storageService.dropStorePartition(storeConfig, partition);
        } catch (Exception e) {
            logger.error(
                "Met error while dropping the partition:" + partition + " in store:" + storeConfig.getStoreName());
        }
        storeIngestionService.resetConsumptionOffset(storeConfig, partition);
    }

    /**
     * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
     * offset for the partition as part of the transition.
     */
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        //TODO Add some control logic here to maintain storage engine to avoid mistake operations.
        executeStateTransition(message, context, ()-> {
            removePartitionFromStore();
        });
    }

    /**
     * Stop the consumption once a replica become ERROR.
     */
    @Override
    public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
        executeStateTransition(message, context, ()-> {
            logger.info(storePartitionDescription + " met an error during state transition. Stop the running consumption. Caused by:",
                error.getException());
            storeIngestionService.stopConsumption(storeConfig, partition);
        });
    }

    /**
     * Handles ERROR->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()->{
            storeIngestionService.stopConsumption(storeConfig, partition);
        });
    }

    /**
     * Handles BOOTSTRAP->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> {
            // TODO stop is an async operation, we need to ensure that it's really stopped before state transition is completed.
            storeIngestionService.stopConsumption(storeConfig, partition);
        });
    }

    /**
     * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribes the partition, removes partition's data
     * from local storage and clears the committed offset.
     */
    @Override
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> {
                try {
                    storeIngestionService.stopConsumption(storeConfig, partition);
                    removePartitionFromStore();
                } catch (Throwable e) {
                    // Catch throwable here to ensure state transition is completed to avoid enter into the infinite loop error->dropped->error->....
                    logger.error("Met error during the  transition.", e);
                }
        });
    }

    private void logEntry(String from, String to, Message message, NotificationContext context) {
        logger.info(storePartitionDescription + " initiating transition from " + from + " to " + to + " Store"
            + storeConfig.getStoreName() + " Partition " + partition +
            " invoked with Message " + message + " and context " + context);
    }

    private void logCompletion(String from, String to, Message message, NotificationContext context) {
        logger.info(storePartitionDescription + " completed transition from " + from + " to " + to + " Store "
            + storeConfig.getStoreName() + " Partition " + partition +
            " invoked with Message " + message + " and context " + context);
    }

    /**
     * This function is trying to wait for current partition of store stop consuming.
     */
    private void makeSurePartitionIsNotConsuming() throws InterruptedException {
        final int SLEEP_SECONDS = 3;
        final int RETRY_NUM = 100; // 5 mins
        int current = 0;
        while (current++ < RETRY_NUM) {
            if (!storeIngestionService.isPartitionConsuming(storeConfig, partition)) {
                return;
            }
            time.sleep(SLEEP_SECONDS * Time.MS_PER_SECOND);
        }
        throw new VeniceException("Partition: " + partition + " of store: " + storeConfig.getStoreName() +
            " is still being consuming after waiting for it to stop a total of" + RETRY_NUM * SLEEP_SECONDS + " seconds.");
    }
}
