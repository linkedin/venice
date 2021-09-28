package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


/**
 * Venice Partition's State model to manage Online/Offline state transitions.
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
 *
 * During the BS -> Online transition, a latch is placed. The latch is released when ingestion has completed or
 * caught up the offset lag. This guarantees that whenever the state model turns to Online, it's viable to
 * serve the traffic.
 */

@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.ONLINE_STATE, HelixState.BOOTSTRAP_STATE})
public class OnlineOfflinePartitionStateModel extends AbstractPartitionStateModel {
    private final static AtomicInteger partitionNumberFromOfflineToBootstrap = new AtomicInteger(0);
    private final static AtomicInteger partitionNumberFromBootstrapToOnline = new AtomicInteger(0);
    private final OnlineOfflineIngestionProgressNotifier notifier;

    public OnlineOfflinePartitionStateModel(VeniceIngestionBackend ingestionBackend, VeniceStoreVersionConfig storeConfig, int partition,
                                            OnlineOfflineIngestionProgressNotifier notifier, ReadOnlyStoreRepository readOnlyStoreRepository,
                                            CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture, String instanceName) {
        super(ingestionBackend, readOnlyStoreRepository, storeConfig, partition, partitionPushStatusAccessorFuture, instanceName);
        this.notifier = notifier;
    }

    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
        partitionNumberFromBootstrapToOnline.incrementAndGet();
        executeStateTransition(message, context, () -> {
            long startTimeForWaitingConsumptionCompletedInNs = System.nanoTime();
            waitConsumptionCompleted(message.getResourceName(), notifier);
            logger.info("Consumption completed for " + message.getResourceName() + " partition " + getPartition()
                + ". Total elapsed time: " + LatencyUtils.getLatencyInMS(startTimeForWaitingConsumptionCompletedInNs) + " ms");
            });
        partitionNumberFromBootstrapToOnline.decrementAndGet();
    }

    @Transition(to = HelixState.BOOTSTRAP_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
        partitionNumberFromOfflineToBootstrap.incrementAndGet();
        executeStateTransition(message, context, () -> {
            long startTimeForSettingUpNewStorePartitionInNs = System.nanoTime();
            setupNewStorePartition();
            logger.info("Completed setting up new store partition for " + message.getResourceName() + " partition " + getPartition()
                + ". Total elapsed time: " + LatencyUtils.getLatencyInMS(startTimeForSettingUpNewStorePartitionInNs) + " ms");
            notifier.startConsumption(message.getResourceName(), getPartition());
        });
        partitionNumberFromOfflineToBootstrap.decrementAndGet();
    }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribe to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        executeStateTransition(message, context, this::stopConsumption);
    }

    /**
     * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
     * offset for the partition as part of the transition.
     */
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        //TODO Add some control logic here to maintain storage engine to avoid mistake operations.
        executeStateTransition(message, context, this::removePartitionFromStoreGracefully);
    }

    /**
     * Handles ERROR->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        executeStateTransition(message, context, this::stopConsumption);
    }

    /**
     * Handles BOOTSTRAP->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
        // TODO stop is an async operation, we need to ensure that it's really stopped before state transition is completed.
        executeStateTransition(message, context, this::stopConsumption);
    }

    public static int getNumberOfPartitionsFromOfflineToBootstrap() {
        return partitionNumberFromOfflineToBootstrap.get();
    }

    public static int getNumberOfPartitionsFromBootstrapToOnline() {
        return partitionNumberFromBootstrapToOnline.get();
    }
}
