package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
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
public class VenicePartitionStateModel extends AbstractParticipantModel {
    private final StateModelNotifier notifier;

    private final static AtomicInteger partitionNumberFromOfflineToBootstrap = new AtomicInteger(0);
    private final static AtomicInteger partitionNumberFromBootstrapToOnline = new AtomicInteger(0);

    public VenicePartitionStateModel(StoreIngestionService storeIngestionService,
        StorageService storageService, VeniceStoreConfig storeConfig, int partition,
        StateModelNotifier notifier, ReadOnlyStoreRepository readOnlyStoreRepository) {
        this(storeIngestionService, storageService, storeConfig, partition, notifier, new SystemTime(),
            readOnlyStoreRepository);
    }

    public VenicePartitionStateModel(StoreIngestionService storeIngestionService, StorageService storageService,
        VeniceStoreConfig storeConfig, int partition, StateModelNotifier notifier,
        ReadOnlyStoreRepository readOnlyStoreRepository,
        CompletableFuture<HelixPartitionPushStatusAccessor> partitionPushStatusAccessorCompletableFuture,
        String instanceName) {
        this(storeIngestionService, storageService, storeConfig, partition, notifier, new SystemTime(),
            readOnlyStoreRepository, partitionPushStatusAccessorCompletableFuture, instanceName);
    }

    public VenicePartitionStateModel(StoreIngestionService storeIngestionService, StorageService storageService,
        VeniceStoreConfig storeConfig, int partition, StateModelNotifier notifier, Time time,
        ReadOnlyStoreRepository readOnlyStoreRepository) {
        super(storeIngestionService, readOnlyStoreRepository, storageService, storeConfig, partition, time);
        this.notifier = notifier;
    }

    public VenicePartitionStateModel(StoreIngestionService storeIngestionService, StorageService storageService,
        VeniceStoreConfig storeConfig, int partition, StateModelNotifier notifier, Time time,
        ReadOnlyStoreRepository readOnlyStoreRepository,
        CompletableFuture<HelixPartitionPushStatusAccessor> partitionPushStatusAccessorFuture, String instanceName) {
        super(storeIngestionService, readOnlyStoreRepository, storageService, storeConfig, partition, time,
            partitionPushStatusAccessorFuture, instanceName);
        this.notifier = notifier;
    }

    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
        partitionNumberFromBootstrapToOnline.incrementAndGet();
        executeStateTransition(message, context, () ->
            waitConsumptionCompleted(message.getResourceName(), notifier));
        partitionNumberFromBootstrapToOnline.decrementAndGet();
    }

    @Transition(to = HelixState.BOOTSTRAP_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
        partitionNumberFromOfflineToBootstrap.incrementAndGet();
        executeStateTransition(message, context, () -> {
            setupNewStorePartition(false);
            notifier.startConsumption(message.getResourceName(), getPartition());
        });
        partitionNumberFromOfflineToBootstrap.decrementAndGet();
    }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribe to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> stopConsumption());
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
