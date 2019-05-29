package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
public class VenicePartitionStateModel extends AbstractParticipantModel {
    private static final Logger logger = Logger.getLogger(VenicePartitionStateModel.class);

    private final VeniceStateModelFactory.StateModelNotifier notifier;
    private final ReadOnlyStoreRepository readOnlyStoreRepository;

    private final static AtomicInteger partitionNumberFromOfflineToBootstrap = new AtomicInteger(0);
    private final static AtomicInteger partitionNumberFromBootstrapToOnline = new AtomicInteger(0);

    public VenicePartitionStateModel(@NotNull StoreIngestionService storeIngestionService,
        @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition,
        VeniceStateModelFactory.StateModelNotifier notifier, ReadOnlyStoreRepository readOnlyStoreRepository) {
        this(storeIngestionService, storageService, storeConfig, partition, notifier, new SystemTime(), readOnlyStoreRepository);
    }

    public VenicePartitionStateModel(@NotNull StoreIngestionService storeIngestionService,
        @NotNull StorageService storageService, @NotNull VeniceStoreConfig storeConfig, int partition,
        VeniceStateModelFactory.StateModelNotifier notifier, Time time, ReadOnlyStoreRepository readOnlyStoreRepository) {
        super(storeIngestionService, storageService, storeConfig, partition, time);
        this.readOnlyStoreRepository = readOnlyStoreRepository;
        this.notifier = notifier;
    }

    @Transition(to = HelixState.ONLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) {
        partitionNumberFromBootstrapToOnline.incrementAndGet();
        executeStateTransition(message, context, () -> {
            try {
                int bootstrapToOnlineTimeoutInHours;
                try {
                    bootstrapToOnlineTimeoutInHours = readOnlyStoreRepository
                        .getStore(Version.parseStoreFromKafkaTopicName(message.getResourceName()))
                        .getBootstrapToOnlineTimeoutInHours();
                } catch (Exception e) {
                    logger.warn("Failed to fetch bootstrapToOnlineTimeoutInHours from store config for resource "
                        + message.getResourceName() + ", using the default value of "
                        + Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS + " hours instead");
                    bootstrapToOnlineTimeoutInHours = Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
                }
                notifier.waitConsumptionCompleted(message.getResourceName(), getPartition(), bootstrapToOnlineTimeoutInHours);
            } catch (InterruptedException e) {
                String errorMsg =
                    "Can not complete consumption for resource:" + message.getResourceName() + " partition:" + getPartition();
                logger.error(errorMsg, e);
                // Please note, after throwing this exception, this node will become ERROR for this resource.
                throw new VeniceException(errorMsg, e);
            }
        });
        partitionNumberFromBootstrapToOnline.decrementAndGet();
    }

    @Transition(to = HelixState.BOOTSTRAP_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
        partitionNumberFromOfflineToBootstrap.incrementAndGet();
        executeStateTransition(message, context, () -> {
            setupNewStorePartition();
            notifier.startConsumption(message.getResourceName(), getPartition());
        });
        partitionNumberFromOfflineToBootstrap.decrementAndGet();
    }

    /**
     * Handles ONLINE->OFFLINE transition. Unsubscribe to the partition as part of the transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ONLINE_STATE)
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> {
            getStoreIngestionService().stopConsumption(getStoreConfig(), getPartition());
        });
    }

    /**
     * Handles OFFLINE->DROPPED transition. Removes partition data from the local storage. Also, clears committed kafka
     * offset for the partition as part of the transition.
     */
    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        //TODO Add some control logic here to maintain storage engine to avoid mistake operations.
        executeStateTransition(message, context, ()-> removePartitionFromStoreGracefully());
    }

    /**
     * Handles ERROR->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()->{
            getStoreIngestionService().stopConsumption(getStoreConfig(), getPartition());
        });
    }

    /**
     * Handles BOOTSTRAP->OFFLINE transition.
     */
    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.BOOTSTRAP_STATE)
    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
        executeStateTransition(message, context, ()-> {
            // TODO stop is an async operation, we need to ensure that it's really stopped before state transition is completed.
            getStoreIngestionService().stopConsumption(getStoreConfig(), getPartition());
        });
    }

    public static int getNumberOfPartitionsFromOfflineToBootstrap() {
        return partitionNumberFromOfflineToBootstrap.get();
    }

    public static int getNumberOfPartitionsFromBootstrapToOnline() {
        return partitionNumberFromBootstrapToOnline.get();
    }
}
