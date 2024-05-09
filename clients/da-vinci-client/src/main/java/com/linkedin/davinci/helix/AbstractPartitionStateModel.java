package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstraction of Helix State model behavior registry that defines how participants
 * (Storage node) fetch data from Kafka.
 *
 * Helix state model is state machine defined by "states" and transitions (edges from one
 * state to the other).
 *
 * In order to make Helix state machine work, 2 parts are required.
 * 1. A model definition that defines all available states and transitions.
 * 2. A behavior registry that defines what participant should do during transitions before
 * moving to next state.
 *
 * Currently, we support LeaderStandbyModel participant model. Check out {@link LeaderStandbySMD} for model
 * definition and {@link LeaderFollowerPartitionStateModel} for behavior registry.
 */
public abstract class AbstractPartitionStateModel extends StateModel {
  protected final Logger logger = LogManager.getLogger(getClass());
  private static final int RETRY_COUNT = 5;
  private static final int RETRY_DURATION_MS = 1000;
  private static final int WAIT_PARTITION_ACCESSOR_TIME_OUT_MS = (int) TimeUnit.MINUTES.toMillis(5);

  private final VeniceIngestionBackend ingestionBackend;
  private final ReadOnlyStoreRepository storeRepository;
  private final VeniceStoreVersionConfig storeAndServerConfigs;
  private final int partition;
  private final String storePartitionDescription;
  private final CompletableFuture<HelixPartitionStatusAccessor> partitionStatusAccessorFuture;
  private final String instanceName;

  private HelixPartitionStatusAccessor partitionPushStatusAccessor;

  public AbstractPartitionStateModel(
      VeniceIngestionBackend ingestionBackend,
      ReadOnlyStoreRepository storeRepository,
      VeniceStoreVersionConfig storeAndServerConfigs,
      int partition,
      CompletableFuture<HelixPartitionStatusAccessor> accessorFuture,
      String instanceName) {
    this.ingestionBackend = ingestionBackend;
    this.storeRepository = storeRepository;
    this.storeAndServerConfigs = storeAndServerConfigs;
    this.partition = partition;
    this.storePartitionDescription = Utils.getReplicaId(storeAndServerConfigs.getStoreVersionName(), partition);
    /**
     * We cannot block here because helix manager connection depends on the state model constructing in helix logic.
     * If we block here in the constructor, it will cause deadlocks.
     */
    this.partitionStatusAccessorFuture = accessorFuture;
    this.instanceName = instanceName;
  }

  protected void executeStateTransition(Message message, NotificationContext context, Runnable handler) {
    executeStateTransition(message, context, handler, false);
  }

  protected void executeStateTransition(
      Message message,
      NotificationContext context,
      Runnable handler,
      boolean rollback) {
    String from = message.getFromState();
    String to = message.getToState();
    logEntry(from, to, message, context, rollback);
    // Change name to indicate which st is occupied this thread.
    Thread.currentThread().setName("Helix-ST-" + message.getResourceName() + "-" + partition + "-" + from + "->" + to);
    try {
      handler.run();
      logCompletion(from, to, message, context, rollback);
    } finally {
      // Once st is terminated, change the name to indicate this thread will not be occupied by this st.
      Thread.currentThread().setName("Inactive ST thread.");
    }
  }

  private void logEntry(String from, String to, Message message, NotificationContext context, boolean rollback) {
    logger.info(
        "{} {} transition from {} to {} for resource: {}, partition: {} invoked with message {} and context {}",
        getStorePartitionDescription(),
        rollback ? "rolling back" : "initiating",
        from,
        to,
        getStoreAndServerConfigs().getStoreVersionName(),
        partition,
        message,
        context);
  }

  private void logCompletion(String from, String to, Message message, NotificationContext context, boolean rollback) {
    logger.info(
        "{} {} transition from {} to {} for resource: {}, partition: {} invoked with message {} and context {}",
        getStorePartitionDescription(),
        rollback ? "rolled back" : "completed",
        from,
        to,
        getStoreAndServerConfigs().getStoreVersionName(),
        partition,
        message,
        context);
  }

  /**
   * Stop the consumption once a replica become ERROR.
   * This function only does clean up when any state transition fails, and it won't retry any state transition.
   */
  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
    executeStateTransition(message, context, () -> {
      logger.info(
          "{} met an error during state transition. Stop the running consumption.",
          getStorePartitionDescription(),
          error.getException());
      /**
       * When state transition fails, we shouldn't remove the corresponding database here since the database could
       * be either recovered by bounce/Helix Reset or completely dropped after going through 'ERROR' to 'DROPPED' state transition.
       *
       * Also the CV state will be updated to `ERROR` state, no need to remove it from here.
       */
      stopConsumption(false);
    }, true);
  }

  /**
   * Handles ERROR->DROPPED transition. Unexpected Transition. Unsubscribe the partition, removes partition's data
   * from local storage and clears the committed offset.
   */
  @Override
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> {
      try {
        removePartitionFromStoreGracefully();
      } catch (Exception e) {
        // Catch exception here to ensure state transition can complete ana avoid error->dropped->error->... loop
        logger.error("Encountered exception during the transition from ERROR to DROPPED.", e);
      }
    });
  }

  @Override
  public void reset() {
    try {
      stopConsumption(false);
    } catch (Exception e) {
      logger.error(
          "Error when trying to stop any ongoing consumption during reset for: {}",
          storePartitionDescription,
          e);
    }
    try {
      waitPartitionPushStatusAccessor();
    } catch (Exception e) {
      throw new VeniceException("Error when initializing partition push status accessor, " + "reset failed. ", e);
    }
    /**
     * reset the customized view to default state before instance gets shut down.
     */
    if (partitionPushStatusAccessor != null) {
      partitionPushStatusAccessor
          .updateReplicaStatus(storeAndServerConfigs.getStoreVersionName(), partition, ExecutionStatus.STARTED);
      partitionPushStatusAccessor.updateHybridQuotaReplicaStatus(
          storeAndServerConfigs.getStoreVersionName(),
          partition,
          HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
    }
  }

  /**
   * set up a new store partition and start the ingestion
   */
  protected void setupNewStorePartition() {
    /**
     * Waiting for push accessor to get initialized before starting ingestion.
     * Otherwise, it's possible that store ingestion starts without having the
     * accessor ready to get notified.
     */
    final Consumer<Double> waitTimeLogging = elapsedTimeInMs -> {
      // TODO Evaluate if debugLoggingEnabled config can be removed, as logging level can be changed at run time.
      if (storeAndServerConfigs.isDebugLoggingEnabled()) {
        logger.info(
            "Completed waiting for partition push status accessor for resource {}, partition {}. Total elapsed time: {} ms",
            storeAndServerConfigs.getStoreVersionName(),
            partition,
            elapsedTimeInMs);
      }
    };

    try (Timer t = Timer.run(waitTimeLogging)) {
      waitPartitionPushStatusAccessor();
      initializePartitionPushStatus();
    } catch (Exception e) {
      throw new VeniceException(
          "Error when initializing partition push status accessor, " + "will not start ingestion for store partition. ",
          e);
    }

    /**
     * If given store and partition have already existed in this node, openStoreForNewPartition is idempotent, so it
     * will not create them again.
     */
    final Consumer<Double> setupTimeLogging = elapsedTimeInMs -> {
      // TODO Evaluate if debugLoggingEnabled config can be removed, as logging level can be changed at run time.
      if (storeAndServerConfigs.isDebugLoggingEnabled()) {
        logger.info(
            "Completed starting the consumption for resource {} partition {}. Total elapsed time: {} ms",
            storeAndServerConfigs.getStoreVersionName(),
            partition,
            elapsedTimeInMs);
      }
    };
    try (Timer t = Timer.run(setupTimeLogging)) {
      ingestionBackend.startConsumption(storeAndServerConfigs, partition);
    }
  }

  protected void removePartitionFromStoreGracefully() {
    // Gracefully drop partition to drain the requests to this partition
    // This method is called during OFFLINE->DROPPED state transition. Due to Zk or other transient issues a store
    // version could miss ONLINE->OFFLINE transition and newer version could come online triggering this transition.
    // Since this removes the storageEngine from the map not doing an un-subscribe and dropping a partition could
    // lead to NPE and other issues.
    // Adding a topic unsubscribe call for those race conditions as a safeguard before dropping the partition.
    ingestionBackend.dropStoragePartitionGracefully(
        storeAndServerConfigs,
        partition,
        getStoreAndServerConfigs().getStopConsumptionTimeoutInSeconds());
    removeCustomizedState();
  }

  /**
   * remove customized state for this partition if there is one
   */
  protected void removeCustomizedState() {
    try {
      waitPartitionPushStatusAccessor();
    } catch (Exception e) {
      throw new VeniceException(
          "Error when initializing partition push status accessor, " + "failed to remove customized state. ",
          e);
    }
    if (partitionPushStatusAccessor != null) {
      String storeName = getStoreAndServerConfigs().getStoreVersionName();
      boolean isSuccess = false;
      int attempt = 0;
      while (!isSuccess && attempt <= RETRY_COUNT) {
        attempt++;
        if (attempt > 1) {
          logger.info("Wait {} ms to retry.", RETRY_DURATION_MS);
          Utils.sleep(RETRY_DURATION_MS);
          logger.info(
              "Attempt #{} in removing customized state for store: {}, partition: {}, on instance: {}",
              attempt,
              storeName,
              partition,
              instanceName);
        }
        try {
          partitionPushStatusAccessor.deleteReplicaStatus(storeName, partition);
          isSuccess = true;
        } catch (Exception e) {
          logger.error(
              "Error in removing customized state for store: {}, partition: {}, on instance: {}",
              storeName,
              partition,
              instanceName,
              e);
        }
      }
      if (!isSuccess) {
        String errorMsg = String.format(
            "Error: After attempting %s times, removing customized state for store: %s, partition: %s, on instance: %s",
            attempt,
            storeName,
            partition,
            instanceName);
        logger.error(errorMsg);
        throw new VeniceException(errorMsg);
      }
    }
  }

  protected void waitConsumptionCompleted(String resourceName, StateModelIngestionProgressNotifier notifier) {
    try {
      int bootstrapToOnlineTimeoutInHours;
      try {
        bootstrapToOnlineTimeoutInHours =
            getStoreRepo().getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName))
                .getBootstrapToOnlineTimeoutInHours();
      } catch (Exception e) {
        logger.warn(
            "Failed to fetch bootstrapToOnlineTimeoutInHours from store config for resource {}, using the default value of {} hours instead",
            resourceName,
            Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
        bootstrapToOnlineTimeoutInHours = Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
      }
      notifier.waitConsumptionCompleted(
          resourceName,
          partition,
          bootstrapToOnlineTimeoutInHours,
          getStoreIngestionService());
    } catch (InterruptedException e) {
      String errorMsg = "Can not complete consumption for resource:" + resourceName + " partition:" + partition;
      logger.error(errorMsg, e);
      // Please note, after throwing this exception, this node will become ERROR for this resource.
      throw new VeniceException(errorMsg, e);
    }
  }

  private void waitPartitionPushStatusAccessor() throws Exception {
    if (partitionPushStatusAccessor == null) {
      partitionPushStatusAccessor =
          partitionStatusAccessorFuture.get(WAIT_PARTITION_ACCESSOR_TIME_OUT_MS, TimeUnit.MILLISECONDS);
    }
  }

  private void initializePartitionPushStatus() {
    if (partitionPushStatusAccessor != null) {
      partitionPushStatusAccessor.updateReplicaStatus(
          storeAndServerConfigs.getStoreVersionName(),
          getPartition(),
          ExecutionStatus.NOT_STARTED);
    } else {
      throw new VeniceException("HelixPartitionStatusAccessor is expected not to be null.");
    }
  }

  protected void stopConsumption(boolean dropCVState) {
    CompletableFuture<Void> future = ingestionBackend.stopConsumption(storeAndServerConfigs, partition);
    if (dropCVState) {
      future.whenComplete((ignored, throwable) -> {
        if (throwable != null) {
          logger.warn(
              "Failed to stop consumption for resource: {}, partition: {}",
              storeAndServerConfigs.getStoreVersionName(),
              partition);
        }
        /**
         * Drop CV state anyway, and it may not work well in error condition, but it is fine since it is still a best effort.
         */
        removeCustomizedState();
      });
    }
  }

  protected VeniceIngestionBackend getIngestionBackend() {
    return ingestionBackend;
  }

  protected StoreIngestionService getStoreIngestionService() {
    return ingestionBackend.getStoreIngestionService();
  }

  protected ReadOnlyStoreRepository getStoreRepo() {
    return storeRepository;
  }

  protected VeniceStoreVersionConfig getStoreAndServerConfigs() {
    return storeAndServerConfigs;
  }

  protected int getPartition() {
    return partition;
  }

  public String getStorePartitionDescription() {
    return storePartitionDescription;
  }
}
