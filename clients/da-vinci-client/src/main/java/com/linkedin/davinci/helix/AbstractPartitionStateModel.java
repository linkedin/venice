package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.PartitionReplicaIngestionContext;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
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
  // The drop partition consumer action may take longer than expected and eventually timeout if:
  // 1. The corresponding ingestion task is stuck and unable to process consumer actions.
  // 2. The corresponding ingestion task died due to errors.
  // We want to wait for drop partition because consumer actions are executed asynchronously w.r.t. Helix state
  // transitions. Currently, we open a partition synchronously during OFFLINE->STANDBY. This can be problematic and have
  // race conditions if we don't wait on the drop partition during X->DROPPED. This is because Helix can send the
  // following state transitions right after each other for the same partition to the same node:
  // X->DROPPED, OFFLINE->STANDBY
  protected static final long WAIT_DROP_PARTITION_TIME_OUT_MS = TimeUnit.MINUTES.toMillis(60);

  private final IngestionBackend ingestionBackend;
  private final ReadOnlyStoreRepository storeRepository;
  private final VeniceStoreVersionConfig storeAndServerConfigs;
  private final int partition;
  private final String storePartitionDescription;
  private final CompletableFuture<HelixPartitionStatusAccessor> partitionStatusAccessorFuture;
  private final String instanceName;
  private final ParticipantStateTransitionStats stateTransitionStats;
  private HelixPartitionStatusAccessor partitionPushStatusAccessor;
  private final String storeName;
  private final int versionNumber;
  private VeniceStoreType storeVersionType;

  public AbstractPartitionStateModel(
      IngestionBackend ingestionBackend,
      ReadOnlyStoreRepository storeRepository,
      VeniceStoreVersionConfig storeAndServerConfigs,
      int partition,
      CompletableFuture<HelixPartitionStatusAccessor> accessorFuture,
      String instanceName,
      ParticipantStateTransitionStats stateTransitionStats,
      String resourceName) {
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
    this.stateTransitionStats = stateTransitionStats;

    // Parse storeName and versionNumber from resourceName
    try {
      this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      this.versionNumber = Version.parseVersionFromKafkaTopicName(resourceName);
    } catch (Exception e) {
      throw new VeniceException("Failed to parse storeName and versionNumber from resourceName: " + resourceName, e);
    }
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
    // Change name to indicate which st is occupied this thread.
    String currentThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName("Helix-ST-" + message.getResourceName() + "-" + partition + "-" + from + "->" + to);
    logEntry(from, to, message, context, rollback);
    try {
      LogContext.setLogContext(storeAndServerConfigs.getLogContext());
      stateTransitionStats.trackStateTransitionStarted(from, to);
      handler.run();
      stateTransitionStats.trackStateTransitionCompleted(from, to);
      logCompletion(from, to, message, context, rollback);
    } finally {
      // Once st is terminated, change the name to indicate this thread will not be occupied by this st.
      Thread.currentThread().setName(currentThreadName);
    }
  }

  private void logEntry(String from, String to, Message message, NotificationContext context, boolean rollback) {
    logger.info(
        "{} replica {} {} transition from {} to {}. Message {} and context: {}",
        getReplicaTypeDescription(),
        getStorePartitionDescription(),
        rollback ? "rolling back" : "initiating",
        from,
        to,
        message,
        HelixTransitionTimingUtils.formatNotificationContext(context));
  }

  private void logCompletion(String from, String to, Message message, NotificationContext context, boolean rollback) {
    logger.info(
        "{} replica {} {} transition from {} to {}. Message {}. LatencyBreakdown: {}",
        getReplicaTypeDescription(),
        getStorePartitionDescription(),
        rollback ? "rolled back" : "completed",
        from,
        to,
        message,
        HelixTransitionTimingUtils.formatTransitionTiming(message, context));
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
        CompletableFuture<Void> dropPartitionFuture = removePartitionFromStoreGracefully();
        dropPartitionFuture.get(WAIT_DROP_PARTITION_TIME_OUT_MS, TimeUnit.MILLISECONDS);
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
      ingestionBackend.startConsumption(storeAndServerConfigs, partition, Optional.empty());
    }
  }

  protected CompletableFuture<Void> removePartitionFromStoreGracefully() {
    // Gracefully drop partition to drain the requests to this partition
    // This method is called during OFFLINE->DROPPED state transition. Due to Zk or other transient issues a store
    // version could miss ONLINE->OFFLINE transition and newer version could come online triggering this transition.
    // Since this removes the storageEngine from the map not doing an un-subscribe and dropping a partition could
    // lead to NPE and other issues.
    // Adding a topic unsubscribe call for those race conditions as a safeguard before dropping the partition.
    CompletableFuture<Void> dropPartitionFuture = ingestionBackend.dropStoragePartitionGracefully(
        storeAndServerConfigs,
        partition,
        getStoreAndServerConfigs().getStopConsumptionTimeoutInSeconds());
    removeCustomizedState();
    return dropPartitionFuture;
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
            getStoreRepo().getStoreOrThrow(storeName).getBootstrapToOnlineTimeoutInHours();
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

  protected IngestionBackend getIngestionBackend() {
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

  protected String getStoreName() {
    return storeName;
  }

  protected int getVersionNumber() {
    return versionNumber;
  }

  public String getStorePartitionDescription() {
    return storePartitionDescription;
  }

  /**
   * Returns a human-readable description of the replica type based on store type and version role.
   * Examples: "System store future version", "Batch store current version", "Hybrid store backup version"
   */
  String getReplicaTypeDescription() {
    VeniceStoreType type = getStoreVersionType();
    String role = getStoreVersionRole();
    String roleDesc = role.isEmpty() ? "unknown" : role.toLowerCase();
    return type.name() + " store " + roleDesc + " version";
  }

  /**
   * Returns the role of this store version (CURRENT, BACKUP, or FUTURE) as a string.
   * This is a best-effort operation - during rollbacks or metadata inconsistencies,
   * the reported role may be temporarily inaccurate (e.g., a backup version may briefly appear as FUTURE).
   * @return the store version role name, or empty string if store metadata is unavailable
   */
  protected String getStoreVersionRole() {
    try {
      Store store = getStoreRepo().getStore(storeName);
      if (store != null) {
        return PartitionReplicaIngestionContext.determineStoreVersionRole(versionNumber, store.getCurrentVersion())
            .name();
      }
    } catch (Exception e) {
      // Ignore exception since this is best-effort and mainly for logging purpose
    }
    return "";
  }

  protected VeniceStoreType getStoreVersionType() {
    if (storeVersionType == null) {
      storeVersionType = determineStoreType();
    }
    return storeVersionType;
  }

  /**
   * Infers the {@link VeniceStoreType} for the current store version.
   *
   * <p>This is a best-effort classification intended primarily for logging.
   * Any parsing or lookup failures are ignored and the method falls back to
   * {@link VeniceStoreType#BATCH}.</p>
   *
   * <ul>
   *   <li>{@link VeniceStoreType#SYSTEM} if the store is a system store</li>
   *   <li>{@link VeniceStoreType#HYBRID} if the referenced version is hybrid</li>
   *   <li>{@link VeniceStoreType#BATCH} for all other cases or on any error</li>
   * </ul>
   */
  private VeniceStoreType determineStoreType() {
    try {
      final String storeVersionName = storeAndServerConfigs.getStoreVersionName();
      final String storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);

      final Store store = getStoreRepo().getStore(storeName);
      if (store != null) {
        if (store.isSystemStore()) {
          return VeniceStoreType.SYSTEM;
        }

        final Version version = store.getVersion(versionNumber);
        if (version != null && version.isHybrid()) {
          return VeniceStoreType.HYBRID;
        }
      }
    } catch (Exception e) {
      // Swallow the exception since this is best-effort classification for logging.
    }

    return VeniceStoreType.BATCH;
  }
}
