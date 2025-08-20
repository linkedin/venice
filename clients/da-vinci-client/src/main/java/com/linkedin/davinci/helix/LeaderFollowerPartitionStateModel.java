package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


/**
 * Venice partition's state model to manage Leader/Follower(Standby) state transitions.
 *
 * Offline (Initial State) -> Follower (ideal state) -> Leader (ideal state)
 *
 * There is only at most one leader at a time and it is elected from the follower. At present,
 * Followers and Leader behave the same in the read path. However, in the write path, leader
 * will take extra work. See {@link LeaderFollowerStoreIngestionTask} for more details.
 *
 * There is an optional latch between Offline to Follower transition. The latch is only placed if the
 * version state model served is the current version or the future version but completed already.
 * (During cluster rebalancing or SN rebouncing) Since Helix rebalancer only refers to state model
 * to determine the rebalancing time. The latch is a safeguard to prevent Helix "over-rebalancing"
 * the cluster and failing the read traffic for the current version or when a deferred swap rolls
 * forward the future version. The latch is released when ingestion has caught up the lag or the
 * ingestion has reached the last known offset of VT.
 */
@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = { HelixState.LEADER_STATE, HelixState.STANDBY_STATE })
public class LeaderFollowerPartitionStateModel extends AbstractPartitionStateModel {
  /**
   * For each role transition between leader and follower, assign an unique and increasing leader session id for
   * the role transition action; we have one state model for one partition, so this session ID only works for the
   * partition in this state model. The session Id is only used in state transition between LEADER state and STANDBY
   * state (STANDBY -> LEADER or LEADER -> STANDBY)
   *
   * In extreme cases, Helix might be back and forth on promoting a replica to leader and demoting it back to follower;
   * in order to handle such behavior, the ingestion task should be able to skip all the old state transition command
   * and directly go to the role (Leader or Follower) that Helix recently assigns to it. To achieve this goal, server
   * should compare the session ID inside the consumer action and compare it with the latest session ID in the state
   * model, skip the action if it's invalid.
   */
  private final AtomicLong leaderSessionId = new AtomicLong(0L);

  private final LeaderFollowerIngestionProgressNotifier notifier;
  private final ParticipantStateTransitionStats stateTransitionStats;

  private final HeartbeatMonitoringService heartbeatMonitoringService;

  public LeaderFollowerPartitionStateModel(
      IngestionBackend ingestionBackend,
      VeniceStoreVersionConfig storeAndServerConfigs,
      int partition,
      LeaderFollowerIngestionProgressNotifier notifier,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName,
      ParticipantStateTransitionStats stateTransitionStats,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    super(
        ingestionBackend,
        metadataRepo,
        storeAndServerConfigs,
        partition,
        partitionPushStatusAccessorFuture,
        instanceName,
        stateTransitionStats);
    this.notifier = notifier;
    this.stateTransitionStats = stateTransitionStats;
    this.heartbeatMonitoringService = heartbeatMonitoringService;
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> {
      String resourceName = message.getResourceName();
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);
      Store store = getStoreRepo().getStoreOrThrow(storeName);
      int currentVersion = store.getCurrentVersion();
      boolean isCurrentVersion = currentVersion == version;

      // A future version is ready to serve if it's status is either PUSHED or ONLINE
      // PUSHED is set for future versions of a target region push with deferred swap
      // ONLINE is set for future versions of a push with deferred swap
      boolean isFutureVersionReady = Utils.isFutureVersionReady(resourceName, getStoreRepo());
      /**
       * For current version and already completed future versions, firstly create a latch, then start ingestion and wait
       * for ingestion completion to make sure that the state transition waits until this new replica finished consuming
       * before asked to serve requests. Also, if we start ingestion first before creating the latch, ingestion completion
       * might be reported before latch creation, and latch will never be released until timeout, resulting in error replica.
       */
      if (isCurrentVersion || isFutureVersionReady) {
        notifier.startConsumption(resourceName, getPartition());
      }
      try {
        long startTimeForSettingUpNewStorePartitionInNs = System.nanoTime();
        setupNewStorePartition();
        logger.info(
            "Completed setting up the replica: {}. Total elapsed time: {} ms",
            Utils.getReplicaId(resourceName, getPartition()),
            LatencyUtils.getElapsedTimeFromNSToMS(startTimeForSettingUpNewStorePartitionInNs));
      } catch (Exception e) {
        logger.error("Failed to set up the new replica: {}", Utils.getReplicaId(resourceName, getPartition()), e);
        if (isCurrentVersion || isFutureVersionReady) {
          notifier.stopConsumption(resourceName, getPartition());
        }
        throw e;
      }
      heartbeatMonitoringService
          .updateLagMonitor(message.getResourceName(), getPartition(), HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
      if (isCurrentVersion || isFutureVersionReady) {
        waitConsumptionCompleted(resourceName, notifier);
      }
    });
  }

  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    LeaderSessionIdChecker checker = new LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
    executeStateTransition(
        message,
        context,
        () -> getIngestionBackend().getStoreIngestionService()
            .promoteToLeader(getStoreAndServerConfigs(), getPartition(), checker));
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    LeaderSessionIdChecker checker = new LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
    executeStateTransition(
        message,
        context,
        () -> getIngestionBackend().getStoreIngestionService()
            .demoteToStandby(getStoreAndServerConfigs(), getPartition(), checker));
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    heartbeatMonitoringService
        .updateLagMonitor(message.getResourceName(), getPartition(), HeartbeatLagMonitorAction.REMOVE_MONITOR);
    executeStateTransition(message, context, () -> stopConsumption(true));
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    heartbeatMonitoringService
        .updateLagMonitor(message.getResourceName(), getPartition(), HeartbeatLagMonitorAction.REMOVE_MONITOR);
    executeStateTransition(message, context, () -> {
      boolean isCurrentVersion = false;
      try {
        String resourceName = message.getResourceName();
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        int version = Version.parseVersionFromKafkaTopicName(resourceName);
        isCurrentVersion = getStoreRepo().getStoreOrThrow(storeName).getCurrentVersion() == version;
      } catch (VeniceNoStoreException e) {
        logger.warn(
            "Failed to determine if the resource is current version. Replica: {}",
            Utils.getReplicaId(message.getResourceName(), getPartition()),
            e);
      }
      if (isCurrentVersion) {
        // Only do graceful drop for current version resources that are being queried
        try {
          this.stateTransitionStats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
          // Gracefully drop partition to drain the requests to this partition
          Thread.sleep(TimeUnit.SECONDS.toMillis(getStoreAndServerConfigs().getPartitionGracefulDropDelaySeconds()));
        } catch (InterruptedException e) {
          throw new VeniceException("Got interrupted while waiting for graceful drop delay of serving version", e);
        } finally {
          this.stateTransitionStats.decrementThreadBlockedOnOfflineToDroppedTransitionCount();
        }
      }
      CompletableFuture<Void> dropPartitionFuture = removePartitionFromStoreGracefully();
      boolean waitForDropPartition = !dropPartitionFuture.isDone();
      try {
        if (waitForDropPartition) {
          this.stateTransitionStats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
        }
        dropPartitionFuture.get(WAIT_DROP_PARTITION_TIME_OUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new VeniceException("Got interrupted while waiting for drop partition future to complete", e);
      } catch (Exception e) {
        logger.error(
            "Exception while waiting for drop partition future during the transition from OFFLINE to DROPPED",
            e);
        throw new VeniceException("Got exception while waiting for drop partition future to complete", e);
      } finally {
        if (waitForDropPartition) {
          this.stateTransitionStats.decrementThreadBlockedOnOfflineToDroppedTransitionCount();
        }
      }
    });
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.DROPPED_STATE)
  public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
    // Venice is not supporting automatically partition recovery. No-oped here.
    logger.warn("unexpected state transition from DROPPED to OFFLINE");
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    // Venice is not supporting automatically partition recovery. No-oped here.
    logger.warn("unexpected state transition from ERROR to OFFLINE");
  }

  /**
   * A leader session id checker will be created for each consumer action;
   * server checks whether the session id is still valid before processing
   * the consumer action.
   */
  public static class LeaderSessionIdChecker {
    private final long assignedSessionId;
    private final AtomicLong latestSessionIdHandle;

    public LeaderSessionIdChecker(long assignedSessionId, AtomicLong latestSessionIdHandle) {
      this.assignedSessionId = assignedSessionId;
      this.latestSessionIdHandle = latestSessionIdHandle;
    }

    public boolean isSessionIdValid() {
      return assignedSessionId == latestSessionIdHandle.get();
    }
  }
}
