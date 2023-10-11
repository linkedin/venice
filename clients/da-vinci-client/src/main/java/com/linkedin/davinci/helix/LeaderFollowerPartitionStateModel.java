package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.LatencyUtils;
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
 * will take extra work. See {@link LeaderFollowerStoreIngestionTask}
 * for more details.
 *
 * There is an optional latch between Offline to Follower transition. The latch is only placed if the
 * version state model served is the current version. (During cluster rebalancing or SN rebouncing)
 * Since Helix rebalancer only refers to state model to determine the rebalancing time. The latch is
 * a safe guard to prevent Helix "over-rebalancing" the cluster and failing the read traffic. The
 * latch is released when ingestion has caught up the lag or the ingestion has reached the last known
 * offset of VT.
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
  private final ParticipantStateTransitionStats threadPoolStats;

  public LeaderFollowerPartitionStateModel(
      VeniceIngestionBackend ingestionBackend,
      VeniceStoreVersionConfig storeAndServerConfigs,
      int partition,
      LeaderFollowerIngestionProgressNotifier notifier,
      ReadOnlyStoreRepository metadataRepo,
      CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture,
      String instanceName,
      ParticipantStateTransitionStats threadPoolStats) {
    super(
        ingestionBackend,
        metadataRepo,
        storeAndServerConfigs,
        partition,
        partitionPushStatusAccessorFuture,
        instanceName);
    this.notifier = notifier;
    this.threadPoolStats = threadPoolStats;
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> {
      String resourceName = message.getResourceName();
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);
      boolean isRegularStoreCurrentVersion = getStoreRepo().getStoreOrThrow(storeName).getCurrentVersion() == version
          && !VeniceSystemStoreUtils.isSystemStore(storeName);

      /**
       * For regular store current version, firstly create a latch, then start ingestion and wait for ingestion
       * completion. Otherwise, if we start ingestion first, ingestion completion might be reported before latch
       * creation, and latch will never be released until timeout, resulting in error replica.
       */
      if (isRegularStoreCurrentVersion) {
        notifier.startConsumption(resourceName, getPartition());
      }
      try {
        long startTimeForSettingUpNewStorePartitionInNs = System.nanoTime();
        setupNewStorePartition();
        logger.info(
            "Completed setting up new store partition for {} partition {}. Total elapsed time: {} ms",
            resourceName,
            getPartition(),
            LatencyUtils.getLatencyInMS(startTimeForSettingUpNewStorePartitionInNs));
      } catch (Exception e) {
        logger.error("Failed to set up new store partition for {} partition {}", resourceName, getPartition(), e);
        if (isRegularStoreCurrentVersion) {
          notifier.stopConsumption(resourceName, getPartition());
        }
        throw e;
      }
      if (isRegularStoreCurrentVersion) {
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
        () -> getIngestionBackend().promoteToLeader(getStoreAndServerConfigs(), getPartition(), checker));
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    LeaderSessionIdChecker checker = new LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
    executeStateTransition(
        message,
        context,
        () -> getIngestionBackend().demoteToStandby(getStoreAndServerConfigs(), getPartition(), checker));
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> stopConsumption(true));
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> {
      boolean isCurrentVersion = false;
      try {
        String resourceName = message.getResourceName();
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        int version = Version.parseVersionFromKafkaTopicName(resourceName);
        isCurrentVersion = getStoreRepo().getStoreOrThrow(storeName).getCurrentVersion() == version;
      } catch (VeniceNoStoreException e) {
        logger.warn("Failed to determine if the resource is current version", e);
      }
      if (isCurrentVersion) {
        // Only do graceful drop for current version resources that are being queried
        try {
          this.threadPoolStats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
          // Gracefully drop partition to drain the requests to this partition
          Thread.sleep(TimeUnit.SECONDS.toMillis(getStoreAndServerConfigs().getPartitionGracefulDropDelaySeconds()));
        } catch (InterruptedException e) {
          throw new VeniceException("Got interrupted during state transition: 'OFFLINE' -> 'DROPPED'", e);
        } finally {
          this.threadPoolStats.decrementThreadBlockedOnOfflineToDroppedTransitionCount();
        }
      }
      removePartitionFromStoreGracefully();
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
