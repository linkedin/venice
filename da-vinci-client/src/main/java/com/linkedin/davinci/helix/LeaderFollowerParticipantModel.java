package com.linkedin.davinci.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.utils.SystemTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.LEADER_STATE, HelixState.STANDBY_STATE})
public class LeaderFollowerParticipantModel extends AbstractParticipantModel {
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
  private AtomicLong leaderSessionId = new AtomicLong(0l);

  private final LeaderFollowerStateModelNotifier notifier;

  public LeaderFollowerParticipantModel(StoreIngestionService storeIngestionService, StorageService storageService,
      VeniceStoreConfig storeConfig, int partition, LeaderFollowerStateModelNotifier notifier,
      ReadOnlyStoreRepository metadataRepo,
      Optional<CompletableFuture<HelixPartitionStatusAccessor>> partitionPushStatusAccessorFuture, String instanceName) {
    super(storeIngestionService, metadataRepo, storageService, storeConfig, partition, new SystemTime(),
        partitionPushStatusAccessorFuture, instanceName);
    this.notifier = notifier;
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> {
      setupNewStorePartition();
      String resourceName = message.getResourceName();
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);

      // Placing a latch in the transition if this is the current version
      if (getMetaDataRepo().getStoreOrThrow(storeName).getCurrentVersion() == version
          && !VeniceSystemStoreUtils.isSystemStore(storeName)) {
        //startConsumption is called in order to create the latch
        notifier.startConsumption(resourceName, getPartition());
        waitConsumptionCompleted(resourceName, notifier);
      }
    });
  }

  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    LeaderSessionIdChecker checker = new LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
    executeStateTransition(message, context, () ->
      getStoreIngestionService().promoteToLeader(getStoreConfig(), getLeaderSubPartition(), checker));
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    LeaderSessionIdChecker checker = new LeaderSessionIdChecker(leaderSessionId.incrementAndGet(), leaderSessionId);
    executeStateTransition(message, context, () ->
      getStoreIngestionService().demoteToStandby(getStoreConfig(), getLeaderSubPartition(), checker));
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> stopConsumption());
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> removePartitionFromStoreGracefully());
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.DROPPED_STATE)
  public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
    //Venice is not supporting automatically partition recovery. No-oped here.
    logger.warn("unexpected state transition from DROPPED to OFFLINE");
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    //Venice is not supporting automatically partition recovery. No-oped here.
    logger.warn("unexpected state transition from ERROR to OFFLINE");
  }

  /**
   * A leader session id checker will be created for each consumer action;
   * server checks whether the session id is still valid before processing
   * the consumer action.
   */
  public static class LeaderSessionIdChecker {
    private long assignedSessionId;
    private AtomicLong latestSessionIdHandle;
    public LeaderSessionIdChecker(long assignedSessionId, AtomicLong latestSessionIdHandle) {
      this.assignedSessionId = assignedSessionId;
      this.latestSessionIdHandle = latestSessionIdHandle;
    }

    public boolean isSessionIdValid() {
      return assignedSessionId == latestSessionIdHandle.get();
    }
  }
}
