package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.SystemTime;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.LEADER_STATE, HelixState.STANDBY_STATE})
public class LeaderFollowerParticipantModel extends AbstractParticipantModel {

  public LeaderFollowerParticipantModel(StoreIngestionService storeIngestionService, StorageService storageService,
      VeniceStoreConfig storeConfig, int partition) {
    super(storeIngestionService, storageService, storeConfig, partition, new SystemTime());
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, () -> setupNewStorePartition());
  }

  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) throws Exception {
    //It's no-oped right now :D
    //TODO: writeCompute logic here
    executeStateTransition(message, context, () -> {});
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    //It's no-oped right now :D
    //TODO: writeCompute logic here
    executeStateTransition(message, context, () -> {});
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, context, ()-> {
      getStoreIngestionService().stopConsumption(getStoreConfig(), getPartition());
    });
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, context, ()-> removePartitionFromStoreGracefully());
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
}