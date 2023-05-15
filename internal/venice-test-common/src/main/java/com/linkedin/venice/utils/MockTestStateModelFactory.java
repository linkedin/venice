package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MockTestStateModelFactory extends StateModelFactory<StateModel> {
  private boolean isBlock = false;
  private static final Logger LOGGER = LogManager.getLogger(MockTestStateModelFactory.class);
  private final VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor;

  // we have a list of state model as the value because this factory could be shared by different participants
  private Map<String, List<OnlineOfflineStateModel>> modelToModelListMap = new HashMap<>();

  public MockTestStateModelFactory(VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor) {
    this.offlinePushStatusAccessor = offlinePushStatusAccessor;
  }

  public void stopAllStateModelThreads() {
    for (List<OnlineOfflineStateModel> onlineOfflineStateModels: modelToModelListMap.values()) {
      for (OnlineOfflineStateModel onlineOfflineStateModel: onlineOfflineStateModels) {
        onlineOfflineStateModel.killThreads();
      }
    }
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel(isBlock, offlinePushStatusAccessor);
    String key = resourceName + "_" + HelixUtils.getPartitionId(partitionName);
    synchronized (modelToModelListMap) {
      if (!modelToModelListMap.containsKey(key)) {
        modelToModelListMap.put(key, new ArrayList<>());
      }
    }
    modelToModelListMap.get(key).add(stateModel);
    return stateModel;
  }

  public void setBlockTransition(boolean isDelay) {
    this.isBlock = isDelay;
    for (List<OnlineOfflineStateModel> modelList: modelToModelListMap.values()) {
      for (OnlineOfflineStateModel model: modelList) {
        model.isDelay = isDelay;
      }
    }
  }

  public void makeTransitionCompleted(String resourceName, int partitionId) {
    for (OnlineOfflineStateModel model: modelToModelListMap.get(resourceName + "_" + partitionId)) {
      model.latch.countDown();
    }
  }

  public List<OnlineOfflineStateModel> getModelList(String resourceName, int partitionId) {
    return modelToModelListMap.getOrDefault(resourceName + "_" + partitionId, Collections.emptyList());
  }

  public void makeTransitionError(String resourceName, int partitionId) {
    for (OnlineOfflineStateModel model: modelToModelListMap.get(resourceName + "_" + partitionId)) {
      model.isError = true;
      model.latch.countDown();
    }
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE','BOOTSTRAP'}", initialState = "OFFLINE")
  public static class OnlineOfflineStateModel extends StateModel {
    private boolean isDelay;
    private boolean isError;
    private CountDownLatch latch;
    private final VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor;
    ConcurrentLinkedQueue<Thread> runningThreads = new ConcurrentLinkedQueue<Thread>();

    OnlineOfflineStateModel(boolean isDelay, VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor) {
      this.isDelay = isDelay;
      this.offlinePushStatusAccessor = offlinePushStatusAccessor;
    }

    public void killThreads() {
      for (Thread thread: runningThreads) {
        try {
          TestUtils.shutdownThread(thread);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        runningThreads.remove(thread);
      }
    }

    @Transition(from = "OFFLINE", to = "BOOTSTRAP")
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
      latch = new CountDownLatch(1);
    }

    @Transition(from = "BOOTSTRAP", to = "ONLINE")
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context) throws InterruptedException {
      if (isDelay) {
        // mock the delay during becoming online.
        latch.await();
      }
      if (isError) {
        isError = true;
        throw new VeniceException("ST is failed.");
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromBootstrap(Message message, NotificationContext context) {

    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    }

    @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) throws InterruptedException {

      String kafkaTopicName = message.getResourceName();
      int partitionId =
          Integer.parseInt(message.getPartitionName().substring(message.getPartitionName().lastIndexOf("_") + 1));
      String instanceId = message.getTgtName();
      offlinePushStatusAccessor
          .updateReplicaStatus(kafkaTopicName, partitionId, instanceId, ExecutionStatus.STARTED, "");

      // We need to do this delay in an async manner as Helix won't populate the EV until this state transition can
      // complete.
      Thread newThread = new Thread(() -> {
        latch = new CountDownLatch(1);

        if (isDelay) {
          // mock the delay during becoming online.
          try {
            boolean latchClosedBeforeTimeout = latch.await(30, TimeUnit.SECONDS);
            if (!latchClosedBeforeTimeout) {
              LOGGER.warn("StateTransition lock wait timed out!!");
            }
          } catch (InterruptedException e) {
            // Do nothing
          }
        }
        if (isError) {
          isError = true;
          throw new VeniceException("ST is failed.");
        }
        offlinePushStatusAccessor
            .updateReplicaStatus(kafkaTopicName, partitionId, instanceId, ExecutionStatus.COMPLETED, "");
        runningThreads.remove(this);
      });
      runningThreads.add(newThread);
      newThread.start();
    }

    @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    }

    @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    }

    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {

    }

    @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    }

    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.DROPPED_STATE)
    public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
    }

    @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    }
  }
}
