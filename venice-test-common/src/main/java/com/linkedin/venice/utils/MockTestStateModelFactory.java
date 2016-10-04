package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


public class MockTestStateModelFactory extends StateModelFactory<StateModel> {
  private boolean isBlock = false;
  private Map<String, List<OnlineOfflineStateModel>> modelToModelListMap = new HashMap<>();
  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel(isBlock);
    CountDownLatch latch = new CountDownLatch(1);
    stateModel.latch = latch;
    String key = resourceName + "_" + HelixUtils.getPartitionId(partitionName);
    if(!modelToModelListMap.containsKey(key)){
      modelToModelListMap.put(key, new ArrayList<>());
    }
    modelToModelListMap.get(key).add(stateModel);
    return stateModel;
  }

  public void setBlockTransition(boolean isDelay){
    this.isBlock = isDelay;
  }

  public void makeTransitionCompleted(String resourceName, int partitionId) {
    for(OnlineOfflineStateModel model : modelToModelListMap.get(resourceName + "_" + partitionId)){
      model.latch.countDown();
    }
  }

  public List<OnlineOfflineStateModel> getModelList(String resourceName, int partitionId) {
    return modelToModelListMap.get(resourceName + "_" + partitionId);
  }

  public void makeTransitionError(String resourceName, int partitionId) {
    for(OnlineOfflineStateModel model : modelToModelListMap.get(resourceName + "_" + partitionId)){
      model.isError = true;
      model.latch.countDown();
    }
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE','BOOTSTRAP'}", initialState = "OFFLINE")
  public static class OnlineOfflineStateModel extends StateModel {
    private boolean isDelay;
    private boolean isError;

    OnlineOfflineStateModel(boolean isDelay){
      this.isDelay = isDelay;
    }

    private CountDownLatch latch;
    @Transition(from = "OFFLINE", to = "BOOTSTRAP")
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
    }

    @Transition(from = "BOOTSTRAP", to = "ONLINE")
    public void onBecomeOnlineFromBootstrap(Message message, NotificationContext context)
        throws InterruptedException {
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
  }
}
