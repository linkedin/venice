package com.linkedin.venice.routerapi;

import com.linkedin.venice.controllerapi.ControllerResponse;
import java.util.List;


public class ResourceStateResponse extends ControllerResponse {
  private List<ReplicaState> replicaStates;
  private List<Integer> unretrievablePartitions;
  private boolean isReadyToServe;

  public List<ReplicaState> getReplicaStates() {
    return replicaStates;
  }

  public List<Integer> getUnretrievablePartitions() {
    return unretrievablePartitions;
  }

  public boolean isReadyToServe() {
    return isReadyToServe;
  }

  public void setReplicaStates(List<ReplicaState> replicaStates) {
    this.replicaStates = replicaStates;
  }

  public void setReadyToServe(boolean readyToServe) {
    isReadyToServe = readyToServe;
  }

  public void setUnretrievablePartitions(List<Integer> unretrievablePartitions) {
    this.unretrievablePartitions = unretrievablePartitions;
  }
}
