package com.linkedin.venice.controllerapi;

import com.linkedin.venice.helix.Replica;
import java.util.List;


public class NodeReplicasReadinessResponse extends ControllerResponse {
  private NodeReplicasReadinessState nodeState;
  private List<Replica> unreadyReplicas;

  public NodeReplicasReadinessState getNodeState() {
    return nodeState;
  }

  public void setNodeState(NodeReplicasReadinessState val) {
    nodeState = val;
  }

  public List<Replica> getUnreadyReplicas() {
    return unreadyReplicas;
  }

  public void setUnreadyReplicas(List<Replica> val) {
    unreadyReplicas = val;
  }
}
