package com.linkedin.venice.controllerapi;

import com.linkedin.venice.helix.Replica;
import java.util.List;


public class NodeReplicasReadinessResponse extends ControllerResponse {
  private boolean nodeReady;
  private List<Replica> unreadyReplicas;

  public boolean isNodeReady() { return nodeReady; }
  public void setNodeReady(boolean val) {
    nodeReady = val;
  }

  public List<Replica> getUnreadyReplicas() { return unreadyReplicas; }
  public void setUnreadyReplicas(List<Replica> val) {
    unreadyReplicas = val;
  }
}
