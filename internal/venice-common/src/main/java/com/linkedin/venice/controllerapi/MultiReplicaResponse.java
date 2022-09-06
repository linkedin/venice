package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.Version;


public class MultiReplicaResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private Replica[] replicas;
  private int version;

  @JsonIgnore
  public String getTopic() {
    return Version.composeKafkaTopic(getName(), getVersion());
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public Replica[] getReplicas() {
    return replicas;
  }

  public void setReplicas(Replica[] nodes) {
    this.replicas = nodes;
  }
}
