package com.linkedin.venice.helix;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.meta.Instance;
import java.util.StringJoiner;


/**
 * Replica is the basic unit to distribute data, replica is belong to a partition and running in a instance.
 */
public class Replica {
  private final Instance instance;
  private final int partitionId;
  private final String resource;
  private HelixState status;

  public Replica(
      @JsonProperty("instance") Instance instance,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("resource") String resource) {
    this.instance = instance;
    this.partitionId = partitionId;
    this.resource = resource;
  }

  public Instance getInstance() {
    return instance;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getResource() {
    return resource;
  }

  public HelixState getStatus() {
    return status;
  }

  /** Used by the JSON deserializer */
  public void setStatus(String status) {
    if (status != null) {
      this.status = HelixState.valueOf(status);
    }
  }

  public void setStatus(HelixState status) {
    this.status = status;
  }

  @Override
  @JsonIgnore
  public String toString() {
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("Host:").add(instance.getUrl());
    joiner.add("Resource:").add(resource);
    joiner.add("Partition:").add(Integer.toString(partitionId));
    if (status != null) {
      joiner.add("Status:").add(status.toString());
    }
    return joiner.toString();
  }
}
