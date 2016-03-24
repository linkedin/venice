package com.linkedin.venice.controllerapi;

/**
 * Created by mwise on 3/17/16.
 */
public class StoreCreationResponse {
  private final String name;
  private final int version;
  private final String owner;
  private final int partitions;
  private final int replicas;

  public StoreCreationResponse(String name, int version, String owner, int partitions, int replicas){
    this.name = name;
    this.version = version;
    this.owner = owner;
    this.partitions = partitions;
    this.replicas = replicas;
  }

  public String getName() {
    return name;
  }

  public int getVersion() {
    return version;
  }

  public String getOwner() {
    return owner;
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplicas() {
    return replicas;
  }
}
