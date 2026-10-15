package com.linkedin.venice.controller.placement;

import java.util.Objects;


/** Store, topic and destination details used to select a backend. */
public final class TopicPlacementContext {
  public enum Operation {
    NEW_VERSION, RECOVERY
  }

  private final String clusterName;
  private final String storeName;
  private final String topicName;
  private final String destinationAddress;
  private final Operation operation;
  private final boolean legacyAlternative;

  public TopicPlacementContext(
      String clusterName,
      String storeName,
      String topicName,
      String destinationAddress,
      Operation operation,
      boolean legacyAlternative) {
    this.clusterName = Objects.requireNonNull(clusterName);
    this.storeName = Objects.requireNonNull(storeName);
    this.topicName = Objects.requireNonNull(topicName);
    this.destinationAddress = destinationAddress;
    this.operation = Objects.requireNonNull(operation);
    this.legacyAlternative = legacyAlternative;
  }

  public String getClusterName() {
    return clusterName;
  }

  /** The user-store name, without a topic version suffix. */
  public String getStoreName() {
    return storeName;
  }

  public String getTopicName() {
    return topicName;
  }

  /** The target TopicManager's address, which may differ from the controller's region. */
  public String getDestinationAddress() {
    return destinationAddress;
  }

  public Operation getOperation() {
    return operation;
  }

  public boolean isLegacyAlternative() {
    return legacyAlternative;
  }

}
