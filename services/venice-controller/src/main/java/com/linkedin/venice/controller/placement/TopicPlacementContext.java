package com.linkedin.venice.controller.placement;

import java.util.Objects;


/**
 * A batch, non-system, non-hybrid user version topic creation request. The destination address comes from the
 * actual TopicManager, including when a parent creates a remote topic. Adapters must resolve destination fabric
 * and logical namespace from this address, never from the controller's execution region.
 */
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
  private final boolean topicExists;

  public TopicPlacementContext(
      String clusterName,
      String storeName,
      String topicName,
      String destinationAddress,
      Operation operation,
      boolean legacyAlternative,
      boolean topicExists) {
    this.clusterName = Objects.requireNonNull(clusterName);
    this.storeName = Objects.requireNonNull(storeName);
    this.topicName = Objects.requireNonNull(topicName);
    this.destinationAddress = destinationAddress;
    this.operation = Objects.requireNonNull(operation);
    this.legacyAlternative = legacyAlternative;
    this.topicExists = topicExists;
  }

  public String getClusterName() {
    return clusterName;
  }

  /** Exact canonical owning user-store name, not a version-topic name. */
  public String getStoreName() {
    return storeName;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getDestinationAddress() {
    return destinationAddress;
  }

  public Operation getOperation() {
    return operation;
  }

  public boolean isLegacyAlternative() {
    return legacyAlternative;
  }

  /** False also when the policy opts out of existence checks (for example in shadow mode). */
  public boolean isTopicExists() {
    return topicExists;
  }
}
