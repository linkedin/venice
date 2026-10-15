package com.linkedin.venice.controller.placement;

import java.util.Objects;


/** Store, topic and destination details used to select a backend. */
public final class TopicPlacementContext {
  private final String clusterName;
  private final String storeName;
  private final String topicName;
  private final String destinationAddress;

  public TopicPlacementContext(String clusterName, String storeName, String topicName, String destinationAddress) {
    this.clusterName = Objects.requireNonNull(clusterName);
    this.storeName = Objects.requireNonNull(storeName);
    this.topicName = Objects.requireNonNull(topicName);
    this.destinationAddress = destinationAddress;
  }

  public String getClusterName() {
    return clusterName;
  }

  /** The owning user-store name, including when the topic belongs to its push-status store. */
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

}
