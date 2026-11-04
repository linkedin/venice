package com.linkedin.venice.controller.placement;

import com.linkedin.venice.meta.Store;
import java.util.Objects;


/** Store, topic and destination details used to select a backend. */
public final class TopicPlacementContext {
  public enum MigrationCategory {
    UNKNOWN, BATCH_VT, HYBRID_VT
  }

  private final String clusterName;
  private final String storeName;
  private final String topicName;
  private final String destinationAddress;
  private final boolean legacyAlternative;
  private final MigrationCategory migrationCategory;

  /** Compatibility constructor: without owner metadata, the category is UNKNOWN. */
  public TopicPlacementContext(
      String clusterName,
      String storeName,
      String topicName,
      String destinationAddress,
      boolean legacyAlternative) {
    this(clusterName, storeName, topicName, destinationAddress, legacyAlternative, null);
  }

  /** Snapshots the owning user store's metadata; never retains the mutable store. */
  public TopicPlacementContext(
      String clusterName,
      String storeName,
      String topicName,
      String destinationAddress,
      boolean legacyAlternative,
      Store ownerStore) {
    this.clusterName = Objects.requireNonNull(clusterName);
    this.storeName = Objects.requireNonNull(storeName);
    this.topicName = Objects.requireNonNull(topicName);
    this.destinationAddress = destinationAddress;
    this.legacyAlternative = legacyAlternative;
    this.migrationCategory = classify(ownerStore);
  }

  private static MigrationCategory classify(Store store) {
    if (store == null) {
      return MigrationCategory.UNKNOWN;
    }
    if (store.isHybrid()) {
      return MigrationCategory.HYBRID_VT;
    }
    return MigrationCategory.BATCH_VT;
  }

  public MigrationCategory getMigrationCategory() {
    return migrationCategory;
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

  /** Static configuration's choice for this topic, for comparison rather than an active-placement veto. */
  public boolean isLegacyAlternative() {
    return legacyAlternative;
  }

}
