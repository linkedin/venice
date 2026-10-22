package com.linkedin.venice.controller.placement;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


/** Store, topic and destination details used to select a backend. */
public final class TopicPlacementContext {
  public enum MigrationCategory {
    UNKNOWN, BATCH_VT, HYBRID_VT, CDC_VT, DAVINCI_VT
  }

  // Persisted CDC metadata may outlive the removed before-image view implementation.
  private static final String CHANGE_CAPTURE_VIEW_CLASS = "com.linkedin.venice.views.ChangeCaptureView";

  private final String clusterName;
  private final String storeName;
  private final String topicName;
  private final String destinationAddress;
  private final boolean legacyAlternative;
  private final MigrationCategory migrationCategory;

  /** Compatibility constructor: without owner metadata, eligibility is unverified. */
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
    // Older metadata can omit views; this is equivalent to having no configured views.
    Map<String, ViewConfig> views = store.getViewConfigs();
    if (views == null) {
      views = Collections.emptyMap();
    }
    boolean malformedView = false;
    for (ViewConfig view: views.values()) {
      StoreViewConfig viewConfig = view == null ? null : view.dataModel();
      CharSequence rawClassName = viewConfig == null ? null : viewConfig.getViewClassName();
      String className = rawClassName == null ? null : rawClassName.toString();
      if (CHANGE_CAPTURE_VIEW_CLASS.equals(className)) {
        return MigrationCategory.CDC_VT;
      }
      malformedView |= className == null || className.trim().isEmpty();
    }
    if (malformedView) {
      return MigrationCategory.UNKNOWN;
    }
    if (store.isDaVinciPushStatusStoreEnabled() || store.getIsDavinciHeartbeatReported()
        || store.isBlobTransferEnabled()) {
      return MigrationCategory.DAVINCI_VT;
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
