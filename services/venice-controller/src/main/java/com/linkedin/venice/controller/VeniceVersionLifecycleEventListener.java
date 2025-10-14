package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;


/**
 * Listener for lifecycle events of a Venice version.
 *
 * An instance of this interface is notified when a new version is created or when an existing version is deleted.
 *
 * The listener methods are called in order with concurrency control enforced by the store level write lock in
 * {@link HelixVeniceClusterResources}. i.e. The system ensures that only one version lifecycle event is active at a
 * time and guarantees the order of events.
 */
public interface VeniceVersionLifecycleEventListener {
  void onVersionCreated(Store store, Version version, boolean isSourceCluster);

  void onVersionDeleted(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingCurrentFromFuture(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingCurrentFromBackup(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingBackup(Store store, Version version, boolean isSourceCluster);
}
