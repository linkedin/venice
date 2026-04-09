package com.linkedin.venice.controller;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
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
 *
 * Read-only parameters: All listener methods receive read-only snapshots of the store and version.
 * Specifically, the framework passes {@link com.linkedin.venice.meta.ReadOnlyStore} and
 * {@link com.linkedin.venice.meta.ReadOnlyStore.ReadOnlyVersion} instances, typed here as {@link Store}
 * and {@link Version} for convenience. Do not attempt to mutate these objects; any mutation calls will
 * either be no-ops or throw exceptions. If a listener needs to perform writes, it must acquire the
 * appropriate resources and operate on mutable store/version objects outside of these callbacks.
 */
public interface VeniceVersionLifecycleEventListener {
  void onVersionCreated(Store store, Version version, boolean isSourceCluster);

  void onVersionDeleted(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingCurrentFromFuture(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingCurrentFromBackup(Store store, Version version, boolean isSourceCluster);

  void onVersionBecomingBackup(Store store, Version version, boolean isSourceCluster);

  /**
   * Invoked when a new value schema is successfully registered for a store.
   * This can be used to trigger jobs that need the latest value schema to avoid
   * data missing due to ser/de process.
   */
  void onValueSchemaCreated(Store store, boolean isSourceCluster);

  /**
   * Called once per cluster initialization to provide the schema repository.
   * Listeners that need schema lookups should override this method.
   */
  default void setSchemaRepository(ReadOnlySchemaRepository schemaRepository) {
  }
}
