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
   * Invoked after a new value schema is durably registered for a store.
   *
   * <p>Venice schemas evolve additively: new fields may be added with default values. Records already
   * written to the changelog topic were serialized with an older schema and lack the new fields.
   * A downstream consumer that was initialized with a stale {@code valueSchemaId} will:
   * <ol>
   *   <li>Deserialize new records correctly at the byte level (Avro reader/writer schema resolution
   *       fills in defaults for missing fields), but</li>
   *   <li>Re-serialize output using its stale writer schema, silently dropping any fields that did
   *       not exist when the consumer started — causing irreversible data loss with no error thrown.</li>
   * </ol>
   *
   * <p>Listeners that hold long-running consumers initialized with a specific {@code valueSchemaId}
   * should use this callback to restart those consumers with the latest effective schema so that
   * both deserialization and serialization use the same up-to-date schema going forward.
   *
   * <p>The {@code store} snapshot passed here reflects the state immediately after the new schema
   * is persisted. Listeners that need the new schema ID should resolve it via
   * {@link ReadOnlySchemaRepository} (injected through {@link #setSchemaRepository}) rather than
   * relying on the store object, as {@code store.getLatestSuperSetValueSchemaId()} returns -1 for
   * stores without write-compute enabled.
   */
  void onValueSchemaCreated(Store store, boolean isSourceCluster);

  /**
   * Called once per cluster initialization to provide the schema repository.
   * Listeners that need schema lookups should override this method.
   */
  default void setSchemaRepository(ReadOnlySchemaRepository schemaRepository) {
  }
}
