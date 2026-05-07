package com.linkedin.venice.meta;

import com.linkedin.venice.schema.SchemaEntry;


/**
 * Notified by the leader controller when a new value schema is registered for a store.
 *
 * <p>Implementations must be non-blocking. Exceptions thrown from the callback are caught and logged
 * by the dispatcher; they do not prevent subsequent listeners from running.
 */
public interface ValueSchemaCreatedListener {
  /**
   * Fired after a new value schema is durably persisted. Not fired for duplicate schemas (i.e. when
   * the same schema was already registered).
   *
   * <p>The {@code store} snapshot is read-only; mutation calls on it are no-ops or throw. Listeners
   * that need to modify state must do so on their own resources.
   *
   * @param store           read-only snapshot of the store the schema belongs to
   * @param schemaEntry     the schema that was just persisted (id + schema body)
   * @param isSourceCluster {@code true} if this cluster is the authoritative source for the store
   *                        at the time of the write. For non-migrating stores this is always
   *                        {@code true}; during migration the same registration fires on both
   *                        source and target controllers — listeners that should run only once
   *                        can filter on this flag.
   */
  void handleValueSchemaCreated(Store store, SchemaEntry schemaEntry, boolean isSourceCluster);
}
