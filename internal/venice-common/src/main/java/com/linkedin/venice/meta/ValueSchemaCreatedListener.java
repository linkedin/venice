package com.linkedin.venice.meta;

import com.linkedin.venice.schema.SchemaEntry;


/**
 * Notified by the schema repository when a new value schema is registered for a store. Mirrors the
 * {@code StoreDataChangedListener} pattern on {@link ReadOnlyStoreRepository}.
 *
 * <p>Implementations must be non-blocking. Exceptions thrown from the callback are caught and
 * logged; they do not prevent subsequent listeners from running.
 *
 * <p>The {@code store} snapshot is read-only; mutation calls on it are no-ops or throw. Listeners
 * that need migration awareness can inspect {@link Store#isMigrationDuplicateStore()} on the
 * snapshot to filter source-vs-destination during a store migration.
 */
public interface ValueSchemaCreatedListener {
  /**
   * Fired after a new value schema is durably persisted. Not fired for duplicate schemas (i.e. when
   * the same schema was already registered).
   *
   * @param store       read-only snapshot of the store the schema belongs to
   * @param schemaEntry the schema that was just persisted (id + schema body)
   */
  void handleValueSchemaCreated(Store store, SchemaEntry schemaEntry);
}
