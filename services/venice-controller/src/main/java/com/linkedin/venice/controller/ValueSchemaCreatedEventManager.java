package com.linkedin.venice.controller;

import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ValueSchemaCreatedListener;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Holds and dispatches {@link ValueSchemaCreatedListener}s. {@link VeniceHelixAdmin} invokes
 * {@link #notifyValueSchemaCreated} after a successful, non-duplicate {@code addValueSchema} write.
 *
 * <p>Listener exceptions are caught and logged; they do not block subsequent listeners.
 */
public class ValueSchemaCreatedEventManager {
  private static final Logger LOGGER = LogManager.getLogger(ValueSchemaCreatedEventManager.class);

  private final List<ValueSchemaCreatedListener> listeners = new ArrayList<>();

  public void addListener(ValueSchemaCreatedListener listener) {
    listeners.add(listener);
  }

  void notifyValueSchemaCreated(String storeName, Store store, SchemaEntry schemaEntry, boolean isSourceCluster) {
    if (store == null) {
      LOGGER.warn(
          "Skipping value schema created event for store {} schema id {}: store snapshot is null, "
              + "likely deleted concurrently between the schema write and the listener dispatch.",
          storeName,
          schemaEntry == null ? "<null schemaEntry>" : schemaEntry.getId());
      return;
    }
    Store readOnlyStore = new ReadOnlyStore(store);
    for (ValueSchemaCreatedListener listener: listeners) {
      try {
        listener.handleValueSchemaCreated(readOnlyStore, schemaEntry, isSourceCluster);
      } catch (Exception e) {
        // Isolate listener failures; let fatal JVM errors (Error subclasses) propagate.
        LOGGER.error("Could not handle value schema creation event for store: {}", storeName, e);
      }
    }
  }
}
