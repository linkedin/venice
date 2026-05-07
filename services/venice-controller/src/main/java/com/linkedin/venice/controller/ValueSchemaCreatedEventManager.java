package com.linkedin.venice.controller;

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

  void notifyValueSchemaCreated(String storeName, SchemaEntry schemaEntry, boolean isSourceCluster) {
    for (ValueSchemaCreatedListener listener: listeners) {
      try {
        listener.handleValueSchemaCreated(storeName, schemaEntry, isSourceCluster);
      } catch (Throwable e) {
        LOGGER.error("Could not handle value schema creation event for store: {}", storeName, e);
      }
    }
  }
}
