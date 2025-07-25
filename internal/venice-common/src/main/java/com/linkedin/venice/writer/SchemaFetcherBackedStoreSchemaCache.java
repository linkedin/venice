package com.linkedin.venice.writer;

import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SchemaFetcherBackedStoreSchemaCache {
  private static final Logger LOGGER = LogManager.getLogger(SchemaFetcherBackedStoreSchemaCache.class);
  private final StoreSchemaFetcher schemaFetcher;
  private final Map<Integer, Schema> valueSchemaMap = new VeniceConcurrentHashMap<>();
  private int latestOrSupersetSchemaId = -1;
  private Schema updateSchema = null;

  public SchemaFetcherBackedStoreSchemaCache(StoreSchemaFetcher schemaFetcher) {
    this.schemaFetcher = schemaFetcher;
  }

  public Schema getValueSchema(int valueSchemaId) {
    maybeUpdateSupersetSchema(valueSchemaId);
    return valueSchemaMap.get(valueSchemaId);
  }

  public Schema getSupersetSchema() {
    return valueSchemaMap.get(latestOrSupersetSchemaId);
  }

  public Schema getUpdateSchema() {
    return updateSchema;
  }

  public void maybeUpdateSupersetSchema(int valueSchemaId) {
    Schema valueSchema = valueSchemaMap.get(valueSchemaId);
    if (valueSchema != null) {
      return;
    }
    //

    /**
     * This means we are seeing a new value schema, hence perform a refresh on superset schema, its update schema as well
     * as the whole value schema map.
     * For partial update enabled store, {@link StoreSchemaFetcher#getLatestValueSchemaEntry()} will return superset
     * schema and its ID. For other store, it will return latest value schema.
     */
    SchemaEntry schemaEntry = schemaFetcher.getLatestValueSchemaEntry();
    if (schemaEntry != null) {
      latestOrSupersetSchemaId = schemaEntry.getId();
      valueSchemaMap.put(schemaEntry.getId(), schemaEntry.getSchema());
      try {
        updateSchema = schemaFetcher.getUpdateSchemaEntry(latestOrSupersetSchemaId).getSchema();
        LOGGER.info(
            "Update the update schema: {} associated with superset schema ID: {}",
            updateSchema,
            latestOrSupersetSchemaId);
      } catch (VeniceException e) {
        // For non-partial-update store, there is no update schema and we should swallow the exception as well.
        LOGGER.warn("Unable to refresh update schema with superset schema ID: {}", latestOrSupersetSchemaId, e);
      }
    }
    for (Map.Entry<Integer, Schema> entry: schemaFetcher.getAllValueSchemasWithId().entrySet()) {
      if (valueSchemaMap.putIfAbsent(entry.getKey(), entry.getValue()) == null) {
        LOGGER.info("Add value schema id: {} to cache", entry.getKey());
      }
    }
  }

  public void close() throws IOException {
    valueSchemaMap.clear();
    schemaFetcher.close();
  }
}
