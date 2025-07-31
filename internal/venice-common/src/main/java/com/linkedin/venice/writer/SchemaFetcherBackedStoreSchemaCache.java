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


/**
 * This class uses a {@link StoreSchemaFetcher} to fetch value / update schema and supserset schema ID and store into
 * cache for fast retrieval.
 * Note that it does NOT automatically refresh schema information. Once a new value schema is registered in the backend,
 * superset schema can also be changed. User of this class can invoke {@link #maybeUpdateSupersetSchema(int)} to
 * explicitly check against an incoming value schema and potentially update schema information if the value schema is not
 * present in the local cache.
 */
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
    return getValueSchemaMap().get(valueSchemaId);
  }

  public Schema getSupersetSchema() {
    if (latestOrSupersetSchemaId <= 0) {
      refreshSchemaInformation();
    }
    return getValueSchemaMap().get(latestOrSupersetSchemaId);
  }

  /**
   * Return the latest known superset value schema ID for partial update store.
   * For non-partial-update store, this method will return the latest known value schema ID.
   */
  public int getLatestOrSupersetSchemaId() {
    if (latestOrSupersetSchemaId <= 0) {
      refreshSchemaInformation();
    }
    return latestOrSupersetSchemaId;
  }

  /**
   * Return the latest update schema associated with the superset value schema ID.
   */
  public Schema getUpdateSchema() {
    if (updateSchema == null) {
      refreshSchemaInformation();
    }
    return updateSchema;
  }

  public void maybeUpdateSupersetSchema(int valueSchemaId) {
    Schema valueSchema = getValueSchemaMap().get(valueSchemaId);
    if (valueSchema != null) {
      return;
    }
    /**
     * This means we are seeing a new value schema, hence perform a refresh on superset schema, its update schema as well
     * as the whole value schema map.
     * For partial update enabled store, {@link StoreSchemaFetcher#getLatestValueSchemaEntry()} will return superset
     * schema and its ID. For other store, it will return latest value schema.
     */
    refreshSchemaInformation();
  }

  /**
   * Perform a mandatory refresh of the store's schema information from {@link StoreSchemaFetcher} and update cache information.
   */
  public void refreshSchemaInformation() {
    SchemaEntry schemaEntry = getSchemaFetcher().getLatestValueSchemaEntry();
    if (schemaEntry != null) {
      latestOrSupersetSchemaId = schemaEntry.getId();
      getValueSchemaMap().put(schemaEntry.getId(), schemaEntry.getSchema());
      try {
        updateSchema = getSchemaFetcher().getUpdateSchemaEntry(latestOrSupersetSchemaId).getSchema();
        LOGGER.info(
            "Update the update schema: {} associated with superset schema ID: {}",
            updateSchema,
            latestOrSupersetSchemaId);
      } catch (VeniceException e) {
        // For non-partial-update store, there is no update schema and we should swallow the exception as well.
        LOGGER.warn("Unable to refresh update schema with superset schema ID: {}", latestOrSupersetSchemaId, e);
      }
    }
    for (Map.Entry<Integer, Schema> entry: getSchemaFetcher().getAllValueSchemasWithId().entrySet()) {
      if (getValueSchemaMap().putIfAbsent(entry.getKey(), entry.getValue()) == null) {
        LOGGER.info("Add value schema ID: {}, schema: {} into store schema cache", entry.getKey(), entry.getValue());
      }
    }
  }

  public void close() throws IOException {
    valueSchemaMap.clear();
    schemaFetcher.close();
  }

  StoreSchemaFetcher getSchemaFetcher() {
    return schemaFetcher;
  }

  Map<Integer, Schema> getValueSchemaMap() {
    return valueSchemaMap;
  }
}
