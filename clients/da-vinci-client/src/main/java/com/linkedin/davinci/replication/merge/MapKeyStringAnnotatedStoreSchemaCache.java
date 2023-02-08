package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedStringMapDerivedSchemaEntry;
import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedStringMapValueSchemaEntry;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;


/**
 * This class is a wrapper schema repository class, which is only used by merge conflict resolver in A/A store ingestion
 * task. This class will annotate value schema and partial update schema so when performing Map collection merging,
 * Map field's key can be deserialized into String type and thus is comparable for DCR purpose. Without this wrapper or
 * user annotation, map fields will have key in UTF-8 type which is not comparable.
 * This wrapper class only implements retrieval of the value schema (including the latest value schema and superset schema)
 * and partial update schema as they are the only usage in merge conflict resolver. Other operations are not supported
 * intentionally to avoid unexpected behavior.
 */
public class MapKeyStringAnnotatedStoreSchemaCache {
  private final ReadOnlySchemaRepository internalSchemaRepo;
  private final String storeName;
  private final Map<Integer, SchemaEntry> valueSchemaEntryMapCache = new VeniceConcurrentHashMap<>();
  private final Map<String, DerivedSchemaEntry> partialUpdateSchemaEntryMapCache = new VeniceConcurrentHashMap<>();

  public MapKeyStringAnnotatedStoreSchemaCache(String storeName, ReadOnlySchemaRepository internalSchemaRepo) {
    this.storeName = storeName;
    this.internalSchemaRepo = internalSchemaRepo;
  }

  /**
   * Retrieve value schema of a store and annotate its map fields. The annotation will only be done once in the repository's
   * lifetime as the result is cached.
   */
  public SchemaEntry getValueSchema(int id) {
    return valueSchemaEntryMapCache.computeIfAbsent(id, k -> {
      SchemaEntry schemaEntry = internalSchemaRepo.getValueSchema(storeName, id);
      if (schemaEntry == null) {
        return null;
      }
      return getAnnotatedStringMapValueSchemaEntry(schemaEntry);
    });
  }

  /**
   * Retrieve the superset schema (if exists) or the latest value schema of a store and annotate its map fields.
   * The annotation will be done once for each superset or latest value schema as it will be cached for future usage.
   */
  public SchemaEntry getSupersetOrLatestValueSchema() {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetOrLatestValueSchema(storeName);
    if (schemaEntry == null) {
      return null;
    }
    return valueSchemaEntryMapCache
        .computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedStringMapValueSchemaEntry(schemaEntry));
  }

  /**
   * Retrieve the superset schema (if exists) and annotate its map fields.
   * The annotation will be done once for each superset schema as it will be cached for future usage.
   */
  public SchemaEntry getSupersetSchema() {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetSchema(storeName);
    if (schemaEntry != null) {
      SchemaEntry annotatedSchemaEntry = valueSchemaEntryMapCache
          .computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedStringMapValueSchemaEntry(schemaEntry));
      return annotatedSchemaEntry;
    } else {
      return null;
    }
  }

  /**
   * Retrieve partial update schema of a store and annotate its map fields.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public DerivedSchemaEntry getDerivedSchema(int valueSchemaId, int partialUpdateProtocolId) {
    String partialUpdateSchemaId = valueSchemaId + "-" + partialUpdateProtocolId;
    return partialUpdateSchemaEntryMapCache.computeIfAbsent(partialUpdateSchemaId, k -> {
      DerivedSchemaEntry derivedSchemaEntry =
          internalSchemaRepo.getDerivedSchema(storeName, valueSchemaId, partialUpdateProtocolId);
      if (derivedSchemaEntry == null) {
        return null;
      }
      return getAnnotatedStringMapDerivedSchemaEntry(derivedSchemaEntry);
    });
  }
}
