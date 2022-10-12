package com.linkedin.venice.helix;

import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedStringMapDerivedSchemaEntry;
import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedStringMapValueSchemaEntry;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;


/**
 * This class is a wrapper schema repository class, which is only used by merge conflict resolver in A/A store ingestion
 * task. This class will annotate value schema and partial update schema so when performing Map collection merging,
 * Map field's key can be deserialized into String type and thus is comparable for DCR purpose. Without this wrapper or
 * user annotation, map fields will have key in UTF-8 type which is not comparable.
 */
public class StringAnnotatedHelixReadOnlySchemaRepository implements ReadOnlySchemaRepository {
  private final ReadOnlySchemaRepository internalSchemaRepo;
  private final Map<String, Map<Integer, SchemaEntry>> valueSchemaEntryMapCache = new VeniceConcurrentHashMap<>();
  private final Map<String, Map<String, DerivedSchemaEntry>> partialUpdateSchemaEntryMapCache =
      new VeniceConcurrentHashMap<>();

  public StringAnnotatedHelixReadOnlySchemaRepository(ReadOnlySchemaRepository internalSchemaRepo) {
    this.internalSchemaRepo = internalSchemaRepo;
  }

  @Override
  public void refresh() {
    internalSchemaRepo.refresh();
  }

  @Override
  public void clear() {
    internalSchemaRepo.clear();
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    return internalSchemaRepo.getKeySchema(storeName);
  }

  @Override
  /**
   * Retrieve value schema of a store and annotate its map fields. The annotation will only be done once in the repository's
   * lifetime as the result is cached.
   */
  public SchemaEntry getValueSchema(String storeName, int id) {
    if (!(valueSchemaEntryMapCache.containsKey(storeName) && valueSchemaEntryMapCache.get(storeName).containsKey(id))) {
      SchemaEntry schemaEntry = internalSchemaRepo.getValueSchema(storeName, id);
      SchemaEntry annotatedSchemaEntry = getAnnotatedStringMapValueSchemaEntry(schemaEntry);
      valueSchemaEntryMapCache.putIfAbsent(storeName, new VeniceConcurrentHashMap<>());
      valueSchemaEntryMapCache.get(storeName).put(id, annotatedSchemaEntry);
    }
    return valueSchemaEntryMapCache.get(storeName).get(id);
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    return internalSchemaRepo.hasValueSchema(storeName, id);
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    return internalSchemaRepo.getValueSchemaId(storeName, valueSchemaStr);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    return internalSchemaRepo.getValueSchemas(storeName);
  }

  @Override
  /**
   * Retrieve the superset schema (if exists) or the latest value schema of a store and annotate its map fields.
   * The annotation will be done everytime when this method is invoked, as the superset schema or the latest value schema
   * of the store can be changing from time to time.
   */
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetOrLatestValueSchema(storeName);
    return getAnnotatedStringMapValueSchemaEntry(schemaEntry);
  }

  @Override
  /**
   * Retrieve the superset schema (if exists) and annotate its map fields.
   * The annotation will be done everytime when this method is invoked, as the superset schema of the store can be
   * changing from time to time.
   */
  public Optional<SchemaEntry> getSupersetSchema(String storeName) {
    Optional<SchemaEntry> schemaEntryOptional = internalSchemaRepo.getSupersetSchema(storeName);
    if (schemaEntryOptional.isPresent()) {
      SchemaEntry schemaEntry = schemaEntryOptional.get();
      return Optional.of(getAnnotatedStringMapValueSchemaEntry(schemaEntry));
    } else {
      return schemaEntryOptional;
    }
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    return internalSchemaRepo.getDerivedSchemaId(storeName, derivedSchemaStr);
  }

  @Override
  /**
   * Retrieve partial update schema of a store and annotate its map fields. The annotation will only be done once in the
   * repository's lifetime as the result is cached.
   */
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int partialUpdateSchemaId) {
    String derivedSchemaId = valueSchemaId + "-" + partialUpdateSchemaId;
    if (!(partialUpdateSchemaEntryMapCache.containsKey(storeName)
        && partialUpdateSchemaEntryMapCache.get(storeName).containsKey(derivedSchemaId))) {
      DerivedSchemaEntry derivedSchemaEntry =
          internalSchemaRepo.getDerivedSchema(storeName, valueSchemaId, partialUpdateSchemaId);
      DerivedSchemaEntry annotatedEntry = getAnnotatedStringMapDerivedSchemaEntry(derivedSchemaEntry);
      partialUpdateSchemaEntryMapCache.putIfAbsent(storeName, new VeniceConcurrentHashMap<>());
      partialUpdateSchemaEntryMapCache.get(storeName).put(derivedSchemaId, annotatedEntry);
    }
    return partialUpdateSchemaEntryMapCache.get(storeName).get(derivedSchemaId);
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    return internalSchemaRepo.getDerivedSchemas(storeName);
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    return internalSchemaRepo.getLatestDerivedSchema(storeName, valueSchemaId);
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    return internalSchemaRepo.getReplicationMetadataSchema(storeName, valueSchemaId, replicationMetadataVersionId);
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    return internalSchemaRepo.getReplicationMetadataSchemas(storeName);
  }
}
