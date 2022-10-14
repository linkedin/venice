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
public class MapKeyStringAnnotatedReadOnlySchemaRepository implements ReadOnlySchemaRepository {
  private final ReadOnlySchemaRepository internalSchemaRepo;
  private final Map<String, Map<Integer, SchemaEntry>> valueSchemaEntryMapCache = new VeniceConcurrentHashMap<>();
  private final Map<String, Map<String, DerivedSchemaEntry>> partialUpdateSchemaEntryMapCache =
      new VeniceConcurrentHashMap<>();

  public MapKeyStringAnnotatedReadOnlySchemaRepository(ReadOnlySchemaRepository internalSchemaRepo) {
    this.internalSchemaRepo = internalSchemaRepo;
  }

  @Override
  public void refresh() {
    internalSchemaRepo.refresh();
  }

  @Override
  public void clear() {
    valueSchemaEntryMapCache.clear();
    partialUpdateSchemaEntryMapCache.clear();
    internalSchemaRepo.clear();
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  /**
   * Retrieve value schema of a store and annotate its map fields. The annotation will only be done once in the repository's
   * lifetime as the result is cached.
   */
  public SchemaEntry getValueSchema(String storeName, int id) {
    Map<Integer, SchemaEntry> schemaMap =
        valueSchemaEntryMapCache.computeIfAbsent(storeName, x -> new VeniceConcurrentHashMap<>());
    return schemaMap.computeIfAbsent(id, k -> {
      SchemaEntry schemaEntry = internalSchemaRepo.getValueSchema(storeName, id);
      if (schemaEntry == null) {
        return null;
      }
      return getAnnotatedStringMapValueSchemaEntry(schemaEntry);
    });
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  /**
   * Retrieve the superset schema (if exists) or the latest value schema of a store and annotate its map fields.
   * The annotation will be done once for each superset or latest value schema as it will be cached for future usage.
   */
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetOrLatestValueSchema(storeName);
    if (schemaEntry == null) {
      return null;
    }
    Map<Integer, SchemaEntry> schemaMap =
        valueSchemaEntryMapCache.computeIfAbsent(storeName, x -> new VeniceConcurrentHashMap<>());
    return schemaMap.computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedStringMapValueSchemaEntry(schemaEntry));
  }

  @Override
  /**
   * Retrieve the superset schema (if exists) and annotate its map fields.
   * The annotation will be done once for each superset schema as it will be cached for future usage.
   */
  public Optional<SchemaEntry> getSupersetSchema(String storeName) {
    Optional<SchemaEntry> schemaEntryOptional = internalSchemaRepo.getSupersetSchema(storeName);
    if (schemaEntryOptional.isPresent()) {
      SchemaEntry schemaEntry = schemaEntryOptional.get();
      Map<Integer, SchemaEntry> schemaMap =
          valueSchemaEntryMapCache.computeIfAbsent(storeName, x -> new VeniceConcurrentHashMap<>());
      SchemaEntry annotatedSchemaEntry =
          schemaMap.computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedStringMapValueSchemaEntry(schemaEntry));
      return Optional.of(annotatedSchemaEntry);
    } else {
      return schemaEntryOptional;
    }
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  /**
   * Retrieve partial update schema of a store and annotate its map fields.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int partialUpdateProtocolId) {
    String partialUpdateSchemaId = valueSchemaId + "-" + partialUpdateProtocolId;
    Map<String, DerivedSchemaEntry> schemaMap =
        partialUpdateSchemaEntryMapCache.computeIfAbsent(storeName, k -> new VeniceConcurrentHashMap<>());
    return schemaMap.computeIfAbsent(partialUpdateSchemaId, k -> {
      DerivedSchemaEntry derivedSchemaEntry =
          internalSchemaRepo.getDerivedSchema(storeName, valueSchemaId, partialUpdateProtocolId);
      if (derivedSchemaEntry == null) {
        return null;
      }
      return getAnnotatedStringMapDerivedSchemaEntry(derivedSchemaEntry);
    });
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  /**
   * Retrieve the latest partial update schema of a store and annotate its map fields.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    DerivedSchemaEntry derivedSchemaEntry = internalSchemaRepo.getLatestDerivedSchema(storeName, valueSchemaId);
    if (derivedSchemaEntry == null) {
      return null;
    }
    String partialUpdateSchemaId = valueSchemaId + "-" + derivedSchemaEntry.getId();
    Map<String, DerivedSchemaEntry> schemaMap =
        partialUpdateSchemaEntryMapCache.computeIfAbsent(storeName, k -> new VeniceConcurrentHashMap<>());
    return schemaMap
        .computeIfAbsent(partialUpdateSchemaId, k -> getAnnotatedStringMapDerivedSchemaEntry(derivedSchemaEntry));
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    throw new UnsupportedOperationException("This method is not supported");
  }
}
