package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedDerivedSchemaEntry;
import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedRmdSchemaEntry;
import static com.linkedin.venice.schema.SchemaUtils.getAnnotatedValueSchemaEntry;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.SparseConcurrentList;
import org.apache.avro.util.Utf8;


/**
 * This class serves as the annotated schema cache for merge conflict resolution purpose in Active/Active replication.
 * String in Avro string array and map key will be deserialized into {@link org.apache.avro.util.Utf8} instead of Java
 * String and this will result in incorrect and inefficient processing during collection merge operations.
 * This class wraps schema repository and annotates value schema, update schema, superset schema and RMD schema to be
 * used by {@link MergeConflictResolver} and {@link RmdSerDe} only. The {@link MergeConflictResolver} will process all
 * string array and map field in Java string correctly and no internal {@link Utf8#toString()} is needed.
 */
public class StringAnnotatedStoreSchemaCache {
  private final ReadOnlySchemaRepository internalSchemaRepo;
  private final String storeName;
  private final SparseConcurrentList<SchemaEntry> valueSchemaEntryMapCache = new SparseConcurrentList<>();
  private final SparseConcurrentList<SparseConcurrentList<DerivedSchemaEntry>> partialUpdateSchemaEntryMapCache =
      new SparseConcurrentList<>();
  private final SparseConcurrentList<SparseConcurrentList<RmdSchemaEntry>> rmdSchemaEntryMapCache =
      new SparseConcurrentList<>();

  public StringAnnotatedStoreSchemaCache(String storeName, ReadOnlySchemaRepository internalSchemaRepo) {
    this.storeName = storeName;
    this.internalSchemaRepo = internalSchemaRepo;
  }

  /**
   * Retrieve value schema and annotate its top-level string array and map fields.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public SchemaEntry getValueSchema(int id) {
    return valueSchemaEntryMapCache.computeIfAbsent(id, k -> {
      SchemaEntry schemaEntry = internalSchemaRepo.getValueSchema(storeName, id);
      if (schemaEntry == null) {
        return null;
      }
      return getAnnotatedValueSchemaEntry(schemaEntry);
    });
  }

  /**
   * Retrieve the superset schema (if exists) or the latest value schema and return corresponding annotated value schema.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public SchemaEntry getSupersetOrLatestValueSchema() {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetOrLatestValueSchema(storeName);
    if (schemaEntry == null) {
      return null;
    }
    return valueSchemaEntryMapCache
        .computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedValueSchemaEntry(schemaEntry));
  }

  /**
   * Retrieve the superset schema (if exists) and return corresponding annotated value schema.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public SchemaEntry getSupersetSchema() {
    SchemaEntry schemaEntry = internalSchemaRepo.getSupersetSchema(storeName);
    if (schemaEntry == null) {
      return null;
    }
    return valueSchemaEntryMapCache
        .computeIfAbsent(schemaEntry.getId(), k -> getAnnotatedValueSchemaEntry(schemaEntry));
  }

  /**
   * Retrieve update schema and annotate top-level string array and map field's field update and collection merge operations.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public DerivedSchemaEntry getDerivedSchema(int valueSchemaId, int partialUpdateProtocolId) {
    SparseConcurrentList<DerivedSchemaEntry> innerList =
        partialUpdateSchemaEntryMapCache.computeIfAbsent(valueSchemaId, SparseConcurrentList.SUPPLIER);
    return innerList.computeIfAbsent(partialUpdateProtocolId, k -> {
      DerivedSchemaEntry derivedSchemaEntry =
          internalSchemaRepo.getDerivedSchema(storeName, valueSchemaId, partialUpdateProtocolId);
      if (derivedSchemaEntry == null) {
        return null;
      }
      return getAnnotatedDerivedSchemaEntry(derivedSchemaEntry);
    });
  }

  /**
   * Retrieve RMD schema and annotate map field and string array field's deleted elements list field.
   * The annotation will only be done once in the repository's lifetime as the result is cached.
   */
  public RmdSchemaEntry getRmdSchema(int valueSchemaId, int rmdSchemaProtocolId) {
    SparseConcurrentList<RmdSchemaEntry> innerList =
        rmdSchemaEntryMapCache.computeIfAbsent(valueSchemaId, SparseConcurrentList.SUPPLIER);
    return innerList.computeIfAbsent(rmdSchemaProtocolId, k -> {
      RmdSchemaEntry rmdSchemaEntry =
          internalSchemaRepo.getReplicationMetadataSchema(storeName, valueSchemaId, rmdSchemaProtocolId);
      if (rmdSchemaEntry == null) {
        return null;
      }
      return getAnnotatedRmdSchemaEntry(rmdSchemaEntry);
    });

  }
}
