package com.linkedin.venice.schema;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * This class is used to store all the schemas related to a given store:
 * 1. key schema
 * 2. value schemas
 * 3. partial update schemas
 * 4. replication metadata schemas
 */
public final class SchemaData {
  private final String storeName;
  private SchemaEntry keySchema;
  private final SparseConcurrentList<SchemaEntry> valueSchemaMap;
  private final Map<SchemaEntry, Integer> valueSchemaRMap;
  private final Map<GeneratedSchemaID, DerivedSchemaEntry> updateSchemaMap;
  private final Map<String, GeneratedSchemaID> updateSchemaRMap;
  private final Map<RmdVersionId, RmdSchemaEntry> rmdSchemaMap;
  private final List<Object> updateSchemaExistenceSet;
  private final List<Object> rmdSchemaExistenceSet;

  private volatile int maxValueSchemaId;

  public final static int UNKNOWN_SCHEMA_ID = 0;
  public final static int INVALID_VALUE_SCHEMA_ID = -1;
  public final static int DUPLICATE_VALUE_SCHEMA_CODE = -2;

  public SchemaData(String storeName, SchemaEntry keySchema) {
    this.storeName = storeName;
    this.keySchema = keySchema;
    this.maxValueSchemaId = INVALID_VALUE_SCHEMA_ID;
    this.valueSchemaMap = new SparseConcurrentList<>();
    this.valueSchemaRMap = new VeniceConcurrentHashMap<>();

    this.updateSchemaMap = new VeniceConcurrentHashMap<>();
    this.updateSchemaRMap = new VeniceConcurrentHashMap<>();

    this.rmdSchemaMap = new VeniceConcurrentHashMap<>();

    this.updateSchemaExistenceSet = new SparseConcurrentList<>();
    this.rmdSchemaExistenceSet = new SparseConcurrentList<>();
  }

  public String getStoreName() {
    return storeName;
  }

  /**
   * @return the key {@link SchemaEntry}, which may temporarily be null soon after store initialization...
   */
  public SchemaEntry getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(SchemaEntry keySchema) {
    this.keySchema = keySchema;
  }

  public SchemaEntry getValueSchema(int id) {
    return valueSchemaMap.get(id);
  }

  public void addValueSchema(SchemaEntry valueSchema) {
    // value schema should be unique in store level, same as schema id
    synchronized (this) {
      this.maxValueSchemaId = Math.max(valueSchema.getId(), this.maxValueSchemaId);
    }
    this.valueSchemaMap.set(valueSchema.getId(), valueSchema);
    this.valueSchemaRMap.put(valueSchema, Integer.valueOf(valueSchema.getId()));
  }

  public DerivedSchemaEntry getDerivedSchema(int valueSchemaId, int derivedSchemaId) {
    return updateSchemaMap.get(new GeneratedSchemaID(valueSchemaId, derivedSchemaId));
  }

  public Collection<DerivedSchemaEntry> getDerivedSchemas() {
    return updateSchemaMap.values();
  }

  public GeneratedSchemaID getDerivedSchemaId(String schemaStr) {
    return updateSchemaRMap.getOrDefault(schemaStr, GeneratedSchemaID.INVALID);
  }

  public void addDerivedSchema(DerivedSchemaEntry derivedSchemaEntry) {
    GeneratedSchemaID derivedSchemaId =
        new GeneratedSchemaID(derivedSchemaEntry.getValueSchemaID(), derivedSchemaEntry.getId());
    updateSchemaMap.put(derivedSchemaId, derivedSchemaEntry);
    updateSchemaRMap.put(derivedSchemaEntry.getSchemaStr(), derivedSchemaId);
    updateSchemaExistenceSet.set(derivedSchemaEntry.getValueSchemaID(), new Object());
  }

  public int getMaxValueSchemaId() {
    return maxValueSchemaId;
  }

  public int getSchemaID(SchemaEntry entry) {
    return valueSchemaRMap.getOrDefault(entry, INVALID_VALUE_SCHEMA_ID);
  }

  public Collection<SchemaEntry> getValueSchemas() {
    return valueSchemaMap.values();
  }

  public RmdSchemaEntry getReplicationMetadataSchema(int valueSchemaId, int replicationMetadataVersionId) {
    return rmdSchemaMap.get(new RmdVersionId(valueSchemaId, replicationMetadataVersionId));
  }

  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas() {
    return rmdSchemaMap.values();
  }

  public void addReplicationMetadataSchema(RmdSchemaEntry rmdSchemaEntry) {
    RmdVersionId rmdVersionId = new RmdVersionId(rmdSchemaEntry.getValueSchemaID(), rmdSchemaEntry.getId());
    rmdSchemaMap.put(rmdVersionId, rmdSchemaEntry);
    rmdSchemaExistenceSet.set(rmdSchemaEntry.getValueSchemaID(), new Object());
  }

  public boolean hasUpdateSchema(int valueSchemaId) {
    return updateSchemaExistenceSet.get(valueSchemaId) != null;
  }

  public boolean hasRmdSchema(int valueSchemaId) {
    return rmdSchemaExistenceSet.get(valueSchemaId) != null;
  }
}
