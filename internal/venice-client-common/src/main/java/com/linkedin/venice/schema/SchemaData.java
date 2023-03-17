package com.linkedin.venice.schema;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.avro.Schema;


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
  private final SortedMap<Integer, SchemaEntry> valueSchemaMap;
  private final Map<SchemaEntry, Integer> valueSchemaRMap;
  private final Map<GeneratedSchemaID, DerivedSchemaEntry> updateSchemaMap;
  private final Map<String, GeneratedSchemaID> updateSchemaRMap;
  private final Map<RmdVersionId, RmdSchemaEntry> rmdSchemaMap;
  private final Map<Schema, RmdVersionId> rmdSchemaRMap;

  private final Set<Integer> updateSchemaExistenceSet;
  private final Set<Integer> rmdSchemaExistenceSet;

  public final static int UNKNOWN_SCHEMA_ID = 0;
  public final static int INVALID_VALUE_SCHEMA_ID = -1;
  public final static int DUPLICATE_VALUE_SCHEMA_CODE = -2;

  public SchemaData(String storeName) {
    this.storeName = storeName;
    this.valueSchemaMap = new ConcurrentSkipListMap<>();
    this.valueSchemaRMap = new VeniceConcurrentHashMap<>();

    this.updateSchemaMap = new VeniceConcurrentHashMap<>();
    this.updateSchemaRMap = new VeniceConcurrentHashMap<>();

    this.rmdSchemaMap = new VeniceConcurrentHashMap<>();
    this.rmdSchemaRMap = new VeniceConcurrentHashMap<>();

    this.updateSchemaExistenceSet = new ConcurrentSkipListSet<>();
    this.rmdSchemaExistenceSet = new ConcurrentSkipListSet<>();
  }

  public String getStoreName() {
    return storeName;
  }

  public SchemaEntry getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(SchemaEntry keySchema) {
    this.keySchema = keySchema;
  }

  public SchemaEntry getValueSchema(int id) {
    return valueSchemaMap.get(Integer.valueOf(id));
  }

  public void addValueSchema(SchemaEntry valueSchema) {
    // value schema should be unique in store level, same as schema id
    Integer id = Integer.valueOf(valueSchema.getId());
    valueSchemaMap.put(id, valueSchema);
    valueSchemaRMap.put(valueSchema, id);
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
    updateSchemaExistenceSet.add(derivedSchemaEntry.getValueSchemaID());
  }

  public int getMaxValueSchemaId() {
    if (valueSchemaMap.isEmpty()) {
      return INVALID_VALUE_SCHEMA_ID;
    }
    return valueSchemaMap.lastKey();
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

  public RmdVersionId getReplicationMetadataVersionId(RmdSchemaEntry entry) {
    RmdVersionId rmdVersionId = rmdSchemaRMap.get(entry.getSchema());
    if (rmdVersionId == null) {
      rmdVersionId = new RmdVersionId(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);
    }
    return rmdVersionId;
  }

  public void addReplicationMetadataSchema(RmdSchemaEntry rmdSchemaEntry) {
    RmdVersionId rmdVersionId = new RmdVersionId(rmdSchemaEntry.getValueSchemaID(), rmdSchemaEntry.getId());
    rmdSchemaMap.put(rmdVersionId, rmdSchemaEntry);
    rmdSchemaRMap.put(rmdSchemaEntry.getSchema(), rmdVersionId);
    rmdSchemaExistenceSet.add(rmdSchemaEntry.getValueSchemaID());
  }

  public boolean hasUpdateSchema(int valueSchemaId) {
    return updateSchemaExistenceSet.contains(valueSchemaId);
  }

  public boolean hasRmdSchema(int valueSchemaId) {
    return rmdSchemaExistenceSet.contains(valueSchemaId);
  }
}
