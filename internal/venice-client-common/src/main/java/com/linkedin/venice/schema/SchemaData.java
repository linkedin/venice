package com.linkedin.venice.schema;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
  private final Map<Pair<Integer, Integer>, DerivedSchemaEntry> updateSchemaMap;
  private final Map<DerivedSchemaEntry, Pair<Integer, Integer>> updateSchemaRMap;
  private final Map<RmdVersionId, RmdSchemaEntry> rmdSchemaMap;
  private final Map<Schema, RmdVersionId> rmdSchemaRMap;

  private final Set<Integer> updateSchemaExistenceMap;
  private final Set<Integer> rmdSchemaExistenceMap;

  public final static int UNKNOWN_SCHEMA_ID = 0;
  public final static int INVALID_VALUE_SCHEMA_ID = -1;
  public final static int DUPLICATE_VALUE_SCHEMA_CODE = -2;

  public SchemaData(String storeName) {
    this.storeName = storeName;
    valueSchemaMap = new TreeMap<>();
    valueSchemaRMap = new VeniceConcurrentHashMap<>();

    updateSchemaMap = new VeniceConcurrentHashMap<>();
    updateSchemaRMap = new VeniceConcurrentHashMap<>();

    rmdSchemaMap = new VeniceConcurrentHashMap<>();
    rmdSchemaRMap = new VeniceConcurrentHashMap<>();

    updateSchemaExistenceMap = new HashSet<>();
    rmdSchemaExistenceMap = new HashSet<>();
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
    Integer id = valueSchema.getId();
    valueSchemaMap.put(id, valueSchema);
    valueSchemaRMap.put(valueSchema, id);
  }

  public DerivedSchemaEntry getDerivedSchema(int valueSchemaId, int derivedSchemaId) {
    return updateSchemaMap.get(new Pair<>(valueSchemaId, derivedSchemaId));
  }

  public Collection<DerivedSchemaEntry> getDerivedSchemas() {
    return updateSchemaMap.values();
  }

  public Pair<Integer, Integer> getDerivedSchemaId(DerivedSchemaEntry entry) {
    if (updateSchemaRMap.containsKey(entry)) {
      return updateSchemaRMap.get(entry);
    }

    return new Pair<>(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);
  }

  public void addDerivedSchema(DerivedSchemaEntry derivedSchemaEntry) {
    Pair<Integer, Integer> derivedSchemaId =
        new Pair<>(derivedSchemaEntry.getValueSchemaID(), derivedSchemaEntry.getId());
    updateSchemaMap.put(derivedSchemaId, derivedSchemaEntry);
    updateSchemaRMap.put(derivedSchemaEntry, derivedSchemaId);
    updateSchemaExistenceMap.add(derivedSchemaEntry.getValueSchemaID());
  }

  public int getMaxValueSchemaId() {
    if (valueSchemaMap.isEmpty()) {
      return INVALID_VALUE_SCHEMA_ID;
    }
    return valueSchemaMap.lastKey();
  }

  public int getSchemaID(SchemaEntry entry) {
    if (valueSchemaRMap.containsKey(entry)) {
      return valueSchemaRMap.get(entry);
    }
    return INVALID_VALUE_SCHEMA_ID;
  }

  public Collection<SchemaEntry> getValueSchemas() {
    return valueSchemaMap.values();
  }

  public Collection<SchemaEntry> cloneValueSchemas() {
    Collection<SchemaEntry> valueSchemas = new ArrayList<>();
    for (SchemaEntry valueSchema: valueSchemaMap.values()) {
      valueSchemas.add(valueSchema.clone());
    }

    return valueSchemas;
  }

  public RmdSchemaEntry getReplicationMetadataSchema(int valueSchemaId, int replicationMetadataVersionId) {
    return rmdSchemaMap.get(new RmdVersionId(valueSchemaId, replicationMetadataVersionId));
  }

  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas() {
    return rmdSchemaMap.values();
  }

  public RmdVersionId getReplicationMetadataVersionId(RmdSchemaEntry entry) {
    if (rmdSchemaRMap.containsKey(entry.getSchema())) {
      return rmdSchemaRMap.get(entry.getSchema());
    }

    return new RmdVersionId(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);
  }

  public void addReplicationMetadataSchema(RmdSchemaEntry rmdSchemaEntry) {
    RmdVersionId rmdVersionId = new RmdVersionId(rmdSchemaEntry.getValueSchemaID(), rmdSchemaEntry.getId());
    rmdSchemaMap.put(rmdVersionId, rmdSchemaEntry);
    rmdSchemaRMap.put(rmdSchemaEntry.getSchema(), rmdVersionId);
    rmdSchemaExistenceMap.add(rmdSchemaEntry.getValueSchemaID());
  }

  public boolean hasUpdateSchema(int valueSchemaId) {
    return updateSchemaExistenceMap.contains(valueSchemaId);
  }

  public boolean hasRmdSchema(int valueSchemaId) {
    return rmdSchemaExistenceMap.contains(valueSchemaId);
  }
}
