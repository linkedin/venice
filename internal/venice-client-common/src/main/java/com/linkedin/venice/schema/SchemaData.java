package com.linkedin.venice.schema;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.avro.Schema;


/**
 * This class is used to store all the schemas related to a given store:
 * 1. key schema
 * 2. value schemas
 * 3. write compute derived schemas
 * 4. replication metadata schemas
 */
public final class SchemaData {
  private String storeName;
  private SchemaEntry keySchema;
  private SortedMap<Integer, SchemaEntry> valueSchemaMap;
  private Map<SchemaEntry, Integer> valueSchemaRMap;
  private Map<Pair<Integer, Integer>, DerivedSchemaEntry> derivedSchemaMap;
  private Map<DerivedSchemaEntry, Pair<Integer, Integer>> derivedSchemaRMap;
  private Map<RmdVersionId, RmdSchemaEntry> replicationMetadataSchemaMap;
  private Map<Schema, RmdVersionId> replicationMetadataSchemaRMap;

  public final static int UNKNOWN_SCHEMA_ID = 0;
  public final static int INVALID_VALUE_SCHEMA_ID = -1;
  public final static int DUPLICATE_VALUE_SCHEMA_CODE = -2;

  public SchemaData(String storeName) {
    this.storeName = storeName;
    valueSchemaMap = new TreeMap<>();
    valueSchemaRMap = new HashMap<>();

    derivedSchemaMap = new HashMap<>();
    derivedSchemaRMap = new HashMap<>();

    replicationMetadataSchemaMap = new HashMap<>();
    replicationMetadataSchemaRMap = new HashMap<>();
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
    return derivedSchemaMap.get(new Pair<>(valueSchemaId, derivedSchemaId));
  }

  public Collection<DerivedSchemaEntry> getDerivedSchemas() {
    return derivedSchemaMap.values();
  }

  public Pair<Integer, Integer> getDerivedSchemaId(DerivedSchemaEntry entry) {
    if (derivedSchemaRMap.containsKey(entry)) {
      return derivedSchemaRMap.get(entry);
    }

    return new Pair<>(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);
  }

  public void addDerivedSchema(DerivedSchemaEntry derivedSchemaEntry) {
    Pair<Integer, Integer> derivedSchemaId =
        new Pair<>(derivedSchemaEntry.getValueSchemaID(), derivedSchemaEntry.getId());
    derivedSchemaMap.put(derivedSchemaId, derivedSchemaEntry);
    derivedSchemaRMap.put(derivedSchemaEntry, derivedSchemaId);
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
    return replicationMetadataSchemaMap.get(new RmdVersionId(valueSchemaId, replicationMetadataVersionId));
  }

  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas() {
    return replicationMetadataSchemaMap.values();
  }

  public RmdVersionId getReplicationMetadataVersionId(RmdSchemaEntry entry) {
    if (replicationMetadataSchemaRMap.containsKey(entry.getSchema())) {
      return replicationMetadataSchemaRMap.get(entry.getSchema());
    }

    return new RmdVersionId(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);
  }

  public void addReplicationMetadataSchema(RmdSchemaEntry rmdSchemaEntry) {
    RmdVersionId rmdVersionId = new RmdVersionId(rmdSchemaEntry.getValueSchemaID(), rmdSchemaEntry.getId());
    replicationMetadataSchemaMap.put(rmdVersionId, rmdSchemaEntry);
    replicationMetadataSchemaRMap.put(rmdSchemaEntry.getSchema(), rmdVersionId);
  }
}
