package com.linkedin.venice.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This class is used to store all the schemas related to a given store:
 * 1. key schema
 * 2. value schemas
 */
public final class SchemaData {
  private String storeName;
  private SchemaEntry keySchema;
  private SortedMap<Integer, SchemaEntry> valueSchemaMap;
  private Map<SchemaEntry, Integer> valueSchemaRMap;

  public static int INVALID_VALUE_SCHEMA_ID = -1;

  public SchemaData(String storeName) {
    this.storeName = storeName;
    valueSchemaMap = new TreeMap<>();
    valueSchemaRMap = new HashMap<>();
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
    return valueSchemaMap.get(new Integer(id));
  }

  public void addValueSchema(SchemaEntry valueSchema) {
    // value schema should be unique in store level, same as schema id
    Integer id = new Integer(valueSchema.getId());
    valueSchemaMap.put(id, valueSchema);
    valueSchemaRMap.put(valueSchema, id);
  }

  public int getMaxValueSchemaId() {
    if (valueSchemaMap.isEmpty()) {
      return 0;
    }
    return valueSchemaMap.lastKey().intValue();
  }

  public int getSchemaID(SchemaEntry entry) {
    if (valueSchemaRMap.containsKey(entry)) {
      return valueSchemaRMap.get(entry).intValue();
    }
    return INVALID_VALUE_SCHEMA_ID;
  }

  public Collection<SchemaEntry> getValueSchemas() {
    return valueSchemaMap.values();
  }

  public Collection<SchemaEntry> cloneValueSchemas() {
    Collection<SchemaEntry> valueSchemas = new ArrayList<>();
    for (SchemaEntry valueSchema : valueSchemaMap.values()) {
      valueSchemas.add(valueSchema.clone());
    }

    return valueSchemas;
  }
}
