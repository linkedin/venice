package com.linkedin.davinci.replication.merge.helper.utils;

import com.linkedin.venice.utils.IndexedHashMap;


public class PutMapOperation extends CollectionOperation {
  private final IndexedHashMap<String, Object> newMap;

  public PutMapOperation(long opTimestamp, int opColoID, IndexedHashMap<String, ?> newMap, String fieldName) {
    super(opTimestamp, opColoID, fieldName, "put_map");
    this.newMap = (IndexedHashMap<String, Object>) newMap;
  }

  public IndexedHashMap<String, Object> getNewMap() {
    return newMap;
  }
}
