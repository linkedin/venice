package com.linkedin.davinci.replication.merge.helper.utils;

import java.util.List;
import java.util.Map;


public class MergeMapOperation extends CollectionOperation {
  private final Map<String, Object> newEntries;
  private final List<String> toRemoveKeys;

  public MergeMapOperation(
      long opTimestamp,
      int opColoID,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys,
      String fieldName) {
    super(opTimestamp, opColoID, fieldName, "merge_map");
    this.newEntries = newEntries;
    this.toRemoveKeys = toRemoveKeys;
  }

  public Map<String, Object> getNewEntries() {
    return newEntries;
  }

  public List<String> getToRemoveKeys() {
    return toRemoveKeys;
  }
}
