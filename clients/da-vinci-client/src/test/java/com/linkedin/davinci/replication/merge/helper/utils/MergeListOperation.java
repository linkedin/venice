package com.linkedin.davinci.replication.merge.helper.utils;

import java.util.List;


public class MergeListOperation extends CollectionOperation {
  private final List<Object> newElements;
  private final List<Object> toRemoveElements;

  public MergeListOperation(
      long opTimestamp,
      int opColoID,
      List<Object> newElements,
      List<Object> toRemoveElements,
      String fieldName) {
    super(opTimestamp, opColoID, fieldName, "merge_list");
    this.newElements = newElements;
    this.toRemoveElements = toRemoveElements;
  }

  public List<Object> getNewElements() {
    return newElements;
  }

  public List<Object> getToRemoveElements() {
    return toRemoveElements;
  }
}
