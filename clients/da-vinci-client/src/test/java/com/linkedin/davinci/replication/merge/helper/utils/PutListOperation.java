package com.linkedin.davinci.replication.merge.helper.utils;

import java.util.LinkedList;
import java.util.List;


public class PutListOperation extends CollectionOperation {
  private final List<Object> newList;

  public PutListOperation(long opTimestamp, int opColoID, List<Object> newList, String fieldName) {
    super(opTimestamp, opColoID, fieldName, "put_list");
    if (newList instanceof LinkedList) {
      this.newList = newList;
    } else {
      this.newList = new LinkedList<>(newList);
    }
  }

  public List<Object> getNewList() {
    return newList;
  }
}
