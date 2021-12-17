package com.linkedin.davinci.replication.merge.helper.utils;

import java.util.List;


public class PutListOperation extends CollectionOperation {
  private final List<Object> newList;

  public PutListOperation(long opTimestamp, int opColoID, List<Object> newList, String fieldName) {
    super(opTimestamp, opColoID, fieldName, "put_list");
    this.newList = newList;
  }

  public List<Object> getNewList() {
    return newList;
  }
}
