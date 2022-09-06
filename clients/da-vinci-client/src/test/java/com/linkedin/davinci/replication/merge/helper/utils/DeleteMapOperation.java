package com.linkedin.davinci.replication.merge.helper.utils;

public class DeleteMapOperation extends CollectionOperation {
  // No additional fields required.
  public DeleteMapOperation(long opTimestamp, int opColoID, String fieldName) {
    super(opTimestamp, opColoID, fieldName, "delete_map");
  }
}
