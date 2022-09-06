package com.linkedin.davinci.replication.merge.helper.utils;

public class DeleteListOperation extends CollectionOperation {
  // No additional fields required.
  public DeleteListOperation(long opTimestamp, int opColoID, String fieldName) {
    super(opTimestamp, opColoID, fieldName, "delete_list");
  }
}
