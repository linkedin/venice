package com.linkedin.davinci.replication.merge.helper.utils;

/**
 * This class models an operation on a collection field.
 */
public abstract class CollectionOperation {
  private final long opTimestamp;
  private final int opColoID;
  private final String fieldName;
  private final String opName;

  CollectionOperation(long opTimestamp, int opColoID, String fieldName, String opName) {
    this.opTimestamp = opTimestamp;
    this.opColoID = opColoID;
    this.opName = opName;
    this.fieldName = fieldName;
  }

  public long getOpTimestamp() {
    return opTimestamp;
  }

  public int getOpColoID() {
    return opColoID;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  public String toString() {
    return String.format("Op_%s from colo %d @ %d", opName, opColoID, opTimestamp);
  }
}
