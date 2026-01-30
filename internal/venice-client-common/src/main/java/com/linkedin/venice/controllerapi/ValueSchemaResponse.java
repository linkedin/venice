package com.linkedin.venice.controllerapi;

/**
 * Response object for value schema retrieval operations.
 * Contains the schema string and metadata for a specific schema ID of a store.
 */
public class ValueSchemaResponse extends ControllerResponse {
  private int schemaId;
  private String schemaStr;

  public int getSchemaId() {
    return schemaId;
  }

  public void setSchemaId(int schemaId) {
    this.schemaId = schemaId;
  }

  public String getSchemaStr() {
    return schemaStr;
  }

  public void setSchemaStr(String schemaStr) {
    this.schemaStr = schemaStr;
  }
}
