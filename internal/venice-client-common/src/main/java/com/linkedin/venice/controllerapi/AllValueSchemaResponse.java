package com.linkedin.venice.controllerapi;

import java.util.List;


/**
 * Response object for retrieving all value schemas of a store.
 * Contains the list of schemas and the super set schema ID.
 */
public class AllValueSchemaResponse extends ControllerResponse {
  private List<SchemaInfo> schemas;
  private int superSetSchemaId;

  public List<SchemaInfo> getSchemas() {
    return schemas;
  }

  public void setSchemas(List<SchemaInfo> schemas) {
    this.schemas = schemas;
  }

  public int getSuperSetSchemaId() {
    return superSetSchemaId;
  }

  public void setSuperSetSchemaId(int superSetSchemaId) {
    this.superSetSchemaId = superSetSchemaId;
  }

  /**
   * Inner class representing a single schema with ID and schema string.
   */
  public static class SchemaInfo {
    private int id;
    private String schemaStr;

    public SchemaInfo() {
    }

    public SchemaInfo(int id, String schemaStr) {
      this.id = id;
      this.schemaStr = schemaStr;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getSchemaStr() {
      return schemaStr;
    }

    public void setSchemaStr(String schemaStr) {
      this.schemaStr = schemaStr;
    }
  }
}
