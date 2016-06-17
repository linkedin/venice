package com.linkedin.venice.controllerapi;

public class MultiSchemaResponse extends ControllerResponse {
  public static class Schema {
    private int id;
    private String schemaStr;

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

  private Schema[] schemas;

  public Schema[] getSchemas() {
    return schemas;
  }

  public void setSchemas(Schema[] schemas) {
    this.schemas = schemas;
  }
}
