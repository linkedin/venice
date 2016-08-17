package com.linkedin.venice.controllerapi;

public class SchemaResponse extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  int id;
  String schemaStr;

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
