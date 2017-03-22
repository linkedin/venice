package com.linkedin.venice.controllerapi;

import org.codehaus.jackson.annotate.JsonIgnore;

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

  @JsonIgnore
  public String toString() {
    return SchemaResponse.class.getSimpleName() + "(id: " + id +
        ", schemaStr: " + schemaStr +
        ", super: " + super.toString() + ")";
  }
}
