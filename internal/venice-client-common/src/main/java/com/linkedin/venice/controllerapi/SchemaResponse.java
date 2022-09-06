package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class SchemaResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private int id;
  /**
   * An uninitialized primitive defaults to 0, which could cause the Venice Samza System producer starts sending
   * Venice UPDATE messages instead of PUT messages.
   */
  private int derivedSchemaId = -1;
  private String schemaStr;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getDerivedSchemaId() {
    return derivedSchemaId;
  }

  public void setDerivedSchemaId(int derivedSchemaId) {
    this.derivedSchemaId = derivedSchemaId;
  }

  public String getSchemaStr() {
    return schemaStr;
  }

  public void setSchemaStr(String schemaStr) {
    this.schemaStr = schemaStr;
  }

  @JsonIgnore
  public String toString() {
    return SchemaResponse.class.getSimpleName() + "(id: " + id + ", schemaStr: " + schemaStr + ", super: "
        + super.toString() + ")";
  }
}
