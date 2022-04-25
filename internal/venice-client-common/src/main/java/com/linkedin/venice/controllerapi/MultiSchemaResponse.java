package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class MultiSchemaResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  /**
   * If this is set it should be used to replace latestValueSchema to deserialize during read.
   */
  private int superSetSchemaId = -1;

  public void setSuperSetSchemaId(int id) {
    superSetSchemaId = id;
  }

  public int getSuperSetSchemaId() {
    return superSetSchemaId;
  }

  public static class Schema {
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

    @JsonIgnore
    public boolean isDerivedSchema() {
      return derivedSchemaId != -1;
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
  }

  private Schema[] schemas;

  public Schema[] getSchemas() {
    return schemas;
  }

  public void setSchemas(Schema[] schemas) {
    this.schemas = schemas;
  }
}
