package com.linkedin.venice.controllerapi;

import com.linkedin.venice.schema.SchemaData;
import java.util.HashSet;
import java.util.Set;


public class MultiSchemaIdResponse extends ControllerResponse {
  /* Uses Json Reflective Serializer, get without set may break things */
  private int superSetSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
  private Set<Integer> schemaIdSet = new HashSet<>();

  public void setSuperSetSchemaId(int id) {
    superSetSchemaId = id;
  }

  public int getSuperSetSchemaId() {
    return superSetSchemaId;
  }

  public void setSchemaIdSet(Set<Integer> schemaIdSet) {
    this.schemaIdSet = schemaIdSet;
  }

  public Set<Integer> getSchemaIdSet() {
    return schemaIdSet;
  }
}
