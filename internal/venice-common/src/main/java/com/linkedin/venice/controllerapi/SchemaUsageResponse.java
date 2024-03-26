package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaUsageResponse extends ControllerResponse {
  private Set<Integer> inUseValueSchemaIds;

  public Set<Integer> getInUseValueSchemaIds() {
    return inUseValueSchemaIds;
  }

  public void setInUseValueSchemaIds(Set<Integer> schemaIds) {
    this.inUseValueSchemaIds = schemaIds;
  }
}
