package com.linkedin.venice.controllerapi;

import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * Extend this class to create response objects for the controller
 * Any fields that must be in all responses can go here.
 */
public class ControllerResponse {
  private String cluster;
  private String name;
  private String error;

  @JsonIgnore
  public boolean isError(){
    return null!=error;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
}
