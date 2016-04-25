package com.linkedin.venice.controllerapi;

import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * Response for querying job status.
 */
public class JobStatusQueryResponse {


  private String cluster;
  private String name;
  private int version;
  private String status;
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

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public static JobStatusQueryResponse createErrorResponse(String errorMessage){
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setError(errorMessage);
    return response;
  }
}
