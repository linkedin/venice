package com.linkedin.venice.controllerapi;

/**
 * Response for querying job status.
 */
public class JobStatusQueryResponse extends ControllerResponse{ /* Uses Json Reflective Serializer, get without set may break things */

  private int version;
  private String status;

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

  public static JobStatusQueryResponse createErrorResponse(String errorMessage){
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setError(errorMessage);
    return response;
  }
}
