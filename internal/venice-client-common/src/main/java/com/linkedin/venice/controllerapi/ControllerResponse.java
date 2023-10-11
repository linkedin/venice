package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;


/**
 * Extend this class to create response objects for the controller
 * Any fields that must be in all responses can go here.
 */
public class ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private String cluster;
  private String name;
  private String error;
  private ErrorType errorType = null;

  /**
   * Starting with Jackson 1.9, if this is the only annotation: {@link JsonIgnore}
   * associated with a property, it will also cause the whole property to be ignored.
   *
   * So we need to explicitly specify {@link JsonProperty} with {@link #getError} and {@link #setError(String)}
   *
   * @return
   */
  @JsonIgnore
  public boolean isError() {
    return error != null;
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

  public ErrorType getErrorType() {
    return errorType;
  }

  @Deprecated
  public ExceptionType getExceptionType() {
    return inferExceptionTypeFromErrorType();
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty
  public String getError() {
    return error;
  }

  @JsonProperty
  public void setError(String error) {
    this.error = error;
  }

  public void setError(String error, Throwable e) {
    if (e instanceof VeniceException) {
      errorType = ((VeniceException) e).getErrorType();
    }
    this.error = error + ". Exception type: " + e.getClass().toString() + ". Detailed message: " + e.getMessage();
  }

  public void setError(Throwable e) {
    if (e instanceof VeniceException) {
      errorType = ((VeniceException) e).getErrorType();
    }
    this.error = e.getMessage();
  }

  @JsonProperty("exceptionType")
  private ExceptionType inferExceptionTypeFromErrorType() {
    return errorType == null ? null : errorType.getExceptionType();
  }

  @JsonProperty
  public void setErrorType(ErrorType errorType) {
    this.errorType = errorType;
  }

  @JsonIgnore
  public String toString() {
    return new StringBuilder().append(ControllerResponse.class.getSimpleName())
        .append("(cluster: ")
        .append(cluster)
        .append(", name: ")
        .append(name)
        .append(", error: ")
        .append(error)
        .append(", errorType: ")
        .append(errorType)
        .append(", exceptionType: ")
        .append(inferExceptionTypeFromErrorType())
        .append(")")
        .toString();
  }
}
