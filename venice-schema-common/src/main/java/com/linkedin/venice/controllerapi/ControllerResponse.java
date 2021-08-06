package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Extend this class to create response objects for the controller
 * Any fields that must be in all responses can go here.
 */
public class ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private String cluster;
  private String name;
  private String error;
  private ExceptionType exceptionType = ExceptionType.GENERAL_ERROR;

  /**
   * Starting with Jackson 1.9, if this is the only annotation: {@link JsonIgnore}
   * associated with a property, it will also cause cause the whole
   * property to be ignored.
   *
   * So we need to explicitly specify {@link JsonProperty} with {@link #getError} and {@link #setError(String)}
   *
   * @return
   */
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

  public ExceptionType getExceptionType() {
    return exceptionType;
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
      exceptionType = ((VeniceException) e).getExceptionType();
    }
    this.error = error + "," + e.getMessage();
  }

  public void setError(Throwable e) {
    if (e instanceof VeniceException) {
      exceptionType = ((VeniceException) e).getExceptionType();
    }
    this.error = e.getMessage();
  }

  @JsonProperty
  public void setExceptionType(ExceptionType exceptionType) {
    this.exceptionType = exceptionType;
  }

  @JsonIgnore
  public String toString() {
    return ControllerResponse.class.getSimpleName() + "(cluster: " + cluster +
        ", name: " + name +
        ", error: " + error +
        ", exceptionType: " + exceptionType +
        ")";
  }
}
