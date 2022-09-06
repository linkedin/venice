package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class VeniceNoClusterException extends VeniceException {
  private final String clusterName;

  public VeniceNoClusterException(String clusterName) {
    super("Cluster: " + clusterName + " does not exist");
    this.clusterName = clusterName;
  }

  public VeniceNoClusterException(String clusterName, Throwable t) {
    super("Cluster: " + clusterName + " does not exist", t);
    this.clusterName = clusterName;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_NOT_FOUND;
  }
}
