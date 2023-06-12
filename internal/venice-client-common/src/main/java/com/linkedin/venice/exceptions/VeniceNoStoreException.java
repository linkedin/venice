package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should return information about a store, but the store does not exist
 */
public class VeniceNoStoreException extends VeniceException {
  private final String storeName;
  private final String clusterName;

  public VeniceNoStoreException(String storeName, String clusterName) {
    super(getErrorMessage(storeName, clusterName, null), ErrorType.STORE_NOT_FOUND);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceNoStoreException(String storeName, String clusterName, Throwable t) {
    super(getErrorMessage(storeName, clusterName, null), t, ErrorType.STORE_NOT_FOUND);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceNoStoreException(String storeName) {
    super(getErrorMessage(storeName, null, null), ErrorType.STORE_NOT_FOUND);
    this.storeName = storeName;
    this.clusterName = "unspecified";
  }

  public VeniceNoStoreException(String storeName, String clusterName, String additionalMessage) {
    super(getErrorMessage(storeName, clusterName, additionalMessage), ErrorType.STORE_NOT_FOUND);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceNoStoreException(String storeName, Throwable t) {
    super(getErrorMessage(storeName, null, null), t, ErrorType.STORE_NOT_FOUND);
    this.storeName = storeName;
    this.clusterName = "unspecified";
  }

  public String getStoreName() {
    return storeName;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_NOT_FOUND;
  }

  private static String getErrorMessage(String storeName, String clusterName, String additionalMessage) {
    StringBuilder errorBuilder = new StringBuilder().append("Store: ").append(storeName).append(" does not exist");
    if (clusterName != null) {
      errorBuilder.append(" in cluster ").append(clusterName);
    } else {
      errorBuilder.append(".");
    }

    if (additionalMessage != null) {
      errorBuilder.append(additionalMessage);
    }

    return errorBuilder.toString();
  }
}
