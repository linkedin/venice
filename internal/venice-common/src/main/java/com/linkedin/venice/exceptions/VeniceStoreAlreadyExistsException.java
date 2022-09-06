package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should create a store, but the store already exists
 */
public class VeniceStoreAlreadyExistsException extends VeniceException {
  private final String storeName;

  private final String clusterName;

  public VeniceStoreAlreadyExistsException(String storeName, String clusterName) {
    super("Store: " + storeName + " already exists in cluster " + clusterName);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceStoreAlreadyExistsException(String storeName, String clusterName, Throwable t) {
    super("Store: " + storeName + " already exists in cluster " + clusterName, t);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceStoreAlreadyExistsException(String storeName) {
    super("Store: " + storeName + " already exists");
    this.storeName = storeName;
    this.clusterName = "unspecified";
  }

  public VeniceStoreAlreadyExistsException(String storeName, Throwable t) {
    super("Store: " + storeName + " already exists", t);
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
    return HttpStatus.SC_CONFLICT;
  }
}
