package com.linkedin.venice.exceptions;

import java.util.Optional;
import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should return information about a store, but the store does not exist
 */
public class VeniceNoStoreException extends VeniceException {
  private final String storeName;
  private final String clusterName;

  public VeniceNoStoreException(String storeName, String clusterName) {
    super("Store: " + storeName + " does not exist in cluster " + clusterName);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceNoStoreException(String storeName, String clusterName, Throwable t) {
    super("Store: " + storeName + " does not exist in cluster " + clusterName, t);
    this.storeName = storeName;
    this.clusterName = clusterName;
  }

  public VeniceNoStoreException(String storeName) {
    super("Store: " + storeName + " does not exist");
    this.storeName = storeName;
    this.clusterName = "unspecified";
  }

  public VeniceNoStoreException(String storeName, Optional<String> additionalMessage) {
    super("Store: " + storeName + " does not exist. " + (additionalMessage.orElse("")));
    this.storeName = storeName;
    this.clusterName = "unspecified";
  }

  public VeniceNoStoreException(String storeName, Throwable t) {
    super("Store: " + storeName + " does not exist", t);
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
}
