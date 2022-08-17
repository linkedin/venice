package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should return information about a store, but the store is migrated to another cluster
 */
public class VeniceStoreIsMigratedException extends VeniceException {
  private final String storeName;
  private final String clusterName;
  private final String d2ServiceName;

  public VeniceStoreIsMigratedException(String storeName, String clusterName, String d2ServiceName) {
    super("Store: " + storeName + " is migrated to cluster " + clusterName + ", d2Service " + d2ServiceName);
    this.storeName = storeName;
    this.clusterName = clusterName;
    this.d2ServiceName = d2ServiceName;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getD2ServiceName() {
    return d2ServiceName;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_MOVED_PERMANENTLY;
  }

  public static String getD2ServiceName(String errorMsg) {
    if (errorMsg == null || !errorMsg.contains("d2Service")) {
      return null;
    }
    return errorMsg.substring(errorMsg.lastIndexOf(' ') + 1);
  }
}
