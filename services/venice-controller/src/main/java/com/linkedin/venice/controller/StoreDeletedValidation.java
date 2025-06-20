package com.linkedin.venice.controller;

/**
 * Result object for store deletion validation operations.
 * Contains information about whether a store has been successfully deleted
 * and what resources might still exist if deletion is incomplete.
 */
public class StoreDeletedValidation {
  private final String storeName;
  private final String clusterName;
  private boolean isDeleted = true; // Default to deleted unless proven otherwise
  private String error;

  public StoreDeletedValidation(String clusterName, String storeName) {
    this.clusterName = clusterName;
    this.storeName = storeName;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public String getError() {
    return error;
  }

  /**
   * Marks the store as not fully deleted with the specified reason.
   * 
   * @param reason the reason why the store is not considered fully deleted
   */
  public void setStoreNotDeleted(String reason) {
    this.isDeleted = false;
    this.error = reason;
  }

  @Override
  public String toString() {
    if (isDeleted) {
      return String.format("Store '%s' in cluster '%s' is fully deleted", storeName, clusterName);
    } else {
      return String.format("Store '%s' in cluster '%s' is NOT fully deleted: %s", storeName, clusterName, error);
    }
  }
}
