package com.linkedin.venice.controllerapi;

/**
 * Response for store deletion validation operations.
 * Contains information about whether a store has been successfully deleted
 * and what resources might still exist if deletion is incomplete.
 */
public class StoreDeletedValidationResponse extends ControllerResponse {
  private boolean storeDeleted;
  private String reason;

  public boolean isStoreDeleted() {
    return storeDeleted;
  }

  public void setStoreDeleted(boolean storeDeleted) {
    this.storeDeleted = storeDeleted;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }
}
