package com.linkedin.venice.controllerapi;

public class RecoveryProgressResponse extends ControllerResponse {
  /**
   * Indicates the state of the recovery operation.
   */
  public enum RecoveryStatus {
    /** No recovery has been triggered for this datacenter. */
    NOT_FOUND,
    /** Recovery is currently in progress. */
    IN_PROGRESS,
    /** Recovery has completed (check failedStores for partial success). */
    COMPLETED
  }

  private String datacenterName;
  private RecoveryStatus status;
  private int totalStores;
  private int recoveredStores;
  private int failedStores;
  private int versionsTransitioned;
  private boolean complete;
  private double progressFraction;

  public String getDatacenterName() {
    return datacenterName;
  }

  public void setDatacenterName(String datacenterName) {
    this.datacenterName = datacenterName;
  }

  public RecoveryStatus getStatus() {
    return status;
  }

  public void setStatus(RecoveryStatus status) {
    this.status = status;
  }

  public int getTotalStores() {
    return totalStores;
  }

  public void setTotalStores(int totalStores) {
    this.totalStores = totalStores;
  }

  public int getRecoveredStores() {
    return recoveredStores;
  }

  public void setRecoveredStores(int recoveredStores) {
    this.recoveredStores = recoveredStores;
  }

  public int getFailedStores() {
    return failedStores;
  }

  public void setFailedStores(int failedStores) {
    this.failedStores = failedStores;
  }

  public int getVersionsTransitioned() {
    return versionsTransitioned;
  }

  public void setVersionsTransitioned(int versionsTransitioned) {
    this.versionsTransitioned = versionsTransitioned;
  }

  public boolean isComplete() {
    return complete;
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }

  public double getProgressFraction() {
    return progressFraction;
  }

  public void setProgressFraction(double progressFraction) {
    this.progressFraction = progressFraction;
  }
}
