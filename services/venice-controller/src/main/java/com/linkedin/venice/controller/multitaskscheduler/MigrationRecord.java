package com.linkedin.venice.controller.multitaskscheduler;

import java.time.Instant;


public class MigrationRecord {
  private final String storeName;
  private final String sourceCluster;
  private final String destinationCluster;
  private int currentStep;
  private Instant storeMigrationStartTime;
  private int attempts;
  private boolean isAborted = false;

  @Deprecated
  public MigrationRecord(String storeName, String sourceCluster, String destinationCluster, int currentStep) {
    this.storeName = storeName;
    this.sourceCluster = sourceCluster;
    this.destinationCluster = destinationCluster;
    this.currentStep = currentStep;
    this.storeMigrationStartTime = Instant.ofEpochMilli(-1);
    this.attempts = 0;
  }

  private MigrationRecord(Builder builder) {
    this.storeName = builder.storeName;
    this.sourceCluster = builder.sourceCluster;
    this.destinationCluster = builder.destinationCluster;
    this.currentStep = builder.currentStep;
    this.storeMigrationStartTime = builder.storeMigrationStartTime;
    this.attempts = builder.attempts;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getSourceCluster() {
    return sourceCluster;
  }

  public String getDestinationCluster() {
    return destinationCluster;
  }

  public int getCurrentStep() {
    return currentStep;
  }

  public Instant getStoreMigrationStartTime() {
    return storeMigrationStartTime;
  }

  public void setStoreMigrationStartTime(Instant storeMigrationStartTime) {
    this.storeMigrationStartTime = storeMigrationStartTime;
  }

  public int getAttempts() {
    return attempts;
  }

  public void setCurrentStep(int currentStep) {
    this.currentStep = currentStep;
  }

  public void incrementAttempts() {
    this.attempts++;
  }

  public void setIsAborted(boolean isAborted) {
    this.isAborted = isAborted;
  }

  public boolean getIsAborted() {
    return isAborted;
  }

  public String toString() {
    return "MigrationRecord{" + "storeName='" + storeName + '\'' + ", sourceCluster='" + sourceCluster + '\''
        + ", destinationCluster='" + destinationCluster + '\'' + ", currentStep=" + currentStep
        + ", storeMigrationStartTime=" + storeMigrationStartTime + ", attempts=" + attempts + ", isAborted=" + isAborted
        + '}';
  }

  public static class Builder {
    private final String storeName;
    private final String sourceCluster;
    private final String destinationCluster;
    private int currentStep = 0;
    private Instant storeMigrationStartTime = Instant.ofEpochMilli(-1);
    private int attempts = 0;

    public Builder(String storeName, String sourceCluster, String destinationCluster) {
      this.storeName = storeName;
      this.sourceCluster = sourceCluster;
      this.destinationCluster = destinationCluster;
    }

    public Builder currentStep(int currentStep) {
      this.currentStep = currentStep;
      return this;
    }

    public Builder attempts(int attempts) {
      this.attempts = attempts;
      return this;
    }

    public MigrationRecord build() {
      return new MigrationRecord(this);
    }
  }
}
