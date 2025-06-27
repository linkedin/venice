package com.linkedin.venice.controller.multitaskscheduler;

import java.time.Instant;


public class MigrationRecord {
  private final String storeName;
  private final String sourceCluster;
  private final String destinationCluster;
  private Step currentStep;
  private Instant storeMigrationStartTime;
  private int attempts;
  private boolean abortOnFailure = true;
  private boolean isAborted = false;

  public enum Step {
    CHECK_DISK_SPACE(0), PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST(1), VERIFY_MIGRATION_STATUS(2),
    UPDATE_CLUSTER_DISCOVERY(3), VERIFY_READ_REDIRECTION(4), END_MIGRATION(5), MIGRATION_SUCCEED(6);

    private final int stepNumber;

    Step(int stepNumber) {
      this.stepNumber = stepNumber;
    }

    public int getStepNumber() {
      return stepNumber;
    }

    public static Step fromStepNumber(int stepNumber) {
      for (Step step: Step.values()) {
        if (step.stepNumber == stepNumber) {
          return step;
        }
      }
      throw new IllegalArgumentException("Invalid step number: " + stepNumber);
    }
  }

  @Deprecated
  public MigrationRecord(String storeName, String sourceCluster, String destinationCluster, int currentStep) {
    this.storeName = storeName;
    this.sourceCluster = sourceCluster;
    this.destinationCluster = destinationCluster;
    this.currentStep = Step.fromStepNumber(currentStep);
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
    this.isAborted = builder.isAborted;
    this.abortOnFailure = builder.abortOnFailure;
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
    return currentStep.getStepNumber();
  }

  public Step getCurrentStepEnum() {
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
    this.currentStep = Step.fromStepNumber(currentStep);
  }

  public void setCurrentStep(Step currentStep) {
    this.currentStep = currentStep;
  }

  public void resetAttempts() {
    this.attempts = 0;
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

  public boolean getAbortOnFailure() {
    return abortOnFailure;
  }

  public String toString() {
    return String.format(
        "MigrationRecord{storeName='%s', sourceCluster='%s', destinationCluster='%s', "
            + "currentStep=%s(%d), storeMigrationStartTime=%s, attempts=%d, " + "isAborted=%b, abortOnFailure=%b}",
        storeName,
        sourceCluster,
        destinationCluster,
        currentStep,
        currentStep.getStepNumber(),
        storeMigrationStartTime,
        attempts,
        isAborted,
        abortOnFailure);
  }

  public static class Builder {
    private final String storeName;
    private final String sourceCluster;
    private final String destinationCluster;
    private Step currentStep = Step.CHECK_DISK_SPACE;
    private Instant storeMigrationStartTime = Instant.ofEpochMilli(-1);
    private int attempts = 0;
    private boolean isAborted = false;
    private boolean abortOnFailure = true;

    public Builder(String storeName, String sourceCluster, String destinationCluster) {
      this.storeName = storeName;
      this.sourceCluster = sourceCluster;
      this.destinationCluster = destinationCluster;
    }

    public Builder currentStep(int currentStep) {
      this.currentStep = Step.fromStepNumber(currentStep);
      return this;
    }

    public Builder attempts(int attempts) {
      this.attempts = attempts;
      return this;
    }

    public Builder aborted(boolean isAborted) {
      this.isAborted = isAborted;
      return this;
    }

    public Builder abortOnFailure(boolean abortOnFailure) {
      this.abortOnFailure = abortOnFailure;
      return this;
    }

    public MigrationRecord build() {
      return new MigrationRecord(this);
    }
  }
}
