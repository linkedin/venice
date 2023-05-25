package com.linkedin.venice.pushmonitor;

public class ExecutionStatusWithDetails {
  private final ExecutionStatus status;
  private final String details;

  public ExecutionStatusWithDetails(ExecutionStatus status, String details) {
    this.status = status;
    this.details = details;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public String getDetails() {
    return details;
  }
}
