package com.linkedin.venice.pushmonitor;

public class ExecutionStatusWithDetails {
  private final ExecutionStatus status;
  private final String details;
  private final boolean noDaVinciStatusReport;

  public ExecutionStatusWithDetails(ExecutionStatus status, String details, boolean noDaVinciStatusReport) {
    this.status = status;
    this.details = details;
    this.noDaVinciStatusReport = noDaVinciStatusReport;
  }

  public ExecutionStatusWithDetails(ExecutionStatus status, String details) {
    this(status, details, true);
  }

  public ExecutionStatusWithDetails(ExecutionStatus status) {
    this(status, null, true);
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public String getDetails() {
    return details;
  }

  public boolean isNoDaVinciStatusReport() {
    return noDaVinciStatusReport;
  }
}
