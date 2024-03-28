package com.linkedin.venice.pushmonitor;

public class ExecutionStatusWithDetails {
  private final ExecutionStatus status;
  private final String details;
  private final boolean noDaVinciStatusReport;
  private final Long statusUpdateTimestamp;

  public ExecutionStatusWithDetails(
      ExecutionStatus status,
      String details,
      boolean noDaVinciStatusReport,
      Long statusUpdateTimestamp) {
    this.status = status;
    this.details = details;
    this.noDaVinciStatusReport = noDaVinciStatusReport;
    this.statusUpdateTimestamp = statusUpdateTimestamp;
  }

  public ExecutionStatusWithDetails(ExecutionStatus status, String details, boolean noDaVinciStatusReport) {
    this(status, details, noDaVinciStatusReport, null);
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

  /**
   * N.B.: This is not available in older controller versions, and not available for all statuses (e.g. if the status is
   * {@link ExecutionStatus#NOT_CREATED}, it does not have this timestamp).
   *
   * @return the UNIX Epoch timestamp of the last update to the status, or null if not available.
   */
  public Long getStatusUpdateTimestamp() {
    return statusUpdateTimestamp;
  }
}
