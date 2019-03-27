package com.linkedin.venice.pushmonitor;

/**
 * The snap shot of status change.
 */
public class StatusSnapshot {
  private final ExecutionStatus status;
  private final String time;
  private String incrementalPushVersion = "";

  public StatusSnapshot(ExecutionStatus status, String time) {
    this.status = status;
    this.time = time;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public String getTime() {
    return time;
  }

  public String getIncrementalPushVersion() {
    return incrementalPushVersion;
  }

  public void setIncrementalPushVersion(String incrementalPushVersion) {
    this.incrementalPushVersion = incrementalPushVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StatusSnapshot that = (StatusSnapshot) o;

    if (status != that.status) {
      return false;
    }
    if (!incrementalPushVersion.equals(that.incrementalPushVersion)) {
      return false;
    }
    return time.equals(that.time);
  }

  @Override
  public int hashCode() {
    int result = status.hashCode();
    result = 31 * result + incrementalPushVersion.hashCode();
    result = 31 * result + time.hashCode();
    return result;
  }
}
