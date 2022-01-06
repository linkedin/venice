package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;


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

  /**
   * Incremental push job version id follows such pattern: timestampInMs_hadoopClusterName_jobExecutionId
   */
  public static long getIncrementalPushJobTimeInMs(String incPushVersionId) {
    if (Utils.isNullOrEmpty(incPushVersionId)) {
      throw new VeniceException(incPushVersionId + " is not a valid incremental push version id");
    }
    if (incPushVersionId.indexOf("_") > 0) {
      return Long.parseLong(incPushVersionId.substring(0, incPushVersionId.indexOf("_")).trim());
    } else {
      return Long.parseLong(incPushVersionId);
    }
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
