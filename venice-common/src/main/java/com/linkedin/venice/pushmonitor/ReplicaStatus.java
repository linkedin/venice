package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;


/**
 * Class stores replica status and history.
 */
public class ReplicaStatus {
  public static final int MAX_HISTORY_LENGTH = 50;
  private final String instanceId;
  private ExecutionStatus currentStatus = STARTED;
  private long currentProgress = 0;
  /**
   *  This field is only used by incremental push status
   *  Check out {@link ExecutionStatus#START_OF_INCREMENTAL_PUSH_RECEIVED} and
   *  {@link ExecutionStatus#END_OF_INCREMENTAL_PUSH_RECEIVED}
   */
  private String incrementalPushVersion = "";

  private List<StatusSnapshot> statusHistory;

  public ReplicaStatus(String instanceId) {
    this.instanceId = instanceId;
    statusHistory = new LinkedList<>();
  }

  public void updateStatus(ExecutionStatus newStatus) {
    currentStatus = newStatus;
    addHistoricStatus(newStatus);
  }

  public void updateStatus(ExecutionStatus newStatus, String incrementalPushVersion) {
    setIncrementalPushVersion(incrementalPushVersion);
    updateStatus(newStatus);
  }

  public String getInstanceId() {
    return instanceId;
  }

  public ExecutionStatus getCurrentStatus() {
    return currentStatus;
  }

  @SuppressWarnings("unused") // Used by ZK serialize and deserialize
  public void setCurrentStatus(ExecutionStatus currentStatus) {
    this.currentStatus = currentStatus;
  }

  public long getCurrentProgress() {
    return currentProgress;
  }

  public void setCurrentProgress(long currentProgress) {
    this.currentProgress = currentProgress;
  }

  public String getIncrementalPushVersion() {
    return incrementalPushVersion;
  }

  public void setIncrementalPushVersion(String incrementalPushVersion) {
    this.incrementalPushVersion = incrementalPushVersion;
  }

  public List<StatusSnapshot> getStatusHistory() {
    return statusHistory;
  }

  @SuppressWarnings("unused") // Used by ZK serialize and deserialize.
  public void setStatusHistory(List<StatusSnapshot> statusHistory) {
    this.statusHistory = statusHistory;
  }

  private void addHistoricStatus(ExecutionStatus status) {
    // Do not update status in case that replica is already in PROGRESS and target status is also PROGRESS.
    // Because we don't want status history become too long due to lots of PROGRESS statuses.
    if (status.equals(PROGRESS) && !statusHistory.isEmpty() && statusHistory.get(statusHistory.size() - 1)
        .getStatus()
        .equals(PROGRESS)) {
      return;
    }
    // remove the oldest status snapshots once history is too long.
    int statusHistorySize = statusHistory.size();
    if (statusHistorySize >= MAX_HISTORY_LENGTH) {
      statusHistory = statusHistory.subList(statusHistorySize - MAX_HISTORY_LENGTH + 1, statusHistorySize);
    }

    StatusSnapshot snapshot = new StatusSnapshot(status, LocalDateTime.now().toString());
    if (!Utils.isNullOrEmpty(incrementalPushVersion)) {
      snapshot.setIncrementalPushVersion(incrementalPushVersion);
    }
    statusHistory.add(snapshot);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReplicaStatus that = (ReplicaStatus) o;

    if (currentProgress != that.currentProgress) {
      return false;
    }
    if (!instanceId.equals(that.instanceId)) {
      return false;
    }
    if (!incrementalPushVersion.equals(that.incrementalPushVersion)) {
      return false;
    }
    if (currentStatus != that.currentStatus) {
      return false;
    }
    return statusHistory.equals(that.statusHistory);
  }

  @Override
  public int hashCode() {
    int result = instanceId.hashCode();
    result = 31 * result + currentStatus.hashCode();
    result = 31 * result + incrementalPushVersion.hashCode();
    result = 31 * result + (int) (currentProgress ^ (currentProgress >>> 32));
    result = 31 * result + statusHistory.hashCode();
    return result;
  }

  public static String getReplicaId(String kafkaTopic, int partition, String instanceId) {
    return String.format("%s:%d:%s", kafkaTopic, partition, instanceId);
  }

  public static String getInstanceIdFromReplicaId(String replicaId) {
    String[] parts = replicaId.split(":");
    return parts[parts.length - 1];
  }
}
