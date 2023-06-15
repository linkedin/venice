package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.isIncrementalPushStatus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.utils.Pair;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;


/**
 * Class stores replica status and history.
 */
public class ReplicaStatus {
  public static final int MAX_HISTORY_LENGTH = 50;
  public static final long NO_PROGRESS = -1;
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

  @JsonCreator
  public ReplicaStatus(@JsonProperty("instanceId") String instanceId) {
    this(instanceId, true);
  }

  public ReplicaStatus(String instanceId, boolean enableStatusHistory) {
    this.instanceId = instanceId;
    this.statusHistory = enableStatusHistory ? new LinkedList<>() : null;
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
    if (currentProgress != NO_PROGRESS) {
      this.currentProgress = currentProgress;
    }
  }

  public String getIncrementalPushVersion() {
    return incrementalPushVersion;
  }

  public void setIncrementalPushVersion(String incrementalPushVersion) {
    this.incrementalPushVersion = incrementalPushVersion;
  }

  public List<StatusSnapshot> getStatusHistory() {
    return statusHistory == null ? Collections.emptyList() : statusHistory;
  }

  @SuppressWarnings("unused") // Used by ZK serialize and deserialize.
  public void setStatusHistory(List<StatusSnapshot> statusHistory) {
    this.statusHistory = statusHistory;
  }

  private void addHistoricStatus(ExecutionStatus status) {
    if (statusHistory == null) {
      // Status history is disabled
      return;
    }
    // Do not update status in case that replica is already in PROGRESS and target status is also PROGRESS.
    // Because we don't want status history become too long due to lots of PROGRESS statuses.
    if (status.equals(PROGRESS) && !statusHistory.isEmpty()
        && statusHistory.get(statusHistory.size() - 1).getStatus().equals(PROGRESS)) {
      return;
    }

    /**
     * Removed old statuses when status list is over MAX_HISTORY_LENGTH.
     * Preserve:
     * 1. TOPIC_SWITCH_RECEIVED and END_OF_PUSH_RECEIVED, if there is only one single copy
     * 2. COMPLETED and inc pushes, if there is only single copy
     *
     * TODO: If parallel inc push doesn't need inc status history, we can choose to keep only the current inc push status.
     */
    removeOldStatuses();

    StatusSnapshot snapshot = new StatusSnapshot(status, LocalDateTime.now().toString());
    if (!StringUtils.isEmpty(incrementalPushVersion)) {
      snapshot.setIncrementalPushVersion(incrementalPushVersion);
    }
    statusHistory.add(snapshot);
  }

  private void removeOldStatuses() {
    if (statusHistory == null || statusHistory.size() < MAX_HISTORY_LENGTH) {
      return;
    }

    long completeCount = 0, incPushCount = 0, topicSwitchCount = 0, endOfPushCount = 0;
    for (StatusSnapshot snapshot: statusHistory) {
      ExecutionStatus status = snapshot.getStatus();
      if (status == COMPLETED) {
        completeCount++;
      } else if (isIncrementalPushStatus(status)) {
        incPushCount++;
      } else if (status == TOPIC_SWITCH_RECEIVED) {
        topicSwitchCount++;
      } else if (status == END_OF_PUSH_RECEIVED) {
        endOfPushCount++;
      }
    }

    Iterator<StatusSnapshot> itr = statusHistory.iterator();
    while (itr.hasNext()) {
      if (statusHistory.size() < MAX_HISTORY_LENGTH) {
        break;
      }
      StatusSnapshot oldSnapshot = itr.next();
      ExecutionStatus oldStatus = oldSnapshot.getStatus();
      if (isIncrementalPushStatus(oldStatus)
          && oldSnapshot.getIncrementalPushVersion().equals(incrementalPushVersion)) {
        continue;
      }

      if (oldStatus == COMPLETED) {
        if (completeCount > 1) {
          itr.remove();
          completeCount--;
        }
        continue;
      }

      if (isIncrementalPushStatus(oldStatus)) {
        if (incPushCount > 1) {
          itr.remove();
          incPushCount--;
        }
        continue;
      }

      if (oldStatus == TOPIC_SWITCH_RECEIVED) {
        if (topicSwitchCount > 1) {
          itr.remove();
          topicSwitchCount--;
        }
        continue;
      }

      if (oldStatus == END_OF_PUSH_RECEIVED) {
        if (endOfPushCount > 1) {
          itr.remove();
          endOfPushCount--;
        }
        continue;
      }

      itr.remove();
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
    return Objects.equals(statusHistory, that.statusHistory);
  }

  @Override
  public int hashCode() {
    int result = instanceId.hashCode();
    result = 31 * result + currentStatus.hashCode();
    result = 31 * result + incrementalPushVersion.hashCode();
    result = 31 * result + (int) (currentProgress ^ (currentProgress >>> 32));
    result = 31 * result + Objects.hashCode(statusHistory);
    return result;
  }

  public static String getReplicaId(String kafkaTopic, int partition, String instanceId) {
    return String.format("%s:%d:%s", kafkaTopic, partition, instanceId);
  }

  public static String getInstanceIdFromReplicaId(String replicaId) {
    String[] parts = replicaId.split(":");
    return parts[parts.length - 1];
  }

  /**
   * @return the started and completed status snapshot from the status history.
   */
  public Pair<StatusSnapshot, StatusSnapshot> findStartedAndCompletedStatus() {
    int idx = 0, startedIdx = -1, completedIdx = -1;
    StatusSnapshot startedStatus = null, completedStatus = null;

    // We assume that StatusSnapshot in statusHistory is sorted by its time.
    Iterator<StatusSnapshot> iter = getStatusHistory().listIterator();
    while (iter.hasNext() && (startedStatus == null || completedStatus == null)) {
      StatusSnapshot status = iter.next();
      if (status.getStatus() == STARTED && startedStatus == null) {
        startedStatus = status;
        startedIdx = idx;
      } else if (status.getStatus() == COMPLETED && completedStatus == null) {
        completedStatus = status;
        completedIdx = idx;
      }
      idx++;
    }

    // Cannot find a started and completed pair.
    if (startedStatus == null || completedStatus == null) {
      return null;
    }

    /**
     * When server restarts, it adds new started and completed statuses into the history.
     * (see {@link StoreIngestionTask#checkConsumptionStateWhenStart}). We compare the distance between two the indexes
     * and claim that for a real <started, completed> pair, there should be other states in between.
     */
    if (completedIdx - startedIdx <= 1) {
      return null;
    }

    return Pair.create(startedStatus, completedStatus);
  }
}
