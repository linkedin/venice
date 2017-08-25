package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;


/**
 * Class stores replica status and history.
 */
public class ReplicaStatus {
  public static final int MAX_HISTORY_LENGTH = 100;
  private final String instanceId;
  private ExecutionStatus currentStatus = STARTED;
  private long currentProgress = 0;

  private List<StatusSnapshot> statusHistory;

  public ReplicaStatus(String instanceId) {
    this.instanceId = instanceId;
    statusHistory = new LinkedList<>();
    addHistoricStatus(currentStatus);
  }

  public void updateStatus(ExecutionStatus newStatus) {
    if (validateReplicaStatusTransition(newStatus)) {
      currentStatus = newStatus;
      addHistoricStatus(newStatus);
    } else {
      throw new VeniceException("Can not transit from " + currentStatus + " to " + newStatus);
    }
  }

  /**
   * Judge whether current status could be transferred to new status. Note, because each status could be transferred to
   * START again in case that replica is re-allocated to the same server again after it was moved out.
   * <p>
   * Replica status' state machine:
   * <ul>
   *   <li>STARTED->PROGRESS</li>
   *   <li>STARTED->ERROR</li>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->STARTED</li>
   *   <li>PROGRESS->PROGRESS</li>
   *   <li>PROGRESS->ERROR</li>
   *   <li>PROGRESS->COMPLETED</li>
   *   <li>PROGRESS->START</li>
   *   <li>PROGRESS->END_OF_PUSH_RECEIVED</li>
   *   <li>PROGRESS->START_OF_BUFFER_REPLAY_RECEIVED</li>
   *   <li>ERROR->START</li>
   *   <li>COMPLETED->START</li>
   *   <li>COMPLETED->ERROR</li>
   *   <li>END_OF_PUSH_RECEIVED->START</li>
   *   <li>END_OF_PUSH_RECEIVED->START_OF_BUFFER_REPLAY_RECEIVED</li>
   *   <li>END_OF_PUSH_RECEIVED->ERROR</li>
   *   <li>END_OF_PUSH_RECEIVED->COMPLETED</li>
   *   <li>START_OF_BUFFER_REPLAY_RECEIVED->STARTED</li>
   *   <li>START_OF_BUFFER_REPLAY_RECEIVED->ERROR</li>
   *   <li>START_OF_BUFFER_REPLAY_RECEIVED->PROGRESS</li>
   *   <li>START_OF_BUFFER_REPLAY_RECEIVED->COMPLETED</li>
   * </ul>
   * @param newStatus
   * @return
   */
  private boolean validateReplicaStatusTransition(ExecutionStatus newStatus) {
    boolean isValid;
    switch (currentStatus) {
      case STARTED:
      case PROGRESS:
        isValid = Utils.verifyTransition(newStatus, STARTED, PROGRESS, END_OF_PUSH_RECEIVED, START_OF_BUFFER_REPLAY_RECEIVED, ERROR, COMPLETED);
        break;
      case ERROR:
        isValid = Utils.verifyTransition(newStatus, STARTED);
        break;
      case COMPLETED:
        isValid = Utils.verifyTransition(newStatus, STARTED, ERROR);
        break;
      case END_OF_PUSH_RECEIVED:
        isValid = Utils.verifyTransition(newStatus, STARTED, START_OF_BUFFER_REPLAY_RECEIVED, ERROR, COMPLETED);
        break;
      case START_OF_BUFFER_REPLAY_RECEIVED:
        isValid = Utils.verifyTransition(newStatus, STARTED, ERROR, PROGRESS, COMPLETED);
        break;
      default:
        isValid = false;
    }
    return isValid;
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
    // remove the oldest status snapshot once history is too long.
    if (statusHistory.size() == MAX_HISTORY_LENGTH) {
      statusHistory.remove(0);
    }
    statusHistory.add(new StatusSnapshot(status, LocalDateTime.now().toString()));
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
    if (currentStatus != that.currentStatus) {
      return false;
    }
    return statusHistory.equals(that.statusHistory);
  }

  @Override
  public int hashCode() {
    int result = instanceId.hashCode();
    result = 31 * result + currentStatus.hashCode();
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
