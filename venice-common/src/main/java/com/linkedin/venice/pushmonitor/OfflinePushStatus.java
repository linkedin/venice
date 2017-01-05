package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.linkedin.venice.job.ExecutionStatus.*;


/**
 * Class stores all the statuses and history of one offline push.
 */
public class OfflinePushStatus {
  private final String kafkaTopic;
  private final int numberOfPartition;
  private final int replicationFactor;
  private final OfflinePushStrategy strategy;

  private ExecutionStatus currentStatus = STARTED;
  private List<StatusSnapshot> statusHistory;
  private List<PartitionStatus> partitionStatuses;

  public OfflinePushStatus(String kafkaTopic, int numberOfPartition, int replicationFactor,
      OfflinePushStrategy strategy) {
    this.kafkaTopic = kafkaTopic;
    this.numberOfPartition = numberOfPartition;
    this.replicationFactor = replicationFactor;
    this.strategy = strategy;
    statusHistory = new ArrayList<>();
    addHistoricStatus(currentStatus);
    //initialize partition status for each partition.
    partitionStatuses = new ArrayList<>(numberOfPartition);
    for (int i = 0; i < numberOfPartition; i++) {
      ReadonlyPartitionStatus partitionStatus = new ReadonlyPartitionStatus(i, Collections.emptyList());
      partitionStatuses.add(partitionStatus);
    }
 }

  public void updateStatus(ExecutionStatus newStatus) {
    if (validatePushStatusTransition(newStatus)) {
      currentStatus = newStatus;
      addHistoricStatus(newStatus);
    } else {
      throw new VeniceException("Can not transit status from:" + currentStatus + " to " + newStatus);
    }
  }

  /**
   * Judge whether current status could be transferred to the new status.
   * <p>
   * Push's state machine:
   * <ul>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->ERROR</li>
   *   <li>COMPLETED->ARCHIVED</li>
   *   <li>ERROR->ARCHIVED</li>
   * </ul>
   * @param newStatus
   * @return
   */
  public boolean validatePushStatusTransition(ExecutionStatus newStatus) {
    boolean isValid;
    switch (currentStatus) {
      case STARTED:
        isValid = Utils.verifyTransition(newStatus, ERROR, COMPLETED);
        break;
      case ERROR:
      case COMPLETED:
        isValid = Utils.verifyTransition(newStatus, ARCHIVED);
        break;
      default:
        isValid = false;
    }
    return isValid;
  }

  public void setPartitionStatus(PartitionStatus partitionStatus) {
    if (partitionStatus.getPartitionId() < 0 || partitionStatus.getPartitionId() >= numberOfPartition) {
      throw new VeniceException(
          "Received an invalid partition:" + partitionStatus.getPartitionId() + " for topic:" + kafkaTopic);
    }
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      if (partitionId == partitionStatus.getPartitionId()) {
        if(partitionStatus instanceof ReadonlyPartitionStatus) {
          partitionStatuses.set(partitionId, partitionStatus);
        }else{
          partitionStatuses.set(partitionId, ReadonlyPartitionStatus.fromPartitionStatus(partitionStatus));
        }
        break;
      }
    }
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public OfflinePushStrategy getStrategy() {
    return strategy;
  }

  public ExecutionStatus getCurrentStatus() {
    return currentStatus;
  }

  public void setCurrentStatus(ExecutionStatus currentStatus) {
    this.currentStatus = currentStatus;
  }

  public List<StatusSnapshot> getStatusHistory() {
    return statusHistory;
  }

  public void setStatusHistory(List<StatusSnapshot> statusHistory) {
    this.statusHistory = statusHistory;
  }

  // Only used by accessor while loading data from Zookeeper.
  public void setPartitionStatuses(List<PartitionStatus> partitionStatuses) {
    this.partitionStatuses.clear();
    for (PartitionStatus partitionStatus : partitionStatuses) {
      if (partitionStatus instanceof ReadonlyPartitionStatus) {
        this.partitionStatuses.add(partitionStatus);
      } else {
        this.partitionStatuses.add(ReadonlyPartitionStatus.fromPartitionStatus(partitionStatus));
      }
    }
  }

  public OfflinePushStatus clonePushStatus() {
    OfflinePushStatus clonePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    clonePushStatus.setCurrentStatus(currentStatus);
    // Status history is append-only. So here we don't need to deep copy each object in this list. Simply copy the list
    // itself is able to avoid affecting the object while updating the cloned one.
    clonePushStatus.setStatusHistory(new ArrayList<>(statusHistory));
    // As same as status history, there is no way update properties inside Partition status object. So only
    // copy list is enough here.
    clonePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatuses));
    return clonePushStatus;
  }

  protected List<PartitionStatus> getPartitionStatuses() {
    return Collections.unmodifiableList(partitionStatuses);
  }

  protected PartitionStatus getPartitionStatus(int partitionId) {
    return partitionStatuses.get(partitionId);
  }

  private void addHistoricStatus(ExecutionStatus status) {
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

    OfflinePushStatus that = (OfflinePushStatus) o;

    if (numberOfPartition != that.numberOfPartition) {
      return false;
    }
    if (replicationFactor != that.replicationFactor) {
      return false;
    }
    if (!kafkaTopic.equals(that.kafkaTopic)) {
      return false;
    }
    if (strategy != that.strategy) {
      return false;
    }
    if (currentStatus != that.currentStatus) {
      return false;
    }
    if (!statusHistory.equals(that.statusHistory)) {
      return false;
    }
    return partitionStatuses.equals(that.partitionStatuses);
  }

  @Override
  public int hashCode() {
    int result = kafkaTopic.hashCode();
    result = 31 * result + numberOfPartition;
    result = 31 * result + replicationFactor;
    result = 31 * result + strategy.hashCode();
    result = 31 * result + currentStatus.hashCode();
    result = 31 * result + statusHistory.hashCode();
    result = 31 * result + partitionStatuses.hashCode();
    return result;
  }
}
