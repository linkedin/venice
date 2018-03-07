package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.codehaus.jackson.annotate.JsonIgnore;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;


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

  private Map<String, String> pushProperties;

  public OfflinePushStatus(String kafkaTopic, int numberOfPartition, int replicationFactor,
      OfflinePushStrategy strategy) {
    this.kafkaTopic = kafkaTopic;
    this.numberOfPartition = numberOfPartition;
    this.replicationFactor = replicationFactor;
    this.strategy = strategy;
    this.pushProperties = new HashMap<>();
    statusHistory = new ArrayList<>();
    addHistoricStatus(currentStatus);
    //initialize partition status for each partition.
    partitionStatuses = new ArrayList<>(numberOfPartition);
    for (int i = 0; i < numberOfPartition; i++) {
      ReadOnlyPartitionStatus partitionStatus = new ReadOnlyPartitionStatus(i, Collections.emptyList());
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

  protected long getStartTimeSec() {
    String timeString = statusHistory.get(0).getTime();
    LocalDateTime time = LocalDateTime.parse(timeString);
    return time.atZone(ZoneId.systemDefault()).toEpochSecond();
  }


  /**
   * Judge whether current status could be transferred to the new status.
   * <p>
   * Push's state machine:
   * <ul>
   *   <li>STARTED->COMPLETED</li>
   *   <li>STARTED->ERROR</li>
   *   <li>STARTED->END_OF_PUSH_RECEIVED</li>
   *   <li>END_OF_PUSH_RECEIVED->COMPLETED</li>
   *   <li>END_OF_PUSH_RECEIVED->ERROR</li>
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
        isValid = Utils.verifyTransition(newStatus, ERROR, COMPLETED, END_OF_PUSH_RECEIVED);
        break;
      case ERROR:
      case COMPLETED:
        isValid = Utils.verifyTransition(newStatus, ARCHIVED);
        break;
      case END_OF_PUSH_RECEIVED:
        isValid = Utils.verifyTransition(newStatus, COMPLETED, ERROR);
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
        if(partitionStatus instanceof ReadOnlyPartitionStatus) {
          partitionStatuses.set(partitionId, partitionStatus);
        }else{
          partitionStatuses.set(partitionId, ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
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
      if (partitionStatus instanceof ReadOnlyPartitionStatus) {
        this.partitionStatuses.add(partitionStatus);
      } else {
        this.partitionStatuses.add(ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
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
    clonePushStatus.setPushProperties(new HashMap<>(pushProperties));
    return clonePushStatus;
  }

  /**
   * @return a map which's id is replica id and value is the offset that replica already consumed.
   */
  @JsonIgnore
  public Map<String, Long> getProgress() {
    Map<String, Long> progress = new HashMap<>();
    for (PartitionStatus partitionStatus : getPartitionStatuses()) {
      //Don't count progress of error tasks
      partitionStatus.getReplicaStatuses()
          .stream()
          //Don't count progress of error tasks
          .filter(replicaStatus -> !replicaStatus.getCurrentStatus().equals(ExecutionStatus.ERROR))
          .forEach(replicaStatus -> {
            String replicaId =
                ReplicaStatus.getReplicaId(kafkaTopic, partitionStatus.getPartitionId(), replicaStatus.getInstanceId());
            progress.put(replicaId, replicaStatus.getCurrentProgress());
          });
    }
    return progress;
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

  /**
   * Checks whether at least one replica of each partition has returned {@link ExecutionStatus#END_OF_PUSH_RECEIVED}
   *
   * This is intended for {@link OfflinePushStatus} instances which belong to Hybrid Stores, though there
   * should be no negative side-effects if called on an instance tied to a non-hybrid store, as the logic
   * should consistently return false in that case.
   *
   * @return true if at least one replica of each partition has consumed an EOP control message, false otherwise
   */
  public boolean isReadyToStartBufferReplay() {
    // Only allow the push in STARTED status to start buffer replay. It could avoid:
    //   1. Send duplicated start buffer replay message.
    //   2. Send start buffer replay message when a push had already been terminated.
    if(!getCurrentStatus().equals(ExecutionStatus.STARTED)) {
      return false;
    }
    return getPartitionStatuses().stream()
        // For all partitions
        .allMatch(partitionStatus -> partitionStatus.getReplicaStatuses().stream()
            // There must be at least one replica which has received the EOP
            .anyMatch(replicaStatus -> replicaStatus.getCurrentStatus() == ExecutionStatus.END_OF_PUSH_RECEIVED));
  }

  public Map<String, String> getPushProperties() {
    return pushProperties;
  }

  public void setPushProperties(Map<String, String> pushProperties) {
    this.pushProperties = pushProperties;
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
    if (!pushProperties.equals(that.pushProperties)) {
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
    result = 31 * result + pushProperties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OfflinePushStatus{" +
        "kafkaTopic='" + kafkaTopic + '\'' +
        ", numberOfPartition=" + numberOfPartition +
        ", replicationFactor=" + replicationFactor +
        ", strategy=" + strategy +
        ", currentStatus=" + currentStatus +
        '}';
  }
}
