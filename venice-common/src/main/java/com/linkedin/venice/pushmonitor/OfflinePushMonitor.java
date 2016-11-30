package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;


/**
 * Monitor used to track all of offline push statuses.
 */
public class OfflinePushMonitor implements OfflinePushMonitorAccessor.PartitionStatusListener {
  private static final Logger logger = Logger.getLogger(OfflinePushMonitor.class);
  private final OfflinePushMonitorAccessor accessor;
  private final String clusterName;
  private final RoutingDataRepository routingDataRepository;

  private Map<String, OfflinePushStatus> topicToPushMap;

  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushMonitorAccessor accessor) {
    this.clusterName = clusterName;
    this.routingDataRepository = routingDataRepository;
    this.accessor = accessor;
    this.topicToPushMap = new HashMap<>();
  }

  public synchronized void loadAllPushes() {
    List<OfflinePushStatus> statues = accessor.loadOfflinePushStatusesAndPartitionStatuses();
    Map<String, OfflinePushStatus> newTopicToPushMap = new HashMap<>();
    for (OfflinePushStatus status : statues) {
      newTopicToPushMap.put(status.getKafkaTopic(), status);
    }
    topicToPushMap = newTopicToPushMap;
  }

  /**
   * Start monitoring the offline push. This method will create data structure to record status and history of offline
   * push and its partitions. And also subscribe the change of each partition so that monitor could handle accordingly
   * once partition's status is changed.
   */
  public synchronized void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor,
      OfflinePushStrategy strategy) {
    if (topicToPushMap.containsKey(kafkaTopic)) {
      throw new VeniceException(
          "Push status has already been created for topic:" + kafkaTopic + " in cluster:" + clusterName);
    }
    OfflinePushStatus pushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicaFactor, strategy);
    accessor.createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    topicToPushMap.put(kafkaTopic, pushStatus);
    accessor.subscribePartitionStatusChange(pushStatus, this);
    logger.info("Start monitoring push on topic:" + kafkaTopic);
  }

  private ExecutionStatus calculateNewPushStatus(OfflinePushStatus pushStatus, PartitionStatus newPartitionStatus) {
    // TODO implement push status decider.
    return ExecutionStatus.STARTED;
  }

  private void handleCompletedPush(OfflinePushStatus pushStatus) {
    //TODO handle completed push
  }

  public void handleErrorPush(OfflinePushStatus pushStatus) {
    //TODO handle error push
  }

  @Override
  public synchronized void onPartitionStatusChange(String topic, PartitionStatus partitionStatus) {
    // TODO more fine-grained concurrency control here, might lock on push level instead of lock the whole map.
    OfflinePushStatus offlinePushStatus = topicToPushMap.get(topic);
    if (offlinePushStatus == null) {
      logger.error(
          "Can not find Offline push for topic:" + topic + ", ignore the partition status change notification.");
      return;
    } else if (offlinePushStatus.getCurrentStatus().isTerminal()) {
      logger.info(
          "Offline push for topic:" + topic + " already terminated, ignore the partition status change notification.");
    } else if (offlinePushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
      logger.info("Receive notification for push in topic:" + topic + ", status change in partition:"
          + partitionStatus.getPartitionId());
      ExecutionStatus newStatus = calculateNewPushStatus(offlinePushStatus, partitionStatus);
      if (newStatus.equals(ExecutionStatus.COMPLETED)) {
        handleCompletedPush(offlinePushStatus);
      } else if (newStatus.equals(ExecutionStatus.ERROR)) {
        handleErrorPush(offlinePushStatus);
      }
    } else {
      logger.error("Offline push for topic:" + topic + " is in invalid status:" + offlinePushStatus.getCurrentStatus()
          + ", ignore the partition status change notification.");
    }
  }
}
