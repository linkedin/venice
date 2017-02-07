package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Monitor used to track all of offline push statuses. Monitor would watch the routing data(eg Helix external view) for
 * each offline push job. Once the routing data is changed,  monitor would get a notification push and change the
 * offline push job status accordingly.
 * <p>
 * For example, once a replica become ONLINE from BOOTSTRAP, monitor would check is the whole push completed and active
 * the related version if the answer is yes. Or in case of node failure, monitor would fail the push job in some cases.
 */
public class OfflinePushMonitor implements OfflinePushAccessor.PartitionStatusListener, RoutingDataRepository.RoutingDataChangedListener {
  private static final Logger logger = Logger.getLogger(OfflinePushMonitor.class);
  private final OfflinePushAccessor accessor;
  private final String clusterName;
  private final RoutingDataRepository routingDataRepository;

  private Map<String, OfflinePushStatus> topicToPushMap;

  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor accessor) {
    this.clusterName = clusterName;
    this.routingDataRepository = routingDataRepository;
    this.accessor = accessor;
    this.topicToPushMap = new HashMap<>();
  }

  /**
   * Load all of push from accessor and update the push status to represent the current replicas statuses.
   */
  public synchronized void loadAllPushes() {
    List<OfflinePushStatus> offlinePushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();
    Map<String, OfflinePushStatus> newTopicToPushMap = new HashMap<>();
    for (OfflinePushStatus status : offlinePushes) {
      newTopicToPushMap.put(status.getKafkaTopic(), status);
    }
    topicToPushMap = newTopicToPushMap;

    // Check the status for the running push. In case controller missed some notification during the failover, we
    // need to update it based on current routing data.
    topicToPushMap.values()
        .stream()
        .filter(offlinePush -> offlinePush.getCurrentStatus().equals(ExecutionStatus.STARTED))
        .forEach(offlinePush -> {
          updatePushStatus(offlinePush, routingDataRepository.getPartitionAssignments(offlinePush.getKafkaTopic()));
        });
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
    routingDataRepository.subscribeRoutingDataChange(kafkaTopic, this);
    logger.info("Start monitoring push on topic:" + kafkaTopic);
  }

  /**
   * Stop monitoring the given offline push and collect the push and its partitions statuses from remote storage.
   */
  public synchronized void stopMonitorOfflinePush(String kafkaTopic) {
    if (!topicToPushMap.containsKey(kafkaTopic)) {
      logger.error("Push status does not exist for topic:" + kafkaTopic + " in cluster:" + clusterName);
      return;
    }
    OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
    accessor.unsubscribePartitionsStatusChange(pushStatus, this);
    accessor.deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
    topicToPushMap.remove(kafkaTopic);
    logger.info("Stop monitoring push on topic:" + kafkaTopic);
  }

  /**
   * Get the offline push for the given topic.
   */
  public synchronized OfflinePushStatus getOfflinePush(String topic) {
    if (topicToPushMap.containsKey(topic)) {
      return topicToPushMap.get(topic);
    } else {
      throw new VeniceException("Can not find offline push status for topic:" + topic);
    }
  }

  public synchronized ExecutionStatus getOfflinePushStatus(String topic) {
    if (topicToPushMap.containsKey(topic)) {
      return topicToPushMap.get(topic).getCurrentStatus();
    } else {
      return ExecutionStatus.NOT_CREATED;
    }
  }

  @Override
  public synchronized void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    // TODO more fine-grained concurrency control here, might lock on push level instead of lock the whole map.
    OfflinePushStatus offlinePushStatus = topicToPushMap.get(topic);
    if (offlinePushStatus == null) {
      logger.error(
          "Can not find Offline push for topic:" + topic + ", ignore the partition status change notification.");
      return;
    } else {
      // On controller side, partition status is read only. It could only be updated by storage node.
      offlinePushStatus.setPartitionStatus(partitionStatus);
    }
  }

  @Override
  public synchronized void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    logger.info("Received the routing data changed notification for topic:" + partitionAssignment.getTopic());
    String kafkaTopic = partitionAssignment.getTopic();
    OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
    if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
      updatePushStatus(pushStatus, partitionAssignment);
    } else {
      logger.info("Can not find a running offline push for topic:" + partitionAssignment.getTopic()
          + ", ignore the routing data changed notification.");
    }
  }

  @Override
  public synchronized void onRoutingDataDeleted(String kafkaTopic) {
    OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
    if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
      logger.info("Resource for Topic:" + kafkaTopic + " is deleted, stopping the running push.");
      handleErrorPush(pushStatus);
    }
  }

  private void updatePushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(pushStatus.getStrategy());
    ExecutionStatus status = statusDecider.checkPushStatus(pushStatus, partitionAssignment);
    if (status.equals(ExecutionStatus.COMPLETED)) {
      handleCompletedPush(pushStatus);
    } else if (status.equals(ExecutionStatus.ERROR)) {
      handleErrorPush(pushStatus);
    }
  }

  private void handleCompletedPush(OfflinePushStatus pushStatus) {
    OfflinePushStatus clonedPushStatus = pushStatus.clonePushStatus();
    clonedPushStatus.updateStatus(ExecutionStatus.COMPLETED);
    // Update remote storage
    accessor.updateOfflinePushStatus(clonedPushStatus);
    // Update local copy
    topicToPushMap.put(pushStatus.getKafkaTopic(), clonedPushStatus);
    //TODO cleanup retired version
    logger.info("Offline push for topic:" + pushStatus.getKafkaTopic() + " is completed.");
  }

  private void handleErrorPush(OfflinePushStatus pushStatus) {
    OfflinePushStatus clonedPushStatus = pushStatus.clonePushStatus();
    clonedPushStatus.updateStatus(ExecutionStatus.ERROR);
    // Update remote storage
    accessor.updateOfflinePushStatus(clonedPushStatus);
    // Update local copy
    topicToPushMap.put(pushStatus.getKafkaTopic(), clonedPushStatus);
    // TODO cleanup failed version
    logger.info("Offline push for topic:" + pushStatus.getKafkaTopic() + " fails.");
  }
}
