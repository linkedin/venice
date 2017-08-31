package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.linkedin.venice.replication.TopicReplicator;
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
  public static final int MAX_ERROR_PUSH_TO_KEEP = 5;
  private static final Logger logger = Logger.getLogger(OfflinePushMonitor.class);
  private final OfflinePushAccessor accessor;
  private final String clusterName;
  private final RoutingDataRepository routingDataRepository;
  private final ReadWriteStoreRepository metadataRepository;
  private final StoreCleaner storeCleaner;
  private final Object lock;

  private Map<String, OfflinePushStatus> topicToPushMap;
  private Optional<TopicReplicator> topicReplicator = Optional.empty();

  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor accessor, StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository) {
    this.clusterName = clusterName;
    this.routingDataRepository = routingDataRepository;
    this.accessor = accessor;
    this.topicToPushMap = new HashMap<>();
    this.storeCleaner = storeCleaner;
    this.metadataRepository = metadataRepository;

    // This is the VeniceHelixAdmin.  Any locking should be done on this object.  If we just use
    // the synchronized keyword to lock on the OfflinePushMonitor itself, then we have a deadlock
    // condition for any use of the storeCleaner.
    lock = storeCleaner;
  }

  /**
   * Load all of push from accessor and update the push status to represent the current replicas statuses.
   */
  public void loadAllPushes() {
    synchronized (lock) {
      List<OfflinePushStatus> offlinePushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();
      Map<String, OfflinePushStatus> newTopicToPushMap = new HashMap<>();
      for (OfflinePushStatus status : offlinePushes) {
        newTopicToPushMap.put(status.getKafkaTopic(), status);
        accessor.subscribePartitionStatusChange(status, this);
      }
      topicToPushMap = newTopicToPushMap;

      // Check the status for the running push. In case controller missed some notification during the failover, we
      // need to update it based on current routing data.
      topicToPushMap.values()
          .stream()
          .filter(offlinePush -> offlinePush.getCurrentStatus().equals(ExecutionStatus.STARTED))
          .forEach(offlinePush -> {
            routingDataRepository.subscribeRoutingDataChange(offlinePush.getKafkaTopic(), this);
            ExecutionStatus status = checkPushStatus(offlinePush, routingDataRepository.getPartitionAssignments(offlinePush.getKafkaTopic()));
            terminateOfflinePush(offlinePush, status);
          });
    }
  }

  /**
   * Start monitoring the offline push. This method will create data structure to record status and history of offline
   * push and its partitions. And also subscribe the change of each partition so that monitor could handle accordingly
   * once partition's status is changed.
   */
  public void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor,
      OfflinePushStrategy strategy) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(kafkaTopic)) {
        throw new VeniceException("Push status has already been created for topic:" + kafkaTopic + " in cluster:" + clusterName);
      }
      OfflinePushStatus pushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicaFactor, strategy);
      accessor.createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
      topicToPushMap.put(kafkaTopic, pushStatus);
      accessor.subscribePartitionStatusChange(pushStatus, this);
      routingDataRepository.subscribeRoutingDataChange(kafkaTopic, this);
      logger.info("Start monitoring push on topic:" + kafkaTopic);
    }
  }

  /**
   * Stop monitoring the given offline push and collect the push and its partitions statuses from remote storage.
   */
  public void stopMonitorOfflinePush(String kafkaTopic) {
    synchronized (lock) {
      if (!topicToPushMap.containsKey(kafkaTopic)) {
        logger.warn("Push status does not exist for topic:" + kafkaTopic + " in cluster:" + clusterName);
        return;
      }
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
      accessor.unsubscribePartitionsStatusChange(pushStatus, this);
      if (pushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        cleanOlderErrorPushes(pushStatus);
      } else {
        accessor.deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus);
        topicToPushMap.remove(kafkaTopic);
      }
      logger.info("Stop monitoring push on topic:" + kafkaTopic);
    }
  }

  /**
   * Once offline push failed, we want to keep some latest offline push in ZK for debug.
   */
  private void cleanOlderErrorPushes(OfflinePushStatus pushStatus) {
    String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
    List<Integer> versionNumbers = topicToPushMap.keySet()
        .stream()
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .map(Version::parseVersionFromKafkaTopicName)
        .collect(Collectors.toList());
    // Only keep MAX_ERROR_PUSH_TO_KEEP pushes for debug.
    while (versionNumbers.size() > MAX_ERROR_PUSH_TO_KEEP) {
      int oldestVersionNumber = Collections.min(versionNumbers);
      versionNumbers.remove(Integer.valueOf(oldestVersionNumber));

      String topicName = Version.composeKafkaTopic(storeName, oldestVersionNumber);
      topicToPushMap.remove(topicName);
    }
  }

  /**
   * Get the offline push for the given topic.
   */
  public OfflinePushStatus getOfflinePush(String topic) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(topic)) {
        return topicToPushMap.get(topic);
      } else {
        throw new VeniceException("Can not find offline push status for topic:" + topic);
      }
    }
  }

  /**
   * Get the status for the given offline push E.g. STARTED, COMPLETED.
   */
  public ExecutionStatus getOfflinePushStatus(String topic) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(topic)) {
        return topicToPushMap.get(topic).getCurrentStatus();
      } else {
        return ExecutionStatus.NOT_CREATED;
      }
    }
  }

  /**
   * Get the progress of the given offline push.
   * @return a map which's key is replica id and value is the kafka offset that replica already consumed.
   */
  public Map<String, Long> getOfflinePushProgress(String topic) {
    synchronized (lock) {
      if (!topicToPushMap.containsKey(topic)) {
        return Collections.emptyMap();
      } else {
        // As the monitor does NOT delete the replica of disconnected storage node, once a storage node failed, its
        // replicas statuses still be counted in the unfiltered progress map.
        Map<String, Long> progressMap = topicToPushMap.get(topic).getProgress();
        Set<String> liveInstances = routingDataRepository.getLiveInstancesMap().keySet();
        Iterator<String> iterator = progressMap.keySet().iterator();
        // Filter the progress map to remove replicas of disconnected storage node.
        while (iterator.hasNext()) {
          String replicaId = iterator.next();
          String instanceId = ReplicaStatus.getInstanceIdFromReplicaId(replicaId);
          if (!liveInstances.contains(instanceId)) {
            iterator.remove();
          }
        }
        return progressMap;
      }
    }
  }

  /**
   * Predict offline push status based on the given partition assignment.
   *
   * @return true the offline push would fail because the given partition assignment could not keep running. false
   * offline push would not fail.
   */
  public boolean wouldJobFail(String topic, PartitionAssignment partitionAssignment) {
    synchronized (lock) {
      if (!topicToPushMap.containsKey(topic)) {
        //the offline push has been terminated and archived.
        return false;
      } else {
        OfflinePushStatus offlinePush = topicToPushMap.get(topic);
        ExecutionStatus status = checkPushStatus(offlinePush, partitionAssignment);
        return status.equals(ExecutionStatus.ERROR);
      }
    }
  }

  /**
   * Wait helix assigning enough nodes to the given resource. It's not mandatory in our new push monitor, but in order
   * to keep the logic as similar as before, we start from waiting here and could remove this logic later.
   */
  public void waitUntilNodesAreAssignedForResource(String topic, long offlinePushWaitTimeInMilliseconds)
      throws InterruptedException {
    synchronized (lock) {
      final OfflinePushStatus pushStatus = getOfflinePush(topic);
      long startTime = System.currentTimeMillis();
      long nextWaitTime = offlinePushWaitTimeInMilliseconds;
      PushStatusDecider decider = PushStatusDecider.getDecider(pushStatus.getStrategy());
      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      while(!decider.hasEnoughNodesToStartPush(pushStatus, resourceAssignment)){
        if(System.currentTimeMillis() - startTime >= offlinePushWaitTimeInMilliseconds){
          // Time out, after waiting offlinePushWaitTimeInMilliseconds, there are not enough nodes assigned.
          throw new VeniceException(
              "After waiting: " + offlinePushWaitTimeInMilliseconds + ", resource still could not get enough nodes.");
        }
        // How long we spent on waiting and calculating totally.
        long spentTime = System.currentTimeMillis() - startTime;
        // The rest of time we could spent on waiting.
        nextWaitTime = offlinePushWaitTimeInMilliseconds - spentTime;
        logger.info("Resource does not have enough nodes, start waiting: "+nextWaitTime+"ms");
        lock.wait(nextWaitTime);
        resourceAssignment = routingDataRepository.getResourceAssignment();
      }
      // TODO add a metric to track waiting time.
      logger.info(
          "After waiting: " + (System.currentTimeMillis() - startTime) + "ms, resource allocation is completed.");
    }
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    synchronized (lock) {
      // TODO more fine-grained concurrency control here, might lock on push level instead of lock the whole map.
      OfflinePushStatus offlinePushStatus = topicToPushMap.get(topic);
      if (offlinePushStatus == null) {
        logger.error("Can not find Offline push for topic:" + topic + ", ignore the partition status change notification.");
        return;
      } else {
        // On controller side, partition status is read only. It could only be updated by storage node.
        offlinePushStatus.setPartitionStatus(partitionStatus);
        checkHybridPushStatus(offlinePushStatus);
      }
    }
  }

  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    synchronized (lock) {
      logger.info("Received the routing data changed notification for topic:" + partitionAssignment.getTopic());
      String kafkaTopic = partitionAssignment.getTopic();
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)
          || pushStatus.getCurrentStatus().equals(ExecutionStatus.END_OF_PUSH_RECEIVED)) {
        ExecutionStatus status = checkPushStatus(pushStatus, partitionAssignment);
        if (!status.equals(pushStatus.getCurrentStatus())) {
          logger.info("Offline push status will be changed to " + status.toString() + " for topic: " + kafkaTopic + " from status: " + pushStatus.getCurrentStatus());
          terminateOfflinePush(pushStatus, status);
        }
        // Notify the thread waiting on the waitUntilNodesAreAssignedForResource method.
        lock.notifyAll();
      } else {
        logger.info("Can not find a running offline push for topic:" + partitionAssignment.getTopic() + ", ignore the routing data changed notification.");
      }
    }
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    synchronized (lock) {
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
        logger.info("Resource for Topic:" + kafkaTopic + " is deleted, stopping the running push.");
        handleErrorPush(pushStatus);
      }
    }
  }

  private void checkHybridPushStatus(OfflinePushStatus offlinePushStatus) {
    // As the outer method already locked on this instance, so this method is thread-safe.
    String storeName = Version.parseStoreFromKafkaTopicName(offlinePushStatus.getKafkaTopic());
    Store store = metadataRepository.getStore(storeName);
    if (null == store) {
      logger.info("Got a null store from metadataRepository for store name: '" + storeName +
          "'. Will attempt a refresh().");
      // TODO refresh is a very expensive operation, because it will read all stores' metadata from ZK,
      // TODO Do we really need to do this here?
      metadataRepository.refresh();

      store = metadataRepository.getStore(storeName);
      if (null == store) {
        throw new IllegalStateException("checkHybridPushStatus could not find a store named '" + storeName +
            "' in the metadataRepository, even after refresh()!");
      } else {
        logger.info("metadataRepository.refresh() allowed us to retrieve store: '" + storeName + "'!");
      }
    }
    if (store.isHybrid()) {
      if (offlinePushStatus.isReadyToStartBufferReplay()) {
        Optional<TopicReplicator> topicReplicatorOptional = getTopicReplicator();
        if (topicReplicatorOptional.isPresent()) {
          try {
            topicReplicatorOptional.get().startBufferReplay(
                Version.composeRealTimeTopic(storeName),
                offlinePushStatus.getKafkaTopic(),
                store);
            updatePushStatus(offlinePushStatus, ExecutionStatus.END_OF_PUSH_RECEIVED);
            logger.info("Successfully kicked off buffer replay for offlinePushStatus: " + offlinePushStatus.toString());
          } catch (Exception e) {
            // TODO: Figure out a better error handling...
            logger.error("Failed to kick off the buffer replay for offlinePushStatus: " + offlinePushStatus.toString(), e);
          }
        } else {
          logger.error("The TopicReplicator was not properly initialized!");
        }
      }
    }
  }

  private ExecutionStatus checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(pushStatus.getStrategy());
    return statusDecider.checkPushStatus(pushStatus, partitionAssignment);
  }

  private void terminateOfflinePush(OfflinePushStatus pushStatus, ExecutionStatus status) {
    if (status.equals(ExecutionStatus.COMPLETED)) {
      handleCompletedPush(pushStatus);
    } else if (status.equals(ExecutionStatus.ERROR)) {
      handleErrorPush(pushStatus);
    }
  }

  private void handleCompletedPush(OfflinePushStatus pushStatus) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " old status: "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.COMPLETED);
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    updatePushStatus(pushStatus, ExecutionStatus.COMPLETED);
    String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(pushStatus.getKafkaTopic());
    updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ONLINE);
    storeCleaner.retireOldStoreVersions(clusterName, storeName);
    logger.info("Offline push for topic: " + pushStatus.getKafkaTopic() + " is completed.");
  }

  private void handleErrorPush(OfflinePushStatus pushStatus) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " is now "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.ERROR);
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    updatePushStatus(pushStatus, ExecutionStatus.ERROR);
    String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(pushStatus.getKafkaTopic());
    updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ERROR);
    storeCleaner.deleteOneStoreVersion(clusterName, storeName, versionNumber);
    logger.info("Offline push for topic: " + pushStatus.getKafkaTopic() + " fails.");
  }

  private void updateStoreVersionStatus(String storeName, int versionNumber, VersionStatus status) {
    VersionStatus newStatus = status;
    try {
      metadataRepository.lock();
      Store store = metadataRepository.getStore(storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }

      if (!store.isEnableWrites() && status.equals(VersionStatus.ONLINE)) {
        newStatus = VersionStatus.PUSHED;
      }

      store.updateVersionStatus(versionNumber, newStatus);
      logger.info(
          "Updated store: " + store.getName() + " version: " + versionNumber + " to status: " + newStatus.toString());
      if (newStatus.equals(VersionStatus.ONLINE)) {
        if (versionNumber > store.getCurrentVersion()) {
          store.setCurrentVersion(versionNumber);
        } else {
          logger.info("Current version for store " + store.getName() + ": " + store.getCurrentVersion()
              + " is newer than the given version: " + versionNumber + ".  The current version will not be changed.");
        }
      }
      metadataRepository.updateStore(store);
    } finally {
      metadataRepository.unLock();
    }
  }

  private void updatePushStatus(OfflinePushStatus pushStatus, ExecutionStatus status){
    OfflinePushStatus clonedPushStatus = pushStatus.clonePushStatus();
    clonedPushStatus.updateStatus(status);
    // Update remote storage
    accessor.updateOfflinePushStatus(clonedPushStatus);
    // Update local copy
    topicToPushMap.put(pushStatus.getKafkaTopic(), clonedPushStatus);
  }

  public OfflinePushAccessor getAccessor() {
    return accessor;
  }

  public void setTopicReplicator(Optional<TopicReplicator> topicReplicator) {
    this.topicReplicator = topicReplicator;
  }

  public Optional<TopicReplicator> getTopicReplicator() {
    return topicReplicator;
  }
}
