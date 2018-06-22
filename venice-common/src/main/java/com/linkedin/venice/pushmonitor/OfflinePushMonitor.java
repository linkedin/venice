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
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

/**
 * Monitor used to track all of offline push statuses. Monitor would watch the routing data(eg Helix external view) for
 * each offline push job. Once the routing data is changed,  monitor would get a notification push and change the
 * offline push job status accordingly.
 * <p>
 * For example, once a replica become ONLINE from BOOTSTRAP, monitor would check is the whole push completed and active
 * the related version if the answer is yes. Or in case of node failure, monitor would fail the push job in some cases.
 *
 * TODO:
 * The class now is listening to both PartitionStatus changes and RoutingDataRepository changes. This makes
 * the logic a bit of ambiguous. We may want to move RoutingDataRepository listener out of this class later.
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
  private final AggPushHealthStats aggPushHealthStats;

  private Map<String, OfflinePushStatus> topicToPushMap;
  private Optional<TopicReplicator> topicReplicator = Optional.empty();

  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor accessor, StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats) {
    this.clusterName = clusterName;
    this.routingDataRepository = routingDataRepository;
    this.accessor = accessor;
    this.topicToPushMap = new HashMap<>();
    this.storeCleaner = storeCleaner;
    this.metadataRepository = metadataRepository;
    this.aggPushHealthStats = aggPushHealthStats;

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
      logger.info("Start loading pushes for cluster: " + clusterName);
      List<OfflinePushStatus> offlinePushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();
      Map<String, OfflinePushStatus> newTopicToPushMap = new HashMap<>();
      for (OfflinePushStatus status : offlinePushes) {
        newTopicToPushMap.put(status.getKafkaTopic(), status);
        accessor.subscribePartitionStatusChange(status, this);
      }
      topicToPushMap = newTopicToPushMap;

      // Check the status for the running push. In case controller missed some notification during the failover, we
      // need to update it based on current routing data.
      List<OfflinePushStatus> legacyOfflinePushes = new ArrayList<>();
      topicToPushMap.values()
          .stream()
          .filter(offlinePush -> offlinePush.getCurrentStatus().equals(ExecutionStatus.STARTED))
          .forEach(offlinePush -> {
            if(routingDataRepository.containsKafkaTopic(offlinePush.getKafkaTopic())) {
              routingDataRepository.subscribeRoutingDataChange(offlinePush.getKafkaTopic(), this);
              Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(offlinePush, routingDataRepository.getPartitionAssignments(offlinePush.getKafkaTopic()));
              if (status.getFirst().isTerminal()) {
                logger.info(
                    "Found a offline pushes could be terminated: " + offlinePush.getKafkaTopic() + " status: " + status
                        .toString());
                terminateOfflinePush(offlinePush, status.getFirst(), status.getSecond());
              }
            } else {
              // In any case, we found the offline push status is STARTED, but the related version could not be found.
              // We should collect this legacy offline push status.
              logger.info("Found a legacy offline pushes: " + offlinePush.getKafkaTopic());
              legacyOfflinePushes.add(offlinePush);
            }
          });
      //Clear all legacy offline push status.
      for(OfflinePushStatus legacyOfflinePush : legacyOfflinePushes){
        try {
          topicToPushMap.remove(legacyOfflinePush.getKafkaTopic());
          accessor.deleteOfflinePushStatusAndItsPartitionStatuses(legacyOfflinePush);
        }catch(Exception e){
          logger.warn("Could not delete legacy push status: "+legacyOfflinePush.getKafkaTopic(), e);
        }
      }
      //Delete old error pushes.
      Set<String> storeNames =
          topicToPushMap.keySet().stream().map(Version::parseStoreFromKafkaTopicName).collect(Collectors.toSet());
      storeNames.forEach(this::cleanOlderErrorPushes);
      logger.info("Loaded offline pushes for cluster: " + clusterName);
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
    logger.info("Stopping monitoring push on topic:" + kafkaTopic);
    synchronized (lock) {
      if (!topicToPushMap.containsKey(kafkaTopic)) {
        logger.warn("Push status does not exist for topic:" + kafkaTopic + " in cluster:" + clusterName);
        return;
      }
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
      accessor.unsubscribePartitionsStatusChange(pushStatus, this);
      if (pushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
        cleanOlderErrorPushes(storeName);
      } else {
        accessor.deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus);
        topicToPushMap.remove(kafkaTopic);
      }
      logger.info("Stopped monitoring push on topic:" + kafkaTopic);
    }
  }

  /**
   * Once offline push failed, we want to keep some latest offline push in ZK for debug.
   */
  private void cleanOlderErrorPushes(String storeName) {
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
      accessor.deleteOfflinePushStatusAndItsPartitionStatuses(topicToPushMap.get(topicName));
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
   * Find all ongoing offline pushes then return the topics associated to those pushes.
   */
  public List<String> getTopicsOfOngoingOfflinePushes(){
    List<String> result = new ArrayList<>();
    synchronized (lock) {
      result.addAll(topicToPushMap.values()
          .stream()
          .filter(status -> status.getCurrentStatus().equals(ExecutionStatus.STARTED))
          .map(OfflinePushStatus::getKafkaTopic)
          .collect(Collectors.toList()));
    }
    return result;
  }

  /**
   * Get the status for the given offline push E.g. STARTED, COMPLETED.
   */
  public ExecutionStatus getOfflinePushStatus(String topic) {
    return getOfflinePushStatusAndDetails(topic).getFirst();
  }

  public Pair<ExecutionStatus, Optional<String>> getOfflinePushStatusAndDetails(String topic) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(topic)) {
        OfflinePushStatus offlinePushStatus = topicToPushMap.get(topic);
        return new Pair<>(offlinePushStatus.getCurrentStatus(), offlinePushStatus.getOptionalStatusDetails());
      } else {
        return new Pair<>(ExecutionStatus.NOT_CREATED, Optional.of("Kafka topic not detected yet."));
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
        Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(offlinePush, partitionAssignment);
        return status.getFirst().equals(ExecutionStatus.ERROR);
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
      if(pushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        throw new VeniceException("The push is already failed. Status: "+pushStatus.getCurrentStatus().toString()+". Stop waiting.");
      }
      long startTime = System.currentTimeMillis();
      long nextWaitTime = offlinePushWaitTimeInMilliseconds;
      PushStatusDecider decider = PushStatusDecider.getDecider(pushStatus.getStrategy());
      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      while (true) {
        Optional<String> reasonForNotBeingReady = decider.hasEnoughNodesToStartPush(pushStatus, resourceAssignment);
        if (!reasonForNotBeingReady.isPresent()) {
          break;
        } else {
          refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, reasonForNotBeingReady);
        }
        if(System.currentTimeMillis() - startTime >= offlinePushWaitTimeInMilliseconds){
          // Time out, after waiting offlinePushWaitTimeInMilliseconds, there are not enough nodes assigned.
          aggPushHealthStats.recordPushPrepartionDuration(Version.parseStoreFromKafkaTopicName(topic),
              offlinePushWaitTimeInMilliseconds / 1000);
          throw new VeniceException("After waiting: " + offlinePushWaitTimeInMilliseconds + ", resource '" + topic
              + "' still could not get enough nodes.");
        }
        // How long we spent on waiting and calculating totally.
        long spentTime = System.currentTimeMillis() - startTime;
        // The rest of time we could spent on waiting.
        nextWaitTime = offlinePushWaitTimeInMilliseconds - spentTime;
        logger.info("Resource '" + topic + "' does not have enough nodes, start waiting: "+nextWaitTime+"ms");
        lock.wait(nextWaitTime);
        resourceAssignment = routingDataRepository.getResourceAssignment();
      }

      refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, Optional.of("Helix assignment complete"));

      // TODO add a metric to track waiting time.
      long spentTime = System.currentTimeMillis() - startTime;
      logger.info(
          "After waiting: " + spentTime + "ms, resource allocation is completed for '" + topic + "'.");
      aggPushHealthStats.recordPushPrepartionDuration(Version.parseStoreFromKafkaTopicName(topic), spentTime / 1000);
    }
  }

  public void markOfflinePushAsError(String topic, String statusDetails){
    OfflinePushStatus status = topicToPushMap.get(topic);
    if(status == null){
      logger.warn("Could not found offline push status for topic: "+topic+". Ignore the request of marking status as ERROR.");
      return;
    }
    routingDataRepository.unSubscribeRoutingDataChange(topic, this);
    updatePushStatus(status, ExecutionStatus.ERROR, Optional.of(statusDetails));
    aggPushHealthStats.recordFailedPush(Version.parseStoreFromKafkaTopicName(topic), getDurationInSec(status));
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
      ExecutionStatus[] statusesThatRequireNotifyingTheLock = {
          ExecutionStatus.STARTED,
          ExecutionStatus.END_OF_PUSH_RECEIVED};
      if (pushStatus != null && Utils.verifyTransition(pushStatus.getCurrentStatus(), statusesThatRequireNotifyingTheLock)) {
        Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(pushStatus, partitionAssignment);
        if (!status.equals(pushStatus.getCurrentStatus())) {
          logger.info("Offline push status will be changed to " + status.toString() + " for topic: " + kafkaTopic + " from status: " + pushStatus.getCurrentStatus());
          terminateOfflinePush(pushStatus, status.getFirst(), status.getSecond());
        }
        // Notify the thread waiting on the waitUntilNodesAreAssignedForResource method.
        lock.notifyAll();
      } else {
        logger.info("Can not find a running offline push for topic:" + partitionAssignment.getTopic() + ", ignore the routing data changed notification. OfflinePushStatus: " + pushStatus);
      }
    }
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    // Beside the external view, we also care about the ideal state here. If the resource was deleted from the externalview by mistake,
    // as long as the resource exists in the ideal state, helix will recover it automatically, thus push will keep working.
    if(routingDataRepository.doseResourcesExistInIdealState(kafkaTopic)){
      logger.warn("Resource is remaining in the ideal state. Ignore the deletion in the external view.");
      return;
    }
    synchronized (lock) {
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
        String statusDetails = "Helix resource for Topic:" + kafkaTopic + " is deleted, stopping the running push.";
        logger.info(statusDetails);
        handleErrorPush(pushStatus, statusDetails);
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
        logger.info(offlinePushStatus.getKafkaTopic()+" is ready to start buffer replay.");
        Optional<TopicReplicator> topicReplicatorOptional = getTopicReplicator();
        if (topicReplicatorOptional.isPresent()) {
          try {
            topicReplicatorOptional.get().startBufferReplay(
                Version.composeRealTimeTopic(storeName),
                offlinePushStatus.getKafkaTopic(),
                store);
            String newStatusDetails = "kicked off buffer replay";
            updatePushStatus(offlinePushStatus, ExecutionStatus.END_OF_PUSH_RECEIVED, Optional.of(newStatusDetails));
            logger.info("Successfully " + newStatusDetails + " for offlinePushStatus: " + offlinePushStatus.toString());
          } catch (Exception e) {
            // TODO: Figure out a better error handling...
            String newStatusDetails = "Failed to kick off the buffer replay";
            updatePushStatus(offlinePushStatus, ExecutionStatus.ERROR, Optional.of(newStatusDetails));
            logger.error(newStatusDetails + " for offlinePushStatus: " + offlinePushStatus.toString(), e);
          }
        } else {
          String newStatusDetails = "The TopicReplicator was not properly initialized!";
          updatePushStatus(offlinePushStatus, ExecutionStatus.ERROR, Optional.of(newStatusDetails));
          logger.error(newStatusDetails);
        }
      } else {
        logger.info(offlinePushStatus.getKafkaTopic()+" is not ready to start buffer relay.");
      }
    }
  }

  private Pair<ExecutionStatus, Optional<String>> checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(pushStatus.getStrategy());
    return statusDecider.checkPushStatusAndDetails(pushStatus, partitionAssignment);
  }

  private void terminateOfflinePush(OfflinePushStatus pushStatus, ExecutionStatus status, Optional<String> statusDetails) {
    if (status.equals(ExecutionStatus.COMPLETED)) {
      handleCompletedPush(pushStatus);
    } else if (status.equals(ExecutionStatus.ERROR)) {
      String statusDetailsString = "STATUS DETAILS ABSENT.";
      if (statusDetails.isPresent()) {
        statusDetailsString = statusDetails.get();
      } else {
        logger.error("Status details should be provided in order to terminateOfflinePush, but they are missing.",
            new VeniceException("Unthrown exception, for stacktrace logging purposes."));
      }
      handleErrorPush(pushStatus, statusDetailsString);
    }
  }

  private long getDurationInSec(OfflinePushStatus pushStatus) {
    long start = pushStatus.getStartTimeSec();
    return System.currentTimeMillis() / 1000 - start;
  }

  private void handleCompletedPush(OfflinePushStatus pushStatus) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " old status: "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.COMPLETED);
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    updatePushStatus(pushStatus, ExecutionStatus.COMPLETED, Optional.empty());
    String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(pushStatus.getKafkaTopic());
    updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ONLINE);
    aggPushHealthStats.recordSuccessfulPush(storeName, getDurationInSec(pushStatus));
    // If we met some error to retire the old version, we should not throw the exception out to fail this operation,
    // because it will be collected once a new push is completed for this store.
    try {
      storeCleaner.retireOldStoreVersions(clusterName, storeName);
    } catch (Exception e) {
      logger.warn("Could not retire the old versions for store: " + storeName + " in cluster: " + clusterName, e);
    }
    logger.info("Offline push for topic: " + pushStatus.getKafkaTopic() + " is completed.");
  }

  private void handleErrorPush(OfflinePushStatus pushStatus, String statusDetails) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " is now "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.ERROR + ", statusDetails: " + statusDetails);
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    updatePushStatus(pushStatus, ExecutionStatus.ERROR, Optional.of(statusDetails));
    String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
    int versionNumber = Version.parseVersionFromKafkaTopicName(pushStatus.getKafkaTopic());
    try {
      updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ERROR);
      aggPushHealthStats.recordFailedPush(storeName, getDurationInSec(pushStatus));
      // If we met some error to delete error version, we should not throw the exception out to fail this operation,
      // because it will be collected once a new push is completed for this store.
      storeCleaner.deleteOneStoreVersion(clusterName, storeName, versionNumber);
    } catch (Exception e) {
      logger.warn("Could not delete error version: " + versionNumber + " for store: " + storeName + " in cluster: "
          + clusterName, e);
    }
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

  /**
   * Here, we refresh the push status, in order to avoid a race condition where a small job could
   * already be completed. Previously, we would clobber the COMPLETED status with STARTED, which
   * would stall the job forever.
   *
   * Now, since we get the refreshed status, we can validate whether a transition to {@param newStatus}
   * is valid, before making the change. If if wouldn't be valid (because the job already completed
   * or already failed, for example), then we leave the status as is, rather than adding in the
   * new details.
   */
  private void refreshAndUpdatePushStatus(String kafkaTopic, ExecutionStatus newStatus, Optional<String> newStatusDetails){
    final OfflinePushStatus refreshedPushStatus = getOfflinePush(kafkaTopic);
    if (refreshedPushStatus.validatePushStatusTransition(newStatus)) {
      updatePushStatus(refreshedPushStatus, newStatus, newStatusDetails);
    } else {
      logger.info("refreshedPushStatus does not allow transitioning to " + newStatus + ", because it is currently in: "
          + refreshedPushStatus.getCurrentStatus() + " status. Will skip updating the status.");
    }
  }

  private void updatePushStatus(OfflinePushStatus pushStatus, ExecutionStatus newStatus, Optional<String> newStatusDetails){
    OfflinePushStatus clonedPushStatus = pushStatus.clonePushStatus();
    clonedPushStatus.updateStatus(newStatus, newStatusDetails);
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
