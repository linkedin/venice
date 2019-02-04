package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * PushStatusMonitor the high level abstract that manage push status {@link OfflinePushStatus}.
 * {@link AbstractPushStatusMonitor} watches changes of {@link PartitionStatus}.
 * Classes extend from it should implement logic that update {@link OfflinePushStatus} accordingly.
 */

public abstract class AbstractPushStatusMonitor
    implements OfflinePushAccessor.PartitionStatusListener {
  private static final Logger logger = Logger.getLogger(AbstractPushStatusMonitor.class);
  public static final int MAX_ERROR_PUSH_TO_KEEP = 5;
  private final OfflinePushAccessor offlinePushAccessor;
  private final String clusterName;
  private final ReadWriteStoreRepository metadataRepository;
  private final StoreCleaner storeCleaner;

  private final AggPushHealthStats aggPushHealthStats;

  private final Object lock;

  private Map<String, OfflinePushStatus> topicToPushMap;

  private Optional<TopicReplicator> topicReplicator = Optional.empty();

  public AbstractPushStatusMonitor(String clusterName, OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository, AggPushHealthStats aggPushHealthStats) {
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.storeCleaner = storeCleaner;
    this.metadataRepository = metadataRepository;
    this.aggPushHealthStats = aggPushHealthStats;

    // This is the VeniceHelixAdmin.  Any locking should be done on this object.  If we just use
    // the synchronized keyword to lock on the OfflinePushMonitor itself, then we have a deadlock
    // condition for any use of the storeCleaner.
    this.lock = storeCleaner;

    this.topicToPushMap = new HashMap<>();
  }

  /**
   * This method should be implemented by its sub class and will be called in {@link #loadAllPushes()}.
   * synchronized is secured at higher level.
   */
  abstract void loadPush(OfflinePushStatus offlinePushStatus);

  public void loadAllPushes() {
    synchronized (lock) {
      logger.info("Start loading pushes for cluster: " + clusterName);
      List<OfflinePushStatus> offlinePushStatuses = offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses();
      for (OfflinePushStatus status : offlinePushStatuses) {
        loadPush(status);
      }

      //scan the map to see if there are any old error push statues that can be retired
      Map<String, List<Integer>> storeToVersionNumsMap = new HashMap<>();
      topicToPushMap.keySet().forEach(topic ->
          storeToVersionNumsMap.computeIfAbsent(Version.parseStoreFromKafkaTopicName(topic),
              storeName -> new ArrayList<>()).add(Version.parseVersionFromKafkaTopicName(topic))
      );

      storeToVersionNumsMap.entrySet().forEach(entry -> retireOldErrorPushes(entry.getKey(), entry.getValue()));
    }
  }

  public void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(kafkaTopic)) {
        ExecutionStatus existingStatus = getPushStatus(kafkaTopic);
        if (existingStatus.equals(ExecutionStatus.ERROR)) {
          logger.info("The previous push status for topic: " + kafkaTopic + " is 'ERROR',"
              + " and the new push will clean up the previous 'ERROR' push status");
          cleanupPushStatus(topicToPushMap.get(kafkaTopic));
        } else {
          throw new VeniceException("Push status has already been created for topic:" + kafkaTopic + " in cluster:" + clusterName);
        }
      }

      OfflinePushStatus pushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicaFactor, strategy);
      offlinePushAccessor.createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
      topicToPushMap.put(kafkaTopic, pushStatus);
      offlinePushAccessor.subscribePartitionStatusChange(pushStatus, this);
      logger.info("Start monitoring push on topic:" + kafkaTopic);
    }
  }

  public void stopMonitorOfflinePush(String kafkaTopic) {
    logger.info("Stopping monitoring push on topic:" + kafkaTopic);
    synchronized (lock) {
      if (!topicToPushMap.containsKey(kafkaTopic)) {
        logger.warn("Push status does not exist for topic:" + kafkaTopic + " in cluster:" + clusterName);
        return;
      }
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      offlinePushAccessor.unsubscribePartitionsStatusChange(pushStatus, this);
      if (pushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
        retireOldErrorPushes(storeName);
      } else {
        cleanupPushStatus(pushStatus);
      }
      logger.info("Stopped monitoring push on topic:" + kafkaTopic);
    }
  }

  /**
   * Get the push status for the given topic.
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
  public ExecutionStatus getPushStatus(String topic) {
    return getPushStatusAndDetails(topic, Optional.empty()).getFirst();
  }

  public ExecutionStatus getPushStatus(String topic, Optional<String> incrementalPushVersion) {
    return getPushStatusAndDetails(topic, incrementalPushVersion).getFirst();
  }

  public Pair<ExecutionStatus, Optional<String>> getPushStatusAndDetails(String topic, Optional<String> incrementalPushVersion) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(topic)) {
        OfflinePushStatus offlinePushStatus = topicToPushMap.get(topic);
        return incrementalPushVersion.isPresent() ?
            new Pair<>(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion.get()), Optional.empty()) :
            new Pair<>(offlinePushStatus.getCurrentStatus(), offlinePushStatus.getOptionalStatusDetails());
      } else {
        return new Pair<>(ExecutionStatus.NOT_CREATED, Optional.of("Kafka topic not detected yet."));
      }
    }
  }

  /**
   * Find all ongoing pushes then return the topics associated to those pushes.
   */
  public List<String> getTopicsOfOngoingOfflinePushes() {
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
   * Get the progress of the given offline push.
   * @return a map which's key is replica id and value is the kafka offset that replica already consumed.
   *
   * Currently, SN is not in charge of cleaning up PartitionStatus if it stops owning a replica.
   * This may cause misleading progress.
   * TODO: let SN clean up its state before releasing replicas
   */
  public Map<String, Long> getOfflinePushProgress(String topic) {
    synchronized (lock) {
      if (!topicToPushMap.containsKey(topic)) {
        return Collections.emptyMap();
      }
      return topicToPushMap.get(topic).getProgress();
    }
  }

  public void markOfflinePushAsError(String topic, String statusDetails){
    synchronized (lock) {
      OfflinePushStatus status = topicToPushMap.get(topic);
      if (status == null) {
        logger.warn("Could not find offline push status for topic: " + topic
            + ". Ignore the request of marking status as ERROR.");
        return;
      }

      updatePushStatus(status, ExecutionStatus.ERROR, Optional.of(statusDetails));
      aggPushHealthStats.recordFailedPush(Version.parseStoreFromKafkaTopicName(topic), getDurationInSec(status));
    }
  }

  /**
   * this is to clear legacy push statuses
   */
  protected void cleanupPushStatus(OfflinePushStatus offlinePushStatus) {
    synchronized (lock) {
      try {
        topicToPushMap.remove(offlinePushStatus.getKafkaTopic());
        offlinePushAccessor.deleteOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
      } catch (Exception e) {
        logger.warn("Could not delete legacy push status: " + offlinePushStatus.getKafkaTopic(), e);
      }
    }
  }

  protected void retireOldErrorPushes(String storeName) {
    List<Integer> versionNums = topicToPushMap.keySet().stream()
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .map(Version::parseVersionFromKafkaTopicName)
        .collect(Collectors.toList());

    retireOldErrorPushes(storeName, versionNums);
  }

  /**
   * Once offline push failed, we want to keep some latest offline push in ZK for debug.
   */
  protected void retireOldErrorPushes(String storeName, List<Integer> versionNums) {
    while (versionNums.size() > MAX_ERROR_PUSH_TO_KEEP) {
      int oldestVersionNumber = Collections.min(versionNums);
      versionNums.remove(Integer.valueOf(oldestVersionNumber));

      String topicName = Version.composeKafkaTopic(storeName, oldestVersionNumber);

      try {
        offlinePushAccessor.deleteOfflinePushStatusAndItsPartitionStatuses(topicToPushMap.get(topicName));
        topicToPushMap.remove(topicName);
      } catch (Exception e) {
        logger.warn("Could not retire push status: " + topicName, e);
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
   * Checking push status based on Helix external view (RoutingData)
   */
  protected Pair<ExecutionStatus, Optional<String>> checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(pushStatus.getStrategy());
    return statusDecider.checkPushStatusAndDetails(pushStatus, partitionAssignment);
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
  public void refreshAndUpdatePushStatus(String kafkaTopic, ExecutionStatus newStatus, Optional<String> newStatusDetails){
    final OfflinePushStatus refreshedPushStatus = getOfflinePush(kafkaTopic);
    if (refreshedPushStatus.validatePushStatusTransition(newStatus)) {
      updatePushStatus(refreshedPushStatus, newStatus, newStatusDetails);
    } else {
      logger.info("refreshedPushStatus does not allow transitioning to " + newStatus + ", because it is currently in: "
          + refreshedPushStatus.getCurrentStatus() + " status. Will skip updating the status.");
    }
  }

  protected void updatePushStatus(OfflinePushStatus pushStatus, ExecutionStatus newStatus, Optional<String> newStatusDetails){
    OfflinePushStatus clonedPushStatus = pushStatus.clonePushStatus();
    clonedPushStatus.updateStatus(newStatus, newStatusDetails);
    // Update remote storage
    offlinePushAccessor.updateOfflinePushStatus(clonedPushStatus);
    // Update local copy
    topicToPushMap.put(pushStatus.getKafkaTopic(), clonedPushStatus);
  }

  protected long getDurationInSec(OfflinePushStatus pushStatus) {
    long start = pushStatus.getStartTimeSec();
    return System.currentTimeMillis() / Time.MS_PER_SECOND - start;
  }

  protected Map<String, OfflinePushStatus> getTopicToPushMap() {
    return topicToPushMap;
  }

  protected OfflinePushAccessor getOfflinePushAccessor() {
    return offlinePushAccessor;
  }

  protected Object getLock() {
    return lock;
  }

  protected ReadWriteStoreRepository getReadWriteStoreRepository() {
    return metadataRepository;
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
        onPartitionStatusChange(offlinePushStatus, partitionStatus);
      }
    }
  }

  protected abstract void onPartitionStatusChange(OfflinePushStatus offlinePushStatus, ReadOnlyPartitionStatus partitionStatus);

  protected void handleCompletedPush(OfflinePushStatus pushStatus) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " old status: "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.COMPLETED);

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

  protected void handleErrorPush(OfflinePushStatus pushStatus, String statusDetails) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " is now "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.ERROR + ", statusDetails: " + statusDetails);
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

  public void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond) {
    aggPushHealthStats.recordPushPrepartionDuration(topic, offlinePushWaitTimeInSecond);
  }

  public void setTopicReplicator(Optional<TopicReplicator> topicReplicator) {
    this.topicReplicator = topicReplicator;
  }

  public Optional<TopicReplicator> getTopicReplicator() {
    return topicReplicator;
  }
}
