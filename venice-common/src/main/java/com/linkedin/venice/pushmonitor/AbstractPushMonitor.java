package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
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
import com.linkedin.venice.utils.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


/**
 * PushMonitor is the high level abstract that manage push status {@link OfflinePushStatus}.
 * {@link AbstractPushMonitor} watches changes of {@link PartitionStatus} and Helix external
 * view (a.k.a {@link RoutingDataRepository}). Classes extend from it should implement logic that
 * update {@link OfflinePushStatus} accordingly.
 *
 * At present, push status has 1 initial state {@link ExecutionStatus#STARTED} and 2 end states
 * {@link ExecutionStatus#COMPLETED} and {@link ExecutionStatus#ERROR}.
 * State mutation is unidirectional and once it reaches to either end state, we stop mutating it.
 * Check {@link OfflinePushStatus} for more details.
 */

public abstract class AbstractPushMonitor
    implements PushMonitor, OfflinePushAccessor.PartitionStatusListener, RoutingDataRepository.RoutingDataChangedListener {
  protected final Logger logger = Logger.getLogger(getClass().getSimpleName());
  public static final int MAX_PUSH_TO_KEEP = 5;
  private final OfflinePushAccessor offlinePushAccessor;
  private final String clusterName;
  private final ReadWriteStoreRepository metadataRepository;
  private final RoutingDataRepository routingDataRepository;
  private final StoreCleaner storeCleaner;

  private final AggPushHealthStats aggPushHealthStats;
  private final boolean skipBufferReplayForHybrid;

  private final Object lock;

  private Map<String, OfflinePushStatus> topicToPushMap = new ConcurrentHashMap<>();

  private Optional<TopicReplicator> topicReplicator;

  public AbstractPushMonitor(String clusterName, OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository, RoutingDataRepository routingDataRepository,
      AggPushHealthStats aggPushHealthStats, boolean skipBufferReplayForHybrid, Optional<TopicReplicator> topicReplicator) {
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.storeCleaner = storeCleaner;
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    this.aggPushHealthStats = aggPushHealthStats;
    this.skipBufferReplayForHybrid = skipBufferReplayForHybrid;
    this.topicReplicator = topicReplicator;

    // This is the VeniceHelixAdmin.  Any locking should be done on this object.  If we just use
    // the synchronized keyword to lock on the OfflinePushMonitor itself, then we have a deadlock
    // condition for any use of the storeCleaner.
    this.lock = storeCleaner;
  }

  @Override
  public void loadAllPushes() {
    synchronized (lock) {
      List<OfflinePushStatus> offlinePushStatuses = offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses();
      loadAllPushes(offlinePushStatuses);
    }
  }

  public void loadAllPushes(List<OfflinePushStatus> offlinePushStatusList) {
    synchronized (lock) {
      logger.info("Start loading pushes for cluster: " + clusterName);
      for (OfflinePushStatus offlinePushStatus : offlinePushStatusList) {
        getTopicToPushMap().put(offlinePushStatus.getKafkaTopic(), offlinePushStatus);
        getOfflinePushAccessor().subscribePartitionStatusChange(offlinePushStatus, this);

        // Check the status for running pushes. In case controller missed some notification during the failover, we
        // need to update it based on current routing data.
        if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
          String topic = offlinePushStatus.getKafkaTopic();
          if (routingDataRepository.containsKafkaTopic(topic)) {
            routingDataRepository.subscribeRoutingDataChange(topic, this);
            Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(offlinePushStatus, routingDataRepository.getPartitionAssignments(topic));
            if (status.getFirst().isTerminal()) {
              logger.info("Found a offline pushes could be terminated: " + offlinePushStatus.getKafkaTopic() + " status: " + status.getFirst());
              handleOfflinePushUpdate(offlinePushStatus, status.getFirst(), status.getSecond());
            }
          } else {
            // In any case, we found the offline push status is STARTED, but the related version could not be found.
            // We should collect this legacy offline push status.
            logger.info("Found a legacy offline pushes: " + offlinePushStatus.getKafkaTopic());
            try {
              getTopicToPushMap().remove(topic);
              getOfflinePushAccessor().deleteOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
            } catch (Exception e) {
              logger.warn("Could not delete legacy push status: " + offlinePushStatus.getKafkaTopic(), e);
            }
          }
        }
      }

      //scan the map to see if there are any old error push statues that can be retired
      Map<String, List<Integer>> storeToVersionNumsMap = new HashMap<>();
      topicToPushMap.keySet().forEach(topic ->
          storeToVersionNumsMap.computeIfAbsent(Version.parseStoreFromKafkaTopicName(topic),
              storeName -> new ArrayList<>()).add(Version.parseVersionFromKafkaTopicName(topic))
      );

      storeToVersionNumsMap.forEach(this::retireOldErrorPushes);
    }
  }

  @Override
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
      routingDataRepository.subscribeRoutingDataChange(kafkaTopic, this);
      logger.info("Start monitoring push on topic:" + kafkaTopic);
    }
  }

  @Override
  public void stopMonitorOfflinePush(String kafkaTopic) {
    logger.info("Stopping monitoring push on topic:" + kafkaTopic);
    synchronized (lock) {
      if (!topicToPushMap.containsKey(kafkaTopic)) {
        logger.warn("Push status does not exist for topic:" + kafkaTopic + " in cluster:" + clusterName);
        return;
      }
      OfflinePushStatus pushStatus = topicToPushMap.get(kafkaTopic);
      offlinePushAccessor.unsubscribePartitionsStatusChange(pushStatus, this);
      routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
      if (pushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        String storeName = Version.parseStoreFromKafkaTopicName(pushStatus.getKafkaTopic());
        retireOldErrorPushes(storeName);
      } else {
        cleanupPushStatus(pushStatus);
      }
      logger.info("Stopped monitoring push on topic:" + kafkaTopic);
    }
  }

  @Override
  public void cleanupStoreStatus(String storeName) {
    synchronized (lock) {
      List<String> topicList = topicToPushMap.keySet().stream()
          .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
          .collect(Collectors.toList());

      topicList.forEach(topic -> cleanupPushStatus(topicToPushMap.get(topic)));
    }
  }

  @Override
  public OfflinePushStatus getOfflinePush(String topic) {
    synchronized (lock) {
      if (topicToPushMap.containsKey(topic)) {
        return topicToPushMap.get(topic);
      } else {
        throw new VeniceException("Can not find offline push status for topic:" + topic);
      }
    }
  }

  public ExecutionStatus getPushStatus(String topic) {
    return getPushStatusAndDetails(topic, Optional.empty()).getFirst();
  }

  public ExecutionStatus getPushStatus(String topic, Optional<String> incrementalPushVersion) {
    return getPushStatusAndDetails(topic, incrementalPushVersion).getFirst();
  }

  @Override
  public Pair<ExecutionStatus, Optional<String>> getPushStatusAndDetails(String topic, Optional<String> incrementalPushVersion) {
    OfflinePushStatus pushStatus = this.topicToPushMap.get(topic);
    if (pushStatus == null) {
      return new Pair<>(ExecutionStatus.NOT_CREATED, Optional.of("Offline job hasn't been created yet."));
    }
    if (incrementalPushVersion.isPresent()) {
      return new Pair<>(pushStatus.checkIncrementalPushStatus(incrementalPushVersion.get()), Optional.empty());
    }
    return new Pair<>(pushStatus.getCurrentStatus(), pushStatus.getOptionalStatusDetails());
  }

  @Override
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

  @Override
  public Map<String, Long> getOfflinePushProgress(String topic) {
    OfflinePushStatus pushStatus = this.topicToPushMap.get(topic);
    if (pushStatus == null) {
      return Collections.emptyMap();
    }
    Map<String, Long> progress = new HashMap<>(pushStatus.getProgress());
    Set<String> liveInstances = this.routingDataRepository.getLiveInstancesMap().keySet();
    progress.keySet().removeIf(replicaId -> !liveInstances.contains(ReplicaStatus.getInstanceIdFromReplicaId(replicaId)));
    return progress;
  }

  @Override
  public void markOfflinePushAsError(String topic, String statusDetails) {
    synchronized (lock) {
      OfflinePushStatus status = topicToPushMap.get(topic);
      if (status == null) {
        logger.warn("Could not find offline push status for topic: " + topic
            + ". Ignore the request of marking status as ERROR.");
        return;
      }

      handleOfflinePushUpdate(status, ExecutionStatus.ERROR, Optional.of(statusDetails));
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
   * We'd like to keep at most {@link #MAX_PUSH_TO_KEEP} push status for debugging purpose.
   * If it's a successful push, it will be cleaned up when the version is retired. If it's
   * error push, we'll leave it until the store reaches the push status limit.
   */
  protected void retireOldErrorPushes(String storeName, List<Integer> versionNums) {
    List<OfflinePushStatus> errorPushStatusList = versionNums.stream().sorted()
        .map(version -> topicToPushMap.get(Version.composeKafkaTopic(storeName, version)))
        .filter(offlinePushStatus -> offlinePushStatus.getCurrentStatus().equals(ExecutionStatus.ERROR))
        .collect(Collectors.toList());

    for (OfflinePushStatus errorPushStatus : errorPushStatusList) {
      if (versionNums.size() <= MAX_PUSH_TO_KEEP) {
        break;
      }

      int errorVersion = Version.parseVersionFromKafkaTopicName(errorPushStatus.getKafkaTopic());
      // Make sure we do boxing; List.remove(primitive int) treats the primitive int as index
      versionNums.remove(Integer.valueOf(errorVersion));

      cleanupPushStatus(errorPushStatus);
    }
  }

  public boolean wouldJobFail(String topic, PartitionAssignment partitionAssignmentAfterRemoving) {
    synchronized (lock) {
      if (!topicToPushMap.containsKey(topic)) {
        //the offline push has been terminated and archived.
        return false;
      } else {
        OfflinePushStatus offlinePush = topicToPushMap.get(topic);
        Pair<ExecutionStatus, Optional<String>> status = PushStatusDecider.getDecider(offlinePush.getStrategy())
            .checkPushStatusAndDetails(offlinePush, partitionAssignmentAfterRemoving);
        return status.getFirst().equals(ExecutionStatus.ERROR);
      }
    }
  }

  protected abstract Pair<ExecutionStatus, Optional<String>> checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment);

  public abstract List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId);

  public void refreshAndUpdatePushStatus(String kafkaTopic, ExecutionStatus newStatus, Optional<String> newStatusDetails) {
    final OfflinePushStatus refreshedPushStatus = getOfflinePush(kafkaTopic);
    if (refreshedPushStatus.validatePushStatusTransition(newStatus)) {
      updatePushStatus(refreshedPushStatus, newStatus, newStatusDetails);
    } else {
      logger.info("refreshedPushStatus does not allow transitioning to " + newStatus + ", because it is currently in: "
          + refreshedPushStatus.getCurrentStatus() + " status. Will skip updating the status.");
    }
  }

  /**
   * Direct calls to updatePushStatus should be made carefully. e.g. calling with {@link ExecutionStatus}.ERROR or
   * other terminal status update should be made through handleOfflinePushUpdate. That method will then invoke
   * handleErrorPush and perform relevant operations to handle the ERROR status update properly.
   */
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

  protected RoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    synchronized (lock) {
      // TODO more fine-grained concurrency control here, might lock on push level instead of lock the whole map.
      OfflinePushStatus pushStatus = this.topicToPushMap.get(topic);
      if (pushStatus == null) {
        logger.error("Can not find Offline push for topic:" + topic + ", ignore the partition status change notification.");
        return;
      }

      // On controller side, partition status is read only. It could be only updated by storage node.
      pushStatus = pushStatus.clonePushStatus();
      pushStatus.setPartitionStatus(partitionStatus);
      this.topicToPushMap.put(pushStatus.getKafkaTopic(), pushStatus);

      onPartitionStatusChange(pushStatus);
    }
  }

  protected void onPartitionStatusChange(OfflinePushStatus offlinePushStatus) {
    checkWhetherToStartBufferReplayForHybrid(offlinePushStatus);
  }

  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    synchronized (getLock()) {
      logger.info("Received the routing data changed notification for topic:" + partitionAssignment.getTopic());
      String kafkaTopic = partitionAssignment.getTopic();
      OfflinePushStatus pushStatus = getTopicToPushMap().get(kafkaTopic);

      if (pushStatus != null) {
        ExecutionStatus previousStatus = pushStatus.getCurrentStatus();
        if (previousStatus.equals(ExecutionStatus.COMPLETED) || previousStatus.equals(ExecutionStatus.ERROR)) {
          logger.warn("Skip updating push status: " + kafkaTopic + " since it is already in: " + previousStatus);
          return;
        }

        Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(pushStatus, partitionAssignment);
        if (!status.getFirst().equals(pushStatus.getCurrentStatus()) && status.getFirst().isTerminal()) {
          logger.info("Offline push status will be changed to " + status.toString() + " for topic: " + kafkaTopic + " from status: " + pushStatus.getCurrentStatus());
          handleOfflinePushUpdate(pushStatus, status.getFirst(), status.getSecond());
        }
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
    synchronized (getLock()) {
      OfflinePushStatus pushStatus = getTopicToPushMap().get(kafkaTopic);
      if (pushStatus != null && pushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
        String statusDetails = "Helix resource for Topic:" + kafkaTopic + " is deleted, stopping the running push.";
        logger.info(statusDetails);
        handleErrorPush(pushStatus, statusDetails);
      }
    }
  }

  protected void checkWhetherToStartBufferReplayForHybrid(OfflinePushStatus offlinePushStatus) {
    // As the outer method already locked on this instance, so this method is thread-safe.
    String storeName = Version.parseStoreFromKafkaTopicName(offlinePushStatus.getKafkaTopic());
    Store store = getReadWriteStoreRepository().getStore(storeName);
    if (null == store) {
      logger.info("Got a null store from metadataRepository for store name: '" + storeName +
          "'. Will attempt a refresh().");
      // TODO refresh is a very expensive operation, because it will read all stores' metadata from ZK,
      // TODO Do we really need to do this here?
      getReadWriteStoreRepository().refresh();

      store = getReadWriteStoreRepository().getStore(storeName);
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
        if (topicReplicatorOptional.isPresent() || skipBufferReplayForHybrid) {
          try {
            String newStatusDetails;
            if (skipBufferReplayForHybrid) {
              newStatusDetails = "skipped buffer replay";
              logger.info("Skip buffer replay for hybrid store version: " + offlinePushStatus.getKafkaTopic());
            } else {
              topicReplicatorOptional.get()
                  .prepareAndStartReplication(Version.composeRealTimeTopic(storeName), offlinePushStatus.getKafkaTopic(), store);
              newStatusDetails = "kicked off buffer replay";
            }
            updatePushStatus(offlinePushStatus, ExecutionStatus.END_OF_PUSH_RECEIVED, Optional.of(newStatusDetails));
            logger.info("Successfully " + newStatusDetails + " for offlinePushStatus: " + offlinePushStatus.toString());
          } catch (Exception e) {
            // TODO: Figure out a better error handling...
            String newStatusDetails = "Failed to kick off the buffer replay";
            handleOfflinePushUpdate(offlinePushStatus, ExecutionStatus.ERROR, Optional.of(newStatusDetails));
            logger.error(newStatusDetails + " for offlinePushStatus: " + offlinePushStatus.toString(), e);
          }
        } else {
          String newStatusDetails = "The TopicReplicator was not properly initialized!";
          handleOfflinePushUpdate(offlinePushStatus, ExecutionStatus.ERROR, Optional.of(newStatusDetails));
          logger.error(newStatusDetails);
        }
      } else {
        logger.info(offlinePushStatus.getKafkaTopic()+" is not ready to start buffer relay.");
      }
    }
  }

  /**
   * This method will unsubscribe external view changes and is intended to be called when the statues are terminable.
   */
  protected void handleOfflinePushUpdate(OfflinePushStatus pushStatus, ExecutionStatus status, Optional<String> statusDetails) {
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);

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

  protected void handleCompletedPush(OfflinePushStatus pushStatus) {
    logger.info("Updating offline push status, push for: " + pushStatus.getKafkaTopic() + " old status: "
        + pushStatus.getCurrentStatus() + ", new status: " + ExecutionStatus.COMPLETED);

    String topic = pushStatus.getKafkaTopic();
    updatePushStatus(pushStatus, ExecutionStatus.COMPLETED, Optional.empty());
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
    updateStoreVersionStatus(storeName, versionNumber, VersionStatus.ONLINE);
    aggPushHealthStats.recordSuccessfulPush(storeName, getDurationInSec(pushStatus));
    // If we met some error to retire the old version, we should not throw the exception out to fail this operation,
    // because it will be collected once a new push is completed for this store.
    try {
      storeCleaner.topicCleanupWhenPushComplete(clusterName, storeName, versionNumber);
    } catch (Exception e) {
      logger.warn("Couldn't perform topic cleanup when push job completes for topic: " + topic + " in cluster: " + clusterName);
    }
    try {
      storeCleaner.retireOldStoreVersions(clusterName, storeName, false);
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

  @Override
  public void recordPushPreparationDuration(String topic, long offlinePushWaitTimeInSecond) {
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    aggPushHealthStats.recordPushPrepartionDuration(storeName, offlinePushWaitTimeInSecond);
  }

  /**
   * For testing only; in order to override the topicReplicator with mocked Replicator.
   */
  public void setTopicReplicator(Optional<TopicReplicator> topicReplicator) {
    this.topicReplicator = topicReplicator;
  }

  public Optional<TopicReplicator> getTopicReplicator() {
    return topicReplicator;
  }
}
