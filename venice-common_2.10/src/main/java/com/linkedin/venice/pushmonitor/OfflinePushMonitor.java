package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * {@link OfflinePushStatus} watches the changes of {@link RoutingDataRepository}.
 * For batch push, push status is updated according to Helix OfflineOnline state
 * For streaming push, push status is updated according to partition status
 *
 * TODO: rename this class as we have move most of the code to {@link AbstractPushStatusMonitor}
 */

public class OfflinePushMonitor extends AbstractPushStatusMonitor implements RoutingDataRepository.RoutingDataChangedListener {
  private static final Logger logger = Logger.getLogger(OfflinePushMonitor.class);

  private final RoutingDataRepository routingDataRepository;

  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor offlinePushAccessor, StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats) {
    super(clusterName, offlinePushAccessor, storeCleaner, metadataRepository, aggPushHealthStats);

    this.routingDataRepository = routingDataRepository;
  }

  @Override
  public void loadPush(OfflinePushStatus offlinePushStatus) {
    getTopicToPushMap().put(offlinePushStatus.getKafkaTopic(), offlinePushStatus);
    getOfflinePushAccessor().subscribePartitionStatusChange(offlinePushStatus, this);

    // Check the status for running pushes. In case controller missed some notification during the failover, we
    // need to update it based on current routing data.
    if (offlinePushStatus.getCurrentStatus().equals(ExecutionStatus.STARTED)) {
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

  @Override
  public void startMonitorOfflinePush(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy) {
    synchronized (getLock()) {
      super.startMonitorOfflinePush(kafkaTopic, numberOfPartition, replicaFactor, strategy);
      routingDataRepository.subscribeRoutingDataChange(kafkaTopic, this);
    }
  }

  @Override
  public void stopMonitorOfflinePush(String kafkaTopic) {
    synchronized (getLock()) {
      super.stopMonitorOfflinePush(kafkaTopic);
      routingDataRepository.unSubscribeRoutingDataChange(kafkaTopic, this);
    }
  }

  @Override
  public Map<String, Long> getOfflinePushProgress(String topic) {
    // As the monitor does NOT delete the replica of disconnected storage node, once a storage node failed, its
    // replicas statuses still be counted in the unfiltered progress map.
    Map<String, Long> progressMap = super.getOfflinePushProgress(topic);
    Set<String> liveInstances = routingDataRepository.getLiveInstancesMap().keySet();
    Iterator<String> replicaIdIterator = progressMap.keySet().iterator();
    // Filter the progress map to remove replicas of disconnected storage node.
    while (replicaIdIterator.hasNext()) {
      String replicaId = replicaIdIterator.next();
      String instanceId = ReplicaStatus.getInstanceIdFromReplicaId(replicaId);
      if (!liveInstances.contains(instanceId)) {
        replicaIdIterator.remove();
      }
    }
    return progressMap;
  }

  @Override
  public void markOfflinePushAsError(String topic, String statusDetails){
    super.markOfflinePushAsError(topic, statusDetails);
    routingDataRepository.unSubscribeRoutingDataChange(topic, this);
  }

  @Override
  public void onPartitionStatusChange(OfflinePushStatus offlinePushStatus, ReadOnlyPartitionStatus partitionStatus) {
    //only streaming push status is checked here. Batch push status is updated according to routing data.
    checkHybridPushStatus(offlinePushStatus);
  }


  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    synchronized (getLock()) {
      logger.info("Received the routing data changed notification for topic:" + partitionAssignment.getTopic());
      String kafkaTopic = partitionAssignment.getTopic();
      OfflinePushStatus pushStatus = getTopicToPushMap().get(kafkaTopic);

      if (pushStatus != null) {
        Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(pushStatus, partitionAssignment);
        if (!status.getFirst().equals(pushStatus.getCurrentStatus())) {
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
  private void checkHybridPushStatus(OfflinePushStatus offlinePushStatus) {
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

  private void handleOfflinePushUpdate(OfflinePushStatus pushStatus, ExecutionStatus status, Optional<String> statusDetails) {
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

  @Override
  protected void handleCompletedPush(OfflinePushStatus pushStatus) {
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    super.handleCompletedPush(pushStatus);
  }

  @Override
  protected void handleErrorPush(OfflinePushStatus pushStatus, String statusDetails) {
    routingDataRepository.unSubscribeRoutingDataChange(pushStatus.getKafkaTopic(), this);
    super.handleErrorPush(pushStatus, statusDetails);
  }
}
