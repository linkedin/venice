package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.log4j.Logger;


/**
 * This class is not Thread-safe.
 */
public class InstanceStatusDecider {
  private static final Logger logger = Logger.getLogger(InstanceStatusDecider.class);

  protected static List<Replica> getReplicasForInstance(VeniceHelixResources resources, String instanceId) {
    RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
    ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
    List<Replica> replicas = new ArrayList<>();
    // lock resource assignment to avoid it's updated by routing data repository during the searching.
    synchronized (resourceAssignment) {
      Set<String> resourceNames = resourceAssignment.getAssignedResources();

      for (String resourceName : resourceNames) {
        PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
        for (Partition partition : partitionAssignment.getAllPartitions()) {
          String status = partition.getInstanceStatusById(instanceId);
          if (status != null) {
            Replica replica = new Replica(Instance.fromNodeId(instanceId), partition.getId(), resourceName);
            replica.setStatus(status);
            replicas.add(replica);
          }
        }
      }
    }
    return replicas;
  }

  /**
   * Decide whether the given instance could be move out from the cluster.
   */
  protected static boolean isRemovable(VeniceHelixResources resources, String clusterName, String instanceId,
      int minRequiredOnlineReplicaToStopServer) {
    HelixManager manager = resources.getController();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    try {
      // Get session id at first then get current states of given instance and give session.
      LiveInstance instance = accessor.getProperty(keyBuilder.liveInstance(instanceId));
      if (instance == null) {
        // If instance is not alive, it's removable.
        logger.info("Instance:" + instanceId + " is not a live instance, could be removed.");
        return true;
      }
      List<Replica> replicas = getReplicasForInstance(resources, instanceId);
      return isRemovableWithoutLosingData(resources, instanceId, replicas, minRequiredOnlineReplicaToStopServer)
          && isRemovableWithoutFailingPush(resources, instanceId, replicas);
    } catch (Exception e) {
      String errorMsg = "Can not get current states for instance:" + instanceId + " from Zookeeper";
      logger.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  /**
   * Decide whether the given instance could be moved out from cluster without losing any data in Venice. The criteria:
   * does the server hold the last "ReadyToServe" replica for any ONLINE version. ONLINE means the version is ready to
   * server read request. It might or might NOT be the current version of a store.
   */
  protected static boolean isRemovableWithoutLosingData(VeniceHelixResources resources, String instanceId,
      List<Replica> replicas, int minRequiredOnlineReplicaToStopServer) {
    RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
    ReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
    for (Replica replica : replicas) {
      // Only Online replica is considered. If replica is not ready to serve, it does not matter if
      // instance is moved out of cluster. Because we would not lose any of data.
      if (isReadyToServe(replica.getStatus())) {
        String resourceName = replica.getResource();
        Store store = storeRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName));
        if (store == null) {
          logger.info("Can not find store for the resource:" + resourceName + " get from current state of server:"
              + instanceId);
          continue;
        }
        if (!store.isVersionInStatue(Version.parseVersionFromKafkaTopicName(resourceName), VersionStatus.ONLINE)) {
          logger.debug("Ignore the Version which is not ONLINE. Resource:" + resourceName);
          continue;
        }

        Partition partition =
            routingDataRepository.getPartitionAssignments(resourceName).getPartition(replica.getPartitionId());
        // Compare the number of ready to serve instance to minimum required number of replicas. Could add more criteria in the future.
        int currentReplicas = partition.getReadyToServeInstances().size();
        if (logger.isDebugEnabled()) {
          logger.debug(HelixUtils.getPartitionName(resourceName, replica.getPartitionId()) + " have " + currentReplicas
              + " replicas. Minimum required: " + minRequiredOnlineReplicaToStopServer);
        }
        if (currentReplicas <= minRequiredOnlineReplicaToStopServer) {
          logger.info(
              "Current number of replicas:" + currentReplicas + " are smaller than min required number of replicas:"
                  + minRequiredOnlineReplicaToStopServer);
          return false;
        }
      }
    }
    return true;
  }

  // TODO, right now we only consider ONLINE replicas to be ready to serve. But in the future, we should also consider
  // TODO BOOTSTRAP replica as well in some cases. More discussion could be found in r/781272
  private static boolean isReadyToServe(String replicaState){
    if(replicaState.contentEquals(HelixState.ONLINE_STATE)){
      return true;
    }
    return  false;
  }

  /**
   * Decide whether the given instance could be moved from cluster without failing any of running offline push.
   */
  protected static boolean isRemovableWithoutFailingPush(VeniceHelixResources resources, String instanceId,
      List<Replica> replicas) {
    VeniceJobManager jobManager = resources.getJobManager();
    RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
    ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
    synchronized (resourceAssignment) {
      Set<String> resourceNameSet = replicas.stream().map(Replica::getResource).collect(Collectors.toSet());
      for (String resourceName : resourceNameSet) {
        PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
        PartitionAssignment partitionAssignmentAfterRemoving =
            new PartitionAssignment(resourceName, partitionAssignment.getExpectedNumberOfPartitions());

        for (Partition partition : partitionAssignment.getAllPartitions()) {
          Partition partitionAfterRemoving = partition.withRemovedInstance(instanceId);
          partitionAssignmentAfterRemoving.addPartition(partitionAfterRemoving);
        }

        if (jobManager.willJobFail(resourceName, partitionAssignmentAfterRemoving)) {
          logger.info("The job for topic:" + resourceName + " would fail if we remove this instance:" + instanceId);
          return false;
        }
      }
    }
    return true;
  }
}
