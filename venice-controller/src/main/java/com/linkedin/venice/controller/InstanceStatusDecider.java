package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.HelixUtils;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.log4j.Logger;


public class InstanceStatusDecider {
  private static final Logger logger = Logger.getLogger(InstanceStatusDecider.class);
  /**
   * Minimum number of replica in cluster. TODO should be read from configuration.
   */
  private static final int MIN_NUMBER_REPLICA = 1;

  public static boolean isRemovable(VeniceHelixResources resources, String clusterName, String instanceId) {
    HelixManager manager = resources.getController();
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    try {
      // Get session id at first then get current states of given instance and give session.
      LiveInstance instance = accessor.getProperty(keyBuilder.liveInstance(instanceId));
      if(instance == null){
        // If instance is not alive, it's removable.
        return true;
      }
      String sessionId = instance.getSessionId();
      List<CurrentState> currentStates = accessor.getChildValues(keyBuilder.currentStates(instanceId, sessionId));
      if (logger.isDebugEnabled()) {
        logger.debug("Found " + currentStates.size() + " resources for instance:" + instanceId);
      }
      RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();

      for (CurrentState currentState : currentStates) {
        String resourceName = currentState.getResourceName();
        // Get the partitionName->states map for thsi instance.
        Map<String, String> partitionToStates = currentState.getPartitionStateMap();
        for (Map.Entry<String, String> partitionState : partitionToStates.entrySet()) {
          if (isReadyToServe(partitionState.getValue())) {
            // Only Online replica is considered. If replica is not ready to serve, it does not matter if
            // instance is moved out of cluster.
            int partitionId = HelixUtils.getPartitionId(partitionState.getKey());
            Partition partition = routingDataRepository.getPartitionAssignments(resourceName).getPartition(partitionId);

            // Compare the number of reday to serve instance to minimum required number of replicas. Could add more criteria in the future.
            int currentReplicas = partition.getReadyToServeInstances().size();
            if (logger.isDebugEnabled()) {
              logger.debug(partitionState.getKey() + " have " + currentReplicas + " replicas. Minimum required:"
                  + MIN_NUMBER_REPLICA);
            }
            if (currentReplicas <= MIN_NUMBER_REPLICA) {
              return false;
            }
          }
        }
      }
      return true;
    } catch (Exception e) {
      String errorMsg = "Can not get current states for instance:" + instanceId + " from Zookeeper";
      logger.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  // TODO, right now we only consider ONLINE replicas to be ready to serve. But in the future, we should also consider
  // TODO BOOTSTRAP replica as well in some cases. More discussion could be found in r/781272
  private static boolean isReadyToServe(String replicaState){
    if(replicaState.contentEquals(HelixState.ONLINE_STATE)){
      return true;
    }
    return  false;
  }
}
