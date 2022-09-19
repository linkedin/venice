package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.customizedstate.CustomizedStateProviderFactory;
import org.apache.helix.model.CustomizedState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A parent class to access Helix customized partition state, which is different from the states
 * defined in the state model. The partition state is stored on Zookeeper. This class provides
 * the way to read/write the state.
 * Note this class is only an accessor but not a repository so it will not cache anything in local
 * memory. In other words it's stateless and Thread-Safe.
 * The data structure on ZK would be:
 * /VeniceClusterName/INSTANCES/instanceName/CUSTOMIZEDSTATE/$topic -> customized state for $topic
*/

public abstract class HelixPartitionStateAccessor {
  private static final Logger LOGGER = LogManager.getLogger(HelixPartitionStateAccessor.class);
  CustomizedStateProvider customizedStateProvider;

  public HelixPartitionStateAccessor(HelixManager helixManager, String instanceId) {
    this.customizedStateProvider =
        CustomizedStateProviderFactory.getInstance().buildCustomizedStateProvider(helixManager, instanceId);
  }

  public void updateReplicaStatus(HelixPartitionState stateType, String topic, String partitionName, String status) {
    customizedStateProvider.updateCustomizedState(stateType.name(), topic, partitionName, status);
  }

  public void deleteReplicaStatus(HelixPartitionState stateType, String topic, String partitionName) {
    customizedStateProvider.deletePerPartitionCustomizedState(stateType.name(), topic, partitionName);
  }

  public String getReplicaStatus(HelixPartitionState stateType, String topic, String partitionName) {
    try {
      return customizedStateProvider.getPerPartitionCustomizedState(stateType.name(), topic, partitionName)
          .get(CustomizedState.CustomizedStateProperty.CURRENT_STATE.name());
    } catch (NullPointerException e) {
      String errorMsg = String
          .format("The partition %s does not have " + "state %s available in ZK", partitionName, stateType.name());
      LOGGER.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    } catch (HelixMetaDataAccessException e) {
      String errorMsg = String.format("Failed to get state %s for partition " + "%s", stateType.name(), partitionName);
      LOGGER.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  public Map<String, String> getAllReplicaStatus(HelixPartitionState stateType, String topic) {
    try {
      return customizedStateProvider.getCustomizedState(stateType.name(), topic)
          .getPartitionStateMap(CustomizedState.CustomizedStateProperty.CURRENT_STATE);
    } catch (NullPointerException e) {
      throw new VeniceException(
          String.format("The topic %s does not have " + "state %s available in ZK", topic, stateType.name()),
          e);
    } catch (HelixMetaDataAccessException e) {
      throw new VeniceException(String.format("Failed to get state %s for topic " + "%s", stateType.name(), topic), e);
    }
  }

  /**
   * Only used in test now
   */
  public void setCustomizedStateProvider(CustomizedStateProvider customizedStateProvider) {
    this.customizedStateProvider = customizedStateProvider;
  }
}
