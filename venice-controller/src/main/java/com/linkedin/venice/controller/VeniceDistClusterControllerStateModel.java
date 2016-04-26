package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import java.util.concurrent.ConcurrentMap;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


/**
 * State model used to handle the change of leader-standby relationship for controllers.
 * <p>
 * This class should extend DistClusterControllerStateModel, but we need a helix manager to to the essential
 * initialization but it is the private member of DistClusterControllerStateModel, so we can't get it in sub-class. So
 * we don't extend it right now. //TODO Will ask Helix team to modify the visibility.
 */
@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = {HelixState.LEADER_STATE, HelixState.STANDBY_STATE})
public class VeniceDistClusterControllerStateModel extends StateModel {
  public static final String PARTITION_SUBFIX = "_0";
  private static Logger logger = Logger.getLogger(VeniceDistClusterControllerStateModel.class);

  private HelixManager controller;
  private VeniceHelixResources resources;
  private final ZkClient zkClient;
  private String clusterName;

  private final ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap;

  public VeniceDistClusterControllerStateModel(ZkClient zkClient,
      ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap) {
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(VeniceDistClusterControllerStateModel.class);
    this.zkClient = zkClient;
    this.clusterToConfigsMap = clusterToConfigsMap;
  }

  @Transition(to = HelixState.LEADER_STATE, from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());

    if (!clusterToConfigsMap.containsKey(clusterName)) {
      throw new VeniceException("No configuration exists for " + clusterName);
    }

    String controllerName = message.getTgtName();
    logger.info(controllerName + " becoming leader from standby for " + clusterName);
    if (controller == null) {
      controller = HelixManagerFactory
          .getZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, zkClient.getServers());
      controller.connect();
      controller.startTimerTasks();

      resources = new VeniceHelixResources(clusterName, zkClient, controller, clusterToConfigsMap.get(clusterName));
      resources.refresh();

      logger.info(controllerName + " is the leader of " + clusterName);
    } else {
      logger.error("controller already exists:" + controller.getInstanceName() + " for " + clusterName);
    }
  }

  @Transition(to = "STANDBY", from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming standby from leader for " + clusterName);
    reset();
  }

  @Transition(to = "OFFLINE", from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming offline from standby for " + clusterName);
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
    String controllerName = message.getTgtName();
    logger.info(controllerName + " becoming standby from offline for " + clusterName);
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
    String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
    String controllerName = message.getTgtName();

    logger.error(controllerName + " rollbacks on error for " + clusterName);
    reset();
  }

  @Override
  public void reset() {
    closeController();
    clearResources();
  }

  private void closeController() {
    if (controller != null) {
      controller.disconnect();
      controller = null;
    }
  }

  private void clearResources() {
    if (resources != null) {
      resources.clear();
      resources = null;
    }
  }

  protected static String getVeniceClusterNameFromPartitionName(String partitionName) {
    //Exclude the partition id.
    if (!partitionName.endsWith(PARTITION_SUBFIX)) {
      throw new VeniceException("Invalid partition name:" + partitionName + " should end with " + PARTITION_SUBFIX);
    }
    return partitionName.substring(0, partitionName.lastIndexOf('_'));
  }

  protected static String getPartitionNameFromVeniceClusterName(String veniceClusterName) {
    return veniceClusterName + PARTITION_SUBFIX;
  }

  public VeniceHelixResources getResources() {
    if (resources == null) {
      throw new VeniceException("Can not get the resources, current controller is not the leader of " + clusterName);
    }
    return resources;
  }
}
