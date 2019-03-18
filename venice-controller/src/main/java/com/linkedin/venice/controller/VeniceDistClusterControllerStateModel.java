package com.linkedin.venice.controller;

import com.linkedin.venice.controller.init.ControllerInitializationManager;
import com.linkedin.venice.controller.init.ControllerInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.StoreCleaner;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
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

  private SafeHelixManager controller;
  private VeniceHelixResources resources;
  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final StoreCleaner storeCleaner;
  private final MetricsRepository metricsRepository;
  private final ControllerInitializationRoutine controllerInitialization;
  private String clusterName;

  private final ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap;

  public VeniceDistClusterControllerStateModel(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      ConcurrentMap<String, VeniceControllerClusterConfig> clusterToConfigsMap, StoreCleaner storeCleaner,
      MetricsRepository metricsRepository, ControllerInitializationRoutine controllerInitialization) {
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(VeniceDistClusterControllerStateModel.class);
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.clusterToConfigsMap = clusterToConfigsMap;
    this.storeCleaner = storeCleaner;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
  }

  /**
   * This runs after the state transition occurred.
   */
  @Override
  public boolean updateState(String newState) {
    boolean returnValue = super.updateState(newState);
    if (newState.equals(HelixState.LEADER_STATE)) {
      controllerInitialization.execute(clusterName);
    }
    return returnValue;
  }

  private void executeStateTransition(Message message, StateTransition stateTransition) throws VeniceException {
    String from = message.getFromState();
    String to = message.getToState();
    String threadName = "Helix-ST-" + message.getResourceName() + "-" + from + "->" + to;
    // Change name to indicate which st is occupied this thread.
    Thread.currentThread().setName(threadName);
    try {
      stateTransition.execute();
    } catch (Exception e) {
      throw new VeniceException("Failed to execute '" + threadName + "'.", e);
    } finally {
      // Once st is terminated, change the name to indicate this thread will not be occupied by this st.
      Thread.currentThread().setName("Inactive ST thread.");
    }
  }

  private interface StateTransition {
    void execute() throws Exception;
  }

  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());

      if (!clusterToConfigsMap.containsKey(clusterName)) {
        throw new VeniceException("No configuration exists for " + clusterName);
      }

      String controllerName = message.getTgtName();
      logger.info(controllerName + " becoming leader from standby for " + clusterName);
      if (controller == null) {
        controller = new SafeHelixManager(
            HelixManagerFactory.getZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, zkClient.getServers()));
        controller.connect();
        controller.startTimerTasks();
        resources = new VeniceHelixResources(clusterName, zkClient, adapterSerializer, controller,
            clusterToConfigsMap.get(clusterName), storeCleaner, metricsRepository);
        resources.refresh();
        logger.info(controllerName + " is the leader of " + clusterName);
      } else {
        logger.error("controller already exists:" + controller.getInstanceName() + " for " + clusterName);
      }
    });
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
      String controllerName = message.getTgtName();

      logger.info(controllerName + " becoming standby from leader for " + clusterName);
      // We have to create a snapshot of resources here, as the resources will be set to null during the reset. So this
      // snapshot will let us unlock successfully.
      VeniceHelixResources snapshot = resources;
      snapshot.lockForShutdown(); // Lock to prevent the partial result of admin operation.
      try {
        reset();
      } finally {
        // After reset everything, unlock then hand-off the mastership.
        snapshot.unlockForShutdown();
      }
    });
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      String clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
      String controllerName = message.getTgtName();

      logger.info(controllerName + " becoming offline from standby for " + clusterName);
    });
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      clusterName = getVeniceClusterNameFromPartitionName(message.getPartitionName());
      String controllerName = message.getTgtName();
      logger.info(controllerName + " becoming standby from offline for " + clusterName);
    });
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

  public Optional<VeniceHelixResources> getResources() {
    if (resources == null) {
      return Optional.empty();
    } else {
      return Optional.of(resources);
    }
  }

  public String getClusterName() {
    return clusterName;
  }
}
