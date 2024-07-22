package com.linkedin.venice.controller;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * State model used to handle the change of leader-standby relationship for controllers.
 * <p>
 * This class should extend DistClusterControllerStateModel, but we need a helix manager to to the essential
 * initialization but it is the private member of DistClusterControllerStateModel, so we can't get it in sub-class. So
 * we don't extend it right now. //TODO Will ask Helix team to modify the visibility.
 */
@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = { HelixState.LEADER_STATE, HelixState.STANDBY_STATE })
public class VeniceControllerStateModel extends StateModel {
  private static final String PARTITION_SUFFIX = "_0";
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerStateModel.class);

  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final Optional<DynamicAccessController> accessController;
  private final String clusterName;
  private final HelixAdminClient helixAdminClient;
  private final RealTimeTopicSwitcher realTimeTopicSwitcher;

  private VeniceControllerConfig clusterConfig;
  private SafeHelixManager helixManager;
  private HelixVeniceClusterResources clusterResources;

  public VeniceControllerStateModel(
      String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      Optional<DynamicAccessController> accessController,
      HelixAdminClient helixAdminClient) {
    this._currentState = new StateModelParser().getInitialState(VeniceControllerStateModel.class);
    this.clusterName = clusterName;
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.realTimeTopicSwitcher = realTimeTopicSwitcher;
    this.accessController = accessController;
    this.helixAdminClient = helixAdminClient;
  }

  /**
   * Test if current state is {@link HelixState#LEADER_STATE}.
   * @return  <code>true</code> if current state is {@link HelixState#LEADER_STATE};
   *          <code>false</code> otherwise.
   */
  public boolean isLeader() {
    synchronized (_currentState) {
      return getCurrentState().equals(HelixState.LEADER_STATE);
    }
  }

  /**
   * This runs after the state transition occurred.
   */
  @Override
  public boolean updateState(String newState) {
    boolean result;
    synchronized (_currentState) {
      result = super.updateState(newState);
    }
    if (newState.equals(HelixState.LEADER_STATE)) {
      controllerInitialization.execute(clusterName);
    }
    return result;
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

  /**
   * A callback for Helix state transition from {@link HelixState#STANDBY_STATE} to {@link HelixState#LEADER_STATE}.
   */
  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      if (clusterConfig == null) {
        throw new VeniceException("No configuration exists for " + clusterName);
      }
      String controllerName = message.getTgtName();
      LOGGER.info("{} becoming leader from standby for {}", controllerName, clusterName);

      if (helixManagerInitialized()) {
        // TODO: It seems like this should throw an exception. Otherwise the case would be you'd have an instance be
        // leader
        // in Helix that hadn't subscribed to any resource. This could happen if a state transition thread timed out and
        // ERROR'd
        // and the partition was 'reset' instead of bouncing the process.
        LOGGER.error(
            "Helix manager already exists for instance {} on cluster {} and received controller name {}",
            helixManager.getInstanceName(),
            clusterName,
            controllerName);

      } else {
        initHelixManager(controllerName);
        initClusterResources();
        LOGGER.info(
            "Controller {} with instance {} is the leader of cluster {}",
            controllerName,
            helixManager.getInstanceName(),
            clusterName);
      }
    });
  }

  private boolean helixManagerInitialized() {
    return helixManager != null && helixManager.isConnected();
  }

  private void initHelixManager(String controllerName) throws Exception {
    if (helixManagerInitialized()) {
      throw new VeniceException(
          String.format(
              "Helix manager has been initialized with instance %s for cluster %s",
              helixManager.getInstanceName(),
              clusterName));
    }
    InstanceType instanceType =
        clusterConfig.isVeniceClusterLeaderHAAS() ? InstanceType.SPECTATOR : InstanceType.CONTROLLER;
    helixManager = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, controllerName, instanceType, zkClient.getServers()));
    helixManager.connect();
    helixManager.startTimerTasks();
  }

  private void initClusterResources() {
    if (!helixManagerInitialized()) {
      throw new VeniceException("Helix manager should have been initialized for " + clusterName);
    }
    clusterResources = new HelixVeniceClusterResources(
        clusterName,
        zkClient,
        adapterSerializer,
        helixManager,
        clusterConfig,
        admin,
        metricsRepository,
        realTimeTopicSwitcher,
        accessController,
        helixAdminClient);
    clusterResources.refresh();
    clusterResources.startErrorPartitionResetTask();
    clusterResources.startLeakedPushStatusCleanUpService();
  }

  /**
   * A callback for Helix state transition from {@link HelixState#LEADER_STATE} to {@link HelixState#STANDBY_STATE}.
   */
  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      String controllerName = message.getTgtName();

      LOGGER.info("{} becoming standby from leader for {}", controllerName, clusterName);
      // Reset acquires the cluster write lock to prevent the partial result of admin operation.
      reset();
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#STANDBY_STATE} to {@link HelixState#OFFLINE_STATE}.
   */
  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      String controllerName = message.getTgtName();
      LOGGER.info("{} becoming offline from standby for {}", controllerName, clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#OFFLINE_STATE} to {@link HelixState#STANDBY_STATE}.
   */
  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      clusterConfig = multiClusterConfigs.getControllerConfig(clusterName);
      String controllerName = message.getTgtName();
      LOGGER.info("{} becoming standby from offline for {}", controllerName, clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#OFFLINE_STATE} to {@link HelixState#DROPPED_STATE}.
   */
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      LOGGER.info("{} going from OFFLINE to DROPPED.", clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#ERROR_STATE} to {@link HelixState#DROPPED_STATE}.
   */
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      LOGGER.info("{} going from ERROR to DROPPED.", clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#ERROR_STATE} to {@link HelixState#OFFLINE_STATE}.
   */
  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
  public void onBecomingOfflineFromError(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      LOGGER.info("{} going from ERROR to OFFLINE.", clusterName);
    });
  }

  /**
   * Called when error occurs in state transition.
   */
  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
    String controllerName = message.getTgtName();
    LOGGER.error("{} rollbacks on error for {}", controllerName, clusterName);
    reset();
  }

  /**
   * Called when the state model is reset.
   */
  @Override
  public synchronized void reset() {
    if (clusterResources != null) {
      try (AutoCloseableLock ignore = clusterResources.lockForShutdown()) {
        clearResources();
        closeHelixManager();
      }
    }
  }

  /** synchronized because concurrent calls could cause a NPE */
  private synchronized void closeHelixManager() {
    if (helixManager != null) {
      helixManager.disconnect();
      helixManager = null;
    }
  }

  /** synchronized because concurrent calls could cause a NPE */
  private synchronized void clearResources() {
    if (clusterResources != null) {
      /**
       * Leaked push status clean up service depends on VeniceHelixAdmin, so VeniceHelixAdmin should be stopped after
       * its dependent service.
       */
      clusterResources.stopLeakedPushStatusCleanUpService();
      clusterResources.clear();
      clusterResources.stopErrorPartitionResetTask();
      clusterResources = null;
    }
  }

  /**
   * Get the regular Venice cluster name after removing the suffix {@code PARTITION_SUFFIX}.
   * @param partitionName controller partition name.
   * @return Venice cluster name.
   */
  protected static String getVeniceClusterNameFromPartitionName(String partitionName) {
    // Exclude the partition id.
    if (!partitionName.endsWith(PARTITION_SUFFIX)) {
      throw new VeniceException("Invalid partition name:" + partitionName + " should end with " + PARTITION_SUFFIX);
    }
    return partitionName.substring(0, partitionName.lastIndexOf('_'));
  }

  /**
   * Get the controller partition name. The suffix {@code PARTITION_SUFFIX} is used after the regular cluster name.
   * @param veniceClusterName Venice cluster name.
   * @return partition name for the input Venice cluster.
   */
  protected static String getPartitionNameFromVeniceClusterName(String veniceClusterName) {
    return veniceClusterName + PARTITION_SUFFIX;
  }

  /**
   * @return an {@code Optional} describing the Venice cluster aggregated resources, if non-null,
   * otherwise returns an empty {@code Optional}.
   */
  protected Optional<HelixVeniceClusterResources> getResources() {
    return Optional.ofNullable(clusterResources);
  }

  /**
   * @return the name of the Venice cluster that the model manages.
   */
  protected String getClusterName() {
    return clusterName;
  }
}
