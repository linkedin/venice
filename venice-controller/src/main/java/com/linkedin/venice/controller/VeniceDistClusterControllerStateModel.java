package com.linkedin.venice.controller;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.replication.TopicReplicator;
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

  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final StoreCleaner storeCleaner;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final Optional<DynamicAccessController> accessController;
  private final String clusterName;
  private final HelixAdminClient helixAdminClient;

  private VeniceControllerConfig clusterConfig;
  private SafeHelixManager controller;
  private VeniceHelixResources resources;

  private final Optional<TopicReplicator> onlineOfflineTopicReplicator;
  private final Optional<TopicReplicator> leaderFollowerTopicReplicator;

  private final MetadataStoreWriter metadataStoreWriter;

  public VeniceDistClusterControllerStateModel(String clusterName, ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      VeniceControllerMultiClusterConfig multiClusterConfigs, StoreCleaner storeCleaner, MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization, Optional<TopicReplicator> onlineOfflineTopicReplicator,
      Optional<TopicReplicator> leaderFollowerTopicReplicator, Optional<DynamicAccessController> accessController,
      MetadataStoreWriter metadataStoreWriter, HelixAdminClient helixAdminClient) {
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(VeniceDistClusterControllerStateModel.class);
    this.clusterName = clusterName;
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.multiClusterConfigs = multiClusterConfigs;
    this.storeCleaner = storeCleaner;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.onlineOfflineTopicReplicator = onlineOfflineTopicReplicator;
    this.leaderFollowerTopicReplicator = leaderFollowerTopicReplicator;
    this.accessController = accessController;
    this.metadataStoreWriter = metadataStoreWriter;
    this.helixAdminClient = helixAdminClient;
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
      if (clusterConfig == null) {
        throw new VeniceException("No configuration exists for " + clusterName);
      }
      boolean isStorageClusterHAAS = clusterConfig.isVeniceClusterLeaderHAAS();
      String controllerName = message.getTgtName();
      logger.info(controllerName + " becoming leader from standby for " + clusterName);
      if (controller == null || !controller.isConnected()) {
        InstanceType instanceType = isStorageClusterHAAS ? InstanceType.SPECTATOR : InstanceType.CONTROLLER;
        controller = new SafeHelixManager(
            HelixManagerFactory.getZKHelixManager(clusterName, controllerName, instanceType, zkClient.getServers()));
        controller.connect();
        controller.startTimerTasks();
        resources = new VeniceHelixResources(clusterName, zkClient, adapterSerializer, controller, clusterConfig,
            storeCleaner, metricsRepository, onlineOfflineTopicReplicator, leaderFollowerTopicReplicator,
            accessController, metadataStoreWriter, helixAdminClient);
        resources.refresh();
        resources.startErrorPartitionResetTask();
        logger.info(controllerName + " is the leader of " + clusterName);
      } else {
        // TODO: It seems like this should throw an exception.  Otherwise the case would be you'd have an instance be leader
        // in Helix that hadn't subscribed to any resource.  This could happen if a state transition thread timed out and ERROR'd
        // and the partition was 'reset' instead of bouncing the process.
        logger.error("controller already exists:" + controller.getInstanceName() + " for " + clusterName);
      }
    });
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
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
      String controllerName = message.getTgtName();

      logger.info(controllerName + " becoming offline from standby for " + clusterName);
    });
  }

  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      clusterConfig = multiClusterConfigs.getConfigForCluster(clusterName);
      String controllerName = message.getTgtName();
      logger.info(controllerName + " becoming standby from offline for " + clusterName);
    });
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      logger.info(clusterName + " going from OFFLINE to DROPPED.");
    });
  }

  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      logger.info(clusterName + " going from ERROR to DROPPED.");
    });
  }

  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
  public void onBecomingOfflineFromError(Message message, NotificationContext context) {
    executeStateTransition(message, () -> {
      logger.info(clusterName + " going from ERROR to OFFLINE.");
    });
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
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
      resources.stopErrorPartitionResetTask();
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
