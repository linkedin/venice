package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixManager;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.DistClusterControllerStateModelFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Wrapper to start up Helix as a service in integration tests. Since it's for testing purpose there are only two
 * Helix controller in the Helix super cluster (can be increased if needed).
 */
public class HelixAsAServiceWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "HelixAsAService";
  public static final String HELIX_SUPER_CLUSTER_NAME = "helix_controllers";
  public static final String HELIX_INSTANCE_NAME_PREFIX = "helix_controller_";

  private static final int NUM_OF_SUPER_CLUSTER_CONTROLLERS = 2;

  private static final Logger LOGGER = LogManager.getLogger(HelixAsAServiceWrapper.class);

  private final List<SafeHelixManager> managers;
  private final HelixAdmin admin;
  private final String zkAddress;
  private final PropertyKey.Builder keyBuilder;

  static StatefulServiceProvider<HelixAsAServiceWrapper> generateService(String zkAddress) {
    return (serviceName, dataDirectory) -> new HelixAsAServiceWrapper(serviceName, dataDirectory, zkAddress);
  }

  private HelixAsAServiceWrapper(String serviceName, File dataDirectory, String zkAddress) {
    super(serviceName, dataDirectory);
    this.zkAddress = zkAddress;
    admin = new ZKHelixAdmin(zkAddress);
    createClusterIfAbsent();
    keyBuilder = new PropertyKey.Builder(HELIX_SUPER_CLUSTER_NAME);
    DistClusterControllerStateModelFactory distClusterControllerStateModelFactory =
        new DistClusterControllerStateModelFactory(zkAddress);
    managers = new ArrayList<>();
    for (int i = 0; i < NUM_OF_SUPER_CLUSTER_CONTROLLERS; i++) {
      SafeHelixManager manager = new SafeHelixManager(
          HelixManagerFactory.getZKHelixManager(
              HELIX_SUPER_CLUSTER_NAME,
              HELIX_INSTANCE_NAME_PREFIX + i,
              InstanceType.CONTROLLER_PARTICIPANT,
              zkAddress));
      manager.getStateMachineEngine()
          .registerStateModelFactory(LeaderStandbySMD.name, distClusterControllerStateModelFactory);
      managers.add(manager);
    }
  }

  private void createClusterIfAbsent() {
    if (admin.getClusters().contains(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME)) {
      // Not using log interpolation as this string will be created at compile time
      LOGGER.info("Helix cluster " + HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME + " already exists.");
      return;
    }

    if (!admin.addCluster(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME, false)) {
      throw new VeniceException(
          "Failed to create cluster " + HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME + " successfully.");
    }
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME)
        .build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    helixClusterProperties
        .put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(
        HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME,
        LeaderStandbySMD.name,
        LeaderStandbySMD.build());
  }

  private void initializeController() {
    try {
      for (SafeHelixManager manager: managers) {
        manager.connect();
      }
    } catch (Exception e) {
      // Not using log interpolation as this string will be created at compile time
      String errorMessage = "Encountered error starting the Helix controllers for cluster " + HELIX_SUPER_CLUSTER_NAME;
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }

  public List<SafeHelixManager> getHelixSuperControllers() {
    return managers;
  }

  /**
   * @return {@link LiveInstance} representing the cluster leader, or null if there isn't any.
   */
  public LiveInstance getClusterLeader(String clusterName) {
    PropertyKey.Builder clusterKeyBuilder = new PropertyKey.Builder(clusterName);
    return managers.iterator().next().getHelixDataAccessor().getProperty(clusterKeyBuilder.controllerLeader());
  }

  /**
   * @return {@link LiveInstance} representing the super cluster leader, or null if there isn't any.
   */
  public LiveInstance getSuperClusterLeader() {
    return managers.iterator().next().getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
  }

  public String getZkAddress() {
    return zkAddress;
  }

  @Override
  public String getHost() {
    return "localhost";
  }

  @Override
  public int getPort() {
    return 0;
  }

  @Override
  protected void internalStart() {
    initializeController();
  }

  @Override
  protected void internalStop() {
    for (SafeHelixManager manager: managers) {
      manager.disconnect();
    }
    admin.close();
  }

  @Override
  protected void newProcess() throws Exception {
    throw new Exception("newProcess not implemented for " + HelixAsAServiceWrapper.class.getSimpleName());
  }
}
