package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixManager;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;


public class HelixControllerWrapper extends ProcessWrapper{
  public static final String SERVICE_NAME = "HelixController";
  public static final String HELIX_INSTANCE_NAME = "helix_controller";

  private static final Logger logger = Logger.getLogger(HelixControllerWrapper.class);

  private final SafeHelixManager manager;
  private final HelixAdmin admin;
  private final String zkAddress;
  private final String helixClusterName;
  private final PropertyKey.Builder keyBuilder;

  static StatefulServiceProvider<HelixControllerWrapper> generateService(String zkAaddress, String helixClusterName) {
    return (serviceName, port, dataDirectory) ->
        new HelixControllerWrapper(serviceName, dataDirectory, zkAaddress, helixClusterName);
  }

  private HelixControllerWrapper(String serviceName, File dataDirectory, String zkAddress, String helixClusterName) {
    super(serviceName, dataDirectory);
    this.zkAddress = zkAddress;
    this.helixClusterName = helixClusterName;
    ZkClient zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    admin = new ZKHelixAdmin(zkClient);
    createClusterIfAbsent();
    manager = new SafeHelixManager(HelixManagerFactory.getZKHelixManager(helixClusterName, HELIX_INSTANCE_NAME,
        InstanceType.CONTROLLER, zkAddress));
    keyBuilder = new PropertyKey.Builder(manager.getClusterName());
  }

  private void createClusterIfAbsent() {
    if (admin.getClusters().contains(helixClusterName)) {
      logger.info(helixClusterName + " already exists.");
      return;
    }

    if (!admin.addCluster(helixClusterName, false)) {
      throw new VeniceException("Failed to create cluster " + helixClusterName + " successfully.");
    }
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(helixClusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(helixClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
  }

  private void initializeController() {
    try {
      manager.connect();
    } catch (Exception e) {
      String errorMessage = "Encountered error starting the Helix controller for cluster " + helixClusterName;
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }

  public LiveInstance getClusterLeader() {
    return manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
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
    manager.disconnect();
    admin.close();
  }

  @Override
  protected void newProcess() throws Exception {
    throw new Exception("newProcess not implemented for " + HelixControllerWrapper.class.getSimpleName());
  }
}
