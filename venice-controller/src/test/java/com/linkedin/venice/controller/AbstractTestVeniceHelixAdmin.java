package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.VeniceStateModel;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;


class AbstractTestVeniceHelixAdmin {
  static final long MASTER_CHANGE_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  static final long TOTAL_TIMEOUT_FOR_LONG_TEST_MS = 60 * Time.MS_PER_SECOND;
  static final long TOTAL_TIMEOUT_FOR_SHORT_TEST_MS = 10 * Time.MS_PER_SECOND;
  static final int DEFAULT_REPLICA_COUNT = 1;

  static final String KEY_SCHEMA = "\"string\"";
  static final String VALUE_SCHEMA = "\"string\"";
  static final int MAX_NUMBER_OF_PARTITION = 16;
  static String NODE_ID = "localhost_9985";
  static int SERVER_LISTENING_PORT = 9985;

  final Logger logger = Logger.getLogger(getClass().getSimpleName());

  VeniceHelixAdmin veniceAdmin;
  String clusterName;
  String storeOwner = "Doge of Venice";
  VeniceControllerConfig controllerConfig;

  String zkAddress;
  String kafkaZkAddress;

  ZkServerWrapper zkServerWrapper;
  private ZkServerWrapper kafkaZkServer;
  KafkaBrokerWrapper kafkaBrokerWrapper;
  SafeHelixManager helixManager;
  Map<String, SafeHelixManager> helixManagerByNodeID = new HashMap<>();

  VeniceProperties controllerProps;
  Map<String,MockTestStateModelFactory> stateModelFactoryByNodeID = new HashMap<>();
  HelixMessageChannelStats helixMessageChannelStats;
  VeniceControllerMultiClusterConfig multiClusterConfig;

  public void setupCluster() throws Exception {
    setupCluster(true);
  }

  public void setupCluster(boolean createParticipantStore) throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    kafkaZkServer = ServiceFactory.getZkServer();
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(kafkaZkServer);
    kafkaZkAddress = kafkaBrokerWrapper.getZkAddress();
    clusterName = Utils.getUniqueString("test-cluster");
    Properties properties = getControllerProperties(clusterName);
    if (!createParticipantStore) {
      properties.put(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
      properties.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    }
    controllerProps = new VeniceProperties(properties);
    helixMessageChannelStats = new HelixMessageChannelStats(new MetricsRepository(), clusterName);
    controllerConfig = new VeniceControllerConfig(controllerProps);
    multiClusterConfig = TestUtils.getMultiClusterConfigFromOneCluster(controllerConfig);
    veniceAdmin = new VeniceHelixAdmin(multiClusterConfig, new MetricsRepository(), D2TestUtils.getAndStartD2Client(zkAddress));
    veniceAdmin.initVeniceControllerClusterResource(clusterName);
    startParticipant();
    waitUntilIsMaster(veniceAdmin, clusterName, MASTER_CHANGE_TIMEOUT_MS);
  }

  public void cleanupCluster() {
    stopAllParticipants();
    try {
      veniceAdmin.stop(clusterName);
      veniceAdmin.close();
    } catch (Exception e) {
      logger.warn(e);
    }
    zkServerWrapper.close();
    kafkaBrokerWrapper.close();
    kafkaZkServer.close();
  }

  void startParticipant() throws Exception {
    startParticipant(false, NODE_ID);
  }

  void delayParticipantJobCompletion(boolean isDelay) {
    for (MockTestStateModelFactory stateModelFactory: stateModelFactoryByNodeID.values()) {
      stateModelFactory.setBlockTransition(isDelay);
    }
  }

  void startParticipant(boolean isDelay, String nodeId) throws Exception {
    MockTestStateModelFactory stateModelFactory;
    if (stateModelFactoryByNodeID.containsKey(nodeId)) {
      stateModelFactory = stateModelFactoryByNodeID.get(nodeId);
    } else {
      stateModelFactory = new MockTestStateModelFactory();
      stateModelFactoryByNodeID.put(nodeId, stateModelFactory);
    }
    stateModelFactory.setBlockTransition(isDelay);
    helixManager = TestUtils.getParticipant(clusterName, nodeId, zkAddress, SERVER_LISTENING_PORT, stateModelFactory,
        VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL);
    helixManager.connect();
    helixManagerByNodeID.put(nodeId, helixManager);
    HelixUtils.setupInstanceConfig(clusterName, nodeId, zkAddress);
  }

  void stopAllParticipants() {
    for (SafeHelixManager helixManager: helixManagerByNodeID.values()) {
      helixManager.disconnect();
    }
    helixManagerByNodeID.clear();
    stateModelFactoryByNodeID.clear();
  }

  void stopParticipant(String nodeId) {
    if (helixManagerByNodeID.containsKey(nodeId)) {
      helixManagerByNodeID.get(nodeId).disconnect();
      helixManagerByNodeID.remove(nodeId);
      stateModelFactoryByNodeID.remove(nodeId);
    }
  }

  Properties getControllerProperties(String clusterName) throws IOException {
    String currentPath = Paths.get("").toAbsolutePath().toString();
    if (currentPath.endsWith("venice-controller")) {
      currentPath += "/..";
    }
    VeniceProperties clusterProps = Utils.parseProperties(currentPath + "/venice-server/config/cluster.properties");
    VeniceProperties baseControllerProps =
        Utils.parseProperties(currentPath + "/venice-controller/config/controller.properties");
    clusterProps.getString(ConfigKeys.CLUSTER_NAME);
    Properties properties = new Properties();
    properties.putAll(clusterProps.toProperties());
    properties.putAll(baseControllerProps.toProperties());
    properties.put(ENABLE_TOPIC_REPLICATOR, false);
    properties.put(KAFKA_ZK_ADDRESS, kafkaZkAddress);
    properties.put(ZOOKEEPER_ADDRESS, zkAddress);
    properties.put(CLUSTER_NAME, clusterName);
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress());
    properties.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, MAX_NUMBER_OF_PARTITION);
    properties.put(DEFAULT_PARTITION_SIZE, 100);
    properties.put(CLUSTER_TO_D2, TestUtils.getClusterToDefaultD2String(clusterName));
    properties.put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true);
    properties.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, false);
    properties.put(PARTICIPANT_MESSAGE_STORE_ENABLED, true);
    properties.put(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, true);
    properties.put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterName);
    return properties;
  }

  void waitUntilIsMaster(VeniceHelixAdmin admin, String cluster, long timeout) {
    List<VeniceHelixAdmin> admins = Collections.singletonList(admin);
    waitForAMaster(admins, cluster, timeout);
  }

  void waitForAMaster(List<VeniceHelixAdmin> admins, String cluster, long timeout) {
    int sleepDuration = 100;
    for (long i = 0; i < timeout; i += sleepDuration) {
      for (VeniceHelixAdmin admin : admins) {
        if (admin.isLeaderControllerFor(cluster)) {
          return;
        }
      }

      try {
        Thread.sleep(sleepDuration);
      } catch (InterruptedException e) {
        break;
      }
    }

    Assert.fail("No VeniceHelixAdmin became master for cluster: " + cluster + " after timeout: " + timeout);
  }

  VeniceHelixAdmin getMaster(List<VeniceHelixAdmin> admins, String cluster) {
    for (VeniceHelixAdmin admin : admins) {
      if (admin.isLeaderControllerFor(cluster)) {
        return admin;
      }
    }
    throw new VeniceException("no master found for cluster: " + cluster);
  }

  VeniceHelixAdmin getSlave(List<VeniceHelixAdmin> admins, String cluster) {
    for (VeniceHelixAdmin admin : admins) {
      if (!admin.isLeaderControllerFor(cluster)) {
        return admin;
      }
    }
    throw new VeniceException("no slave found for cluster: " + cluster);
  }

  /**
   * Participant store should be set up by child controller.
   */
  void verifyParticipantMessageStoreSetup() {
    String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Store store = veniceAdmin.getStore(clusterName, participantStoreName);
      Assert.assertNotNull(store);
      Assert.assertEquals(store.getVersions().size(), 1);
    });
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS,
        () -> Assert.assertEquals(veniceAdmin.getRealTimeTopic(clusterName, participantStoreName),
            Version.composeRealTimeTopic(participantStoreName)));
  }
}
