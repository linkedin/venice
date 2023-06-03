package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS;
import static com.linkedin.venice.ConfigKeys.UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


class AbstractTestVeniceHelixAdmin {
  static final long LEADER_CHANGE_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  static final long TOTAL_TIMEOUT_FOR_LONG_TEST_MS = 60 * Time.MS_PER_SECOND;
  static final long TOTAL_TIMEOUT_FOR_SHORT_TEST_MS = 10 * Time.MS_PER_SECOND;
  static final int DEFAULT_REPLICA_COUNT = 1;

  static final String KEY_SCHEMA = "\"string\"";
  static final String VALUE_SCHEMA = "\"string\"";
  static final int MAX_NUMBER_OF_PARTITION = 16;
  static String NODE_ID = "localhost_9985";
  static int SERVER_LISTENING_PORT = 9985;

  private final static Logger LOGGER = LogManager.getLogger(AbstractTestVeniceHelixAdmin.class);

  VeniceHelixAdmin veniceAdmin;
  String clusterName;
  String storeOwner = "Doge of Venice";
  VeniceControllerConfig controllerConfig;
  String zkAddress;
  ZkServerWrapper zkServerWrapper;
  PubSubBrokerWrapper pubSubBrokerWrapper;
  SafeHelixManager helixManager;
  Map<String, SafeHelixManager> helixManagerByNodeID = new ConcurrentHashMap<>();

  VeniceProperties controllerProps;
  Map<String, MockTestStateModelFactory> stateModelFactoryByNodeID = new ConcurrentHashMap<>();
  HelixMessageChannelStats helixMessageChannelStats;
  VeniceControllerMultiClusterConfig multiClusterConfig;

  final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  public void setupCluster() throws Exception {
    setupCluster(true, new MetricsRepository());
  }

  public void setupCluster(boolean createParticipantStore) throws Exception {
    setupCluster(createParticipantStore, new MetricsRepository());
  }

  public void setupCluster(boolean createParticipantStore, MetricsRepository metricsRepository) throws Exception {
    Utils.thisIsLocalhost();
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    clusterName = Utils.getUniqueString("test-cluster");
    Properties properties = getControllerProperties(clusterName);
    if (!createParticipantStore) {
      properties.put(PARTICIPANT_MESSAGE_STORE_ENABLED, false);
      properties.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, true);
    }
    properties.put(UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED, true);
    controllerProps = new VeniceProperties(properties);
    helixMessageChannelStats = new HelixMessageChannelStats(new MetricsRepository(), clusterName);
    controllerConfig = new VeniceControllerConfig(controllerProps);
    multiClusterConfig = TestUtils.getMultiClusterConfigFromOneCluster(controllerConfig);
    veniceAdmin = new VeniceHelixAdmin(
        multiClusterConfig,
        metricsRepository,
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory());
    veniceAdmin.initStorageCluster(clusterName);
    startParticipant();
    waitUntilIsLeader(veniceAdmin, clusterName, LEADER_CHANGE_TIMEOUT_MS);

    if (createParticipantStore) {
      // Wait for participant store to finish materializing
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Store store =
            veniceAdmin.getStore(clusterName, VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
        Assert.assertNotNull(store);
        Assert.assertEquals(store.getCurrentVersion(), 1);
      });
    }
  }

  public void cleanupCluster() {
    stopAllParticipants();
    try {
      veniceAdmin.stop(clusterName);
      veniceAdmin.close();
    } catch (Exception e) {
      LOGGER.warn(e);
    }
    zkServerWrapper.close();
    pubSubBrokerWrapper.close();
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
    startParticipant(isDelay, nodeId, LeaderStandbySMD.name);
  }

  void startParticipant(boolean isDelay, String nodeId, String stateModel) throws Exception {

    VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor = new VeniceOfflinePushMonitorAccessor(
        clusterName,
        new ZkClient(zkAddress),
        new HelixAdapterSerializer(),
        3,
        1000);

    MockTestStateModelFactory stateModelFactory;

    if (stateModelFactoryByNodeID.containsKey(nodeId)) {
      stateModelFactory = stateModelFactoryByNodeID.get(nodeId);
    } else {
      stateModelFactory = new MockTestStateModelFactory(offlinePushStatusAccessor);
      stateModelFactoryByNodeID.put(nodeId, stateModelFactory);
    }
    stateModelFactory.setBlockTransition(isDelay);
    helixManager =
        TestUtils.getParticipant(clusterName, nodeId, zkAddress, SERVER_LISTENING_PORT, stateModelFactory, stateModel);
    helixManager.connect();
    helixManagerByNodeID.put(nodeId, helixManager);
    HelixUtils.setupInstanceConfig(clusterName, nodeId, zkAddress);
  }

  void stopAllParticipants() {
    for (String nodeID: stateModelFactoryByNodeID.keySet()) {
      stopParticipant(nodeID);
    }
    stateModelFactoryByNodeID.clear();
    helixManagerByNodeID.clear();
  }

  void stopParticipant(String nodeId) {
    if (helixManagerByNodeID.containsKey(nodeId)) {
      helixManagerByNodeID.get(nodeId).disconnect();
      helixManagerByNodeID.remove(nodeId);
      MockTestStateModelFactory stateModelFactory = stateModelFactoryByNodeID.remove(nodeId);
      stateModelFactory.stopAllStateModelThreads();
    }
  }

  Properties getControllerProperties(String clusterName) throws IOException {
    Properties properties = TestUtils.getPropertiesForControllerConfig();
    properties.put(KAFKA_REPLICATION_FACTOR, 1);
    properties.put(ZOOKEEPER_ADDRESS, zkAddress);
    properties.put(CLUSTER_NAME, clusterName);
    properties.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, MAX_NUMBER_OF_PARTITION);
    properties.put(DEFAULT_PARTITION_SIZE, 10);
    properties.put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_d2")));
    properties.put(
        CLUSTER_TO_SERVER_D2,
        TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_server_d2")));
    properties.put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true);
    properties.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, false);
    properties.put(PARTICIPANT_MESSAGE_STORE_ENABLED, true);
    properties.put(TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS, true);
    properties.put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterName);
    properties.put(CHILD_CLUSTER_ALLOWLIST, "dc-0");
    properties.put(CONTROLLER_SSL_ENABLED, false);
    return properties;
  }

  void waitUntilIsLeader(VeniceHelixAdmin admin, String cluster, long timeout) {
    List<VeniceHelixAdmin> admins = Collections.singletonList(admin);
    waitForALeader(admins, cluster, timeout);
  }

  void waitForALeader(List<VeniceHelixAdmin> admins, String cluster, long timeout) {
    int sleepDuration = 100;
    for (long i = 0; i < timeout; i += sleepDuration) {
      for (VeniceHelixAdmin admin: admins) {
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

    Assert.fail("No VeniceHelixAdmin became leader for cluster: " + cluster + " after timeout: " + timeout);
  }

  VeniceHelixAdmin getLeader(List<VeniceHelixAdmin> admins, String cluster) {
    for (VeniceHelixAdmin admin: admins) {
      if (admin.isLeaderControllerFor(cluster)) {
        return admin;
      }
    }
    throw new VeniceException("no leader found for cluster: " + cluster);
  }

  VeniceHelixAdmin getFollower(List<VeniceHelixAdmin> admins, String cluster) {
    for (VeniceHelixAdmin admin: admins) {
      if (!admin.isLeaderControllerFor(cluster)) {
        return admin;
      }
    }
    throw new VeniceException("no follower found for cluster: " + cluster);
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
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            veniceAdmin.getRealTimeTopic(clusterName, participantStoreName),
            Version.composeRealTimeTopic(participantStoreName)));
  }
}
