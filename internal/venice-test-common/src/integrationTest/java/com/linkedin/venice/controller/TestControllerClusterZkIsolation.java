package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_CLUSTER_ZK_ADDRESSS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Proves that a Venice controller can be pointed at TWO separate ZooKeeper ensembles:
 * <ul>
 *   <li>{@code zookeeper.address} (storage ZK) – holds the Venice storage-cluster Helix metadata
 *       (storage cluster, its resources, store/version znodes).</li>
 *   <li>{@code controller.cluster.zk.address} (controller ZK) – holds the controller-cluster Helix
 *       metadata (the {@code venice-controllers} cluster: leader election, participant registration,
 *       storage-cluster-to-controller assignment) and the HAAS grand cluster.</li>
 * </ul>
 *
 * This is the "work-backwards" integration test Xun asked for: it runs the controller bring-up once
 * with a SHARED ZK (the default/aliasing path) and once with SPLIT ZK, and asserts via a
 * ZkInspector-style probe ({@link ZKHelixAdmin#getClusters()} against each ensemble) that the
 * controller-cluster znodes and the storage-cluster znodes land on the expected — and only the
 * expected — ensemble.
 *
 * The split-ZK branch in {@link ZkHelixAdminClient} and {@code connectToControllerCluster} in
 * {@link VeniceHelixAdmin} is exercised here for the first time; with
 * {@code controller.cluster.zk.address} defaulting to {@code zookeeper.address}, no existing test
 * ever drives the two addresses apart.
 */
public class TestControllerClusterZkIsolation {
  private static final long TIMEOUT_MS = 60 * Time.MS_PER_SECOND;

  private ZkServerWrapper storageZk;
  private ZkServerWrapper controllerZk;
  private PubSubBrokerWrapper pubSubBrokerWrapper;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private VeniceHelixAdmin veniceAdmin;
  private String clusterName;
  private String controllerClusterName;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    storageZk = ServiceFactory.getZkServer();
    controllerZk = ServiceFactory.getZkServer();
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownAdmin() {
    Utils.closeQuietlyWithErrorLogged(veniceAdmin);
    veniceAdmin = null;
    Utils.closeQuietlyWithErrorLogged(pubSubBrokerWrapper);
    Utils.closeQuietlyWithErrorLogged(controllerZk);
    Utils.closeQuietlyWithErrorLogged(storageZk);
  }

  /**
   * SPLIT ZK: controller-cluster Helix metadata must live ONLY on the controller ZK, and the
   * storage-cluster Helix metadata must live ONLY on the storage ZK.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testControllerClusterIsolatedFromStorageZk() {
    bringUpController(/* splitZk */ true);

    // Create a store so the storage cluster has real Venice metadata on the storage ZK.
    String storeName = Utils.getUniqueString("isolation-store");
    veniceAdmin.createStore(clusterName, storeName, "owner", "\"string\"", "\"string\"");
    assertNotNull(veniceAdmin.getStore(clusterName, storeName), "Store should have been created");

    List<String> clustersOnControllerZk = listHelixClusters(controllerZk.getAddress());
    List<String> clustersOnStorageZk = listHelixClusters(storageZk.getAddress());

    // Controller ZK owns the controller cluster, NOT the storage cluster.
    assertTrue(
        clustersOnControllerZk.contains(controllerClusterName),
        "Controller ZK " + controllerZk.getAddress() + " must hold the controller cluster '" + controllerClusterName
            + "', found: " + clustersOnControllerZk);
    assertFalse(
        clustersOnControllerZk.contains(clusterName),
        "Controller ZK must NOT hold the storage cluster '" + clusterName + "', found: " + clustersOnControllerZk);

    // Storage ZK owns the storage cluster, NOT the controller cluster.
    assertTrue(
        clustersOnStorageZk.contains(clusterName),
        "Storage ZK " + storageZk.getAddress() + " must hold the storage cluster '" + clusterName + "', found: "
            + clustersOnStorageZk);
    assertFalse(
        clustersOnStorageZk.contains(controllerClusterName),
        "Storage ZK must NOT hold the controller cluster '" + controllerClusterName + "', found: "
            + clustersOnStorageZk);
  }

  /**
   * SHARED ZK baseline (the default/aliasing path): with {@code controller.cluster.zk.address} ==
   * {@code zookeeper.address}, BOTH clusters land on the single ZK and the second ZK stays empty of
   * Venice clusters. Guards against the split-ZK change regressing the common deployment.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testSharedZkBaseline() {
    bringUpController(/* splitZk */ false);

    List<String> clustersOnStorageZk = listHelixClusters(storageZk.getAddress());
    List<String> clustersOnControllerZk = listHelixClusters(controllerZk.getAddress());

    assertTrue(
        clustersOnStorageZk.contains(controllerClusterName) && clustersOnStorageZk.contains(clusterName),
        "With shared ZK, both controller and storage clusters must live on the single ZK, found: "
            + clustersOnStorageZk);
    assertFalse(
        clustersOnControllerZk.contains(controllerClusterName) || clustersOnControllerZk.contains(clusterName),
        "The unused second ZK must not host any Venice cluster in the shared-ZK case, found: "
            + clustersOnControllerZk);
  }

  /** Builds and starts a real VeniceHelixAdmin (controller) wired to one or two ZK ensembles. */
  private void bringUpController(boolean splitZk) {
    clusterName = Utils.getUniqueString("test-cluster");

    Properties props = TestUtils.getPropertiesForControllerConfig();
    props.put(ZOOKEEPER_ADDRESS, storageZk.getAddress());
    if (splitZk) {
      props.put(CONTROLLER_CLUSTER_ZK_ADDRESSS, controllerZk.getAddress());
    }
    props.put(CLUSTER_NAME, clusterName);
    props.put(KAFKA_REPLICATION_FACTOR, 1);
    props.put(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    props.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 16);
    props.put(DEFAULT_PARTITION_SIZE, 10);
    props.put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_d2")));
    props.put(
        CLUSTER_TO_SERVER_D2,
        TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_server_d2")));
    props.put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true);
    props.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, false);
    props.put(PARTICIPANT_MESSAGE_STORE_ENABLED, true);
    props.put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterName);
    props.put(CONTROLLER_SSL_ENABLED, false);
    props.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));

    VeniceControllerClusterConfig controllerConfig = new VeniceControllerClusterConfig(new VeniceProperties(props));
    VeniceControllerMultiClusterConfig multiClusterConfig =
        TestUtils.getMultiClusterConfigFromOneCluster(controllerConfig);
    controllerClusterName = multiClusterConfig.getControllerClusterName();

    veniceAdmin = new VeniceHelixAdmin(
        multiClusterConfig,
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(storageZk.getAddress()),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory(),
        pubSubBrokerWrapper.getPubSubPositionTypeRegistry(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    veniceAdmin.initStorageCluster(clusterName);

    TestUtils.waitForNonDeterministicCompletion(
        TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.isLeaderControllerFor(clusterName));
  }

  /** ZkInspector-style probe: which Helix clusters physically exist on the given ZK ensemble. */
  private static List<String> listHelixClusters(String zkAddress) {
    ZKHelixAdmin admin = new ZKHelixAdmin(zkAddress);
    try {
      return admin.getClusters();
    } finally {
      admin.close();
    }
  }
}
