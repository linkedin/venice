package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.HelixAsAServiceWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.testng.annotations.Test;


/**
 * Production-shaped ZK isolation test using {@link HelixAsAServiceWrapper} — the same setup
 * pattern as {@link TestHAASController}.
 *
 * <p>{@link TestHAASController} already covers the shared-ZK case (HaaS and Venice on the same
 * ZK). This class covers the complementary split-ZK case: {@link HelixAsAServiceWrapper} is
 * started on a DEDICATED controller ZK and the Venice controller is configured with
 * {@code controller.cluster.zk.address = <controller ZK>}. The assertions are exactly what a
 * human would see by opening a ZK inspector against each ensemble: the controller ZK hosts only
 * the HAAS super cluster + {@code venice-controllers}, while the Venice ZK hosts only the storage
 * cluster.
 */
public class TestVeniceControllerZkIsolationWithHAAS {
  private static final String CONTROLLER_CLUSTER_NAME = "venice-controllers";

  /**
   * SPLIT ZK (isolation): {@link HelixAsAServiceWrapper} runs on a dedicated controller ZK,
   * separate from the Venice storage ZK. The controller is configured with
   * {@code controller.cluster.zk.address = <controller ZK>}. After the controller becomes leader:
   * <ul>
   *   <li>The controller ZK must hold the HAAS super cluster and the {@code venice-controllers}
   *       controller cluster — but NOT the storage cluster.</li>
   *   <li>The Venice ZK must hold only the storage cluster — but NOT the controller/super
   *       clusters.</li>
   * </ul>
   * This is the "ZK inspector" assertion Xun described: open two ZK inspectors, confirm each
   * ensemble holds exactly the clusters it should.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testControllerClusterAndSuperClusterIsolatedOnDedicatedZk() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();

    // Dedicated ZK for the controller cluster + HAAS super cluster, separate from the Venice cluster ZK.
    try (ZkServerWrapper controllerZk = ServiceFactory.getZkServer();
        VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper haas = startAndWaitForHAASToBeAvailable(controllerZk.getAddress())) {

      String storageZkAddress = venice.getZk().getAddress();
      String storageClusterName = venice.getClusterName();

      Properties controllerProps = new Properties();
      controllerProps.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
      // Required alongside CONTROLLER_CLUSTER_LEADER_HAAS so that initStorageCluster() calls
      // setupStorageClusterAsNeeded() and adds the storage cluster as a resource in the controller
      // cluster. Without this, waitUntilClusterResourceIsVisibleInEV() never sees the ExternalView
      // and blocks for up to 5 minutes before the test timeout fires.
      controllerProps.put(ConfigKeys.VENICE_STORAGE_CLUSTER_LEADER_HAAS, String.valueOf(true));
      controllerProps
          .put(ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME, HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME);
      // The crux: route controller-cluster + HAAS grand-cluster Helix ops/participant join to the dedicated ZK.
      controllerProps.put(ConfigKeys.CONTROLLER_CLUSTER_ZK_ADDRESSS, controllerZk.getAddress());

      VeniceControllerWrapper controller = venice.addVeniceController(controllerProps);

      // Controller becomes leader of both its controller cluster (via HAAS) and the storage cluster.
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              controller.isLeaderControllerOfControllerCluster(),
              "The controller should be the leader of the controller cluster"));
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              controller.isLeaderController(storageClusterName),
              "The controller should be the leader of storage cluster " + storageClusterName));

      // Create a store so the storage cluster carries real Venice metadata on the Venice ZK.
      String storeName = Utils.getUniqueString("haas-isolation-store");
      assertNotNull(venice.getNewStore(storeName), "Store should have been created");

      assertNotNull(haas.getSuperClusterLeader(), "HAAS super cluster should have a leader");

      List<String> clustersOnControllerZk = listHelixClusters(controllerZk.getAddress());
      List<String> clustersOnStorageZk = listHelixClusters(storageZkAddress);

      // Controller ZK: super cluster + controller cluster, and NOT the storage cluster.
      assertTrue(
          clustersOnControllerZk.contains(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME),
          "Controller ZK must host the HAAS super cluster, found: " + clustersOnControllerZk);
      assertTrue(
          clustersOnControllerZk.contains(CONTROLLER_CLUSTER_NAME),
          "Controller ZK must host the controller cluster '" + CONTROLLER_CLUSTER_NAME + "', found: "
              + clustersOnControllerZk);
      assertFalse(
          clustersOnControllerZk.contains(storageClusterName),
          "Controller ZK must NOT host the storage cluster '" + storageClusterName + "', found: "
              + clustersOnControllerZk);

      // Venice ZK: storage cluster only, and NOT the controller/super clusters.
      assertTrue(
          clustersOnStorageZk.contains(storageClusterName),
          "Venice ZK must host the storage cluster '" + storageClusterName + "', found: " + clustersOnStorageZk);
      assertFalse(
          clustersOnStorageZk.contains(CONTROLLER_CLUSTER_NAME)
              || clustersOnStorageZk.contains(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME),
          "Venice ZK must NOT host the controller/super clusters, found: " + clustersOnStorageZk);
    }
  }

  private HelixAsAServiceWrapper startAndWaitForHAASToBeAvailable(String zkAddress) {
    HelixAsAServiceWrapper haas = null;
    try {
      haas = ServiceFactory.getHelixController(zkAddress);
      final HelixAsAServiceWrapper finalHaas = haas;
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertNotNull(finalHaas.getSuperClusterLeader(), "Helix super cluster doesn't have a leader yet"));
      return haas;
    } catch (Exception e) {
      Utils.closeQuietlyWithErrorLogged(haas);
      throw e;
    }
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
