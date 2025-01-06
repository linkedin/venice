package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.AGGREGATE_REAL_TIME_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_REGION_STATE;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ZkCopier;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveParentControllerRegion {
  private static final Logger LOGGER = LogManager.getLogger(TestActiveParentControllerRegion.class);

  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(2)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentVeniceZkBasePath("/test-venice-parent")
            .childVeniceZkBasePath("/test-venice-child")
            .parentControllerInChildRegion(true)
            .build());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  /**
   * TODO: Rewrite this workflow summary
   * 1. Create two child regions with one ACTIVE parent controller in one region and one PASSIVE parent controller in the other region.
   * 2. Perform store operation and run VPJ jobs to both parent controllers.
   * 3. Verify ACTIVE parent controller successfully completes operations(createNewStore) and PASSIVE parent controller throws exception.
   * 4. Verify child controllers are updated.
   * 5. To imitate a region going down, we create a new ACTIVE parent controller in the PASSIVE parent controller region.
   * 6. Migrate metadata from old ACTIVE parent controller to the new ACTIVE parent controller
   * 7. Verify both parent controllers ZooKeeper data are synced.
   * 8. Perform store operations and run VPJ jobs to both parent controllers.
   * 9. Verify new ACTIVE parent controller successfully completes operation(getStore).
   * 10. Verify child controllers are updated.
   */
  @Test
  public void testActiveParentControllerRegionE2E() {
    List<VeniceControllerWrapper> parentControllers = venice.getParentControllers();
    List<VeniceMultiClusterWrapper> childDataCenters = venice.getChildRegions();

    Assert.assertEquals(parentControllers.size(), 2);
    Assert.assertEquals(childDataCenters.size(), 2);
    Assert.assertEquals(venice.getClusterNames().length, 1);
    Assert.assertEquals(childDataCenters.get(0).getClusters().size(), 1);
    Assert.assertEquals(childDataCenters.get(1).getClusters().size(), 1);

    String clusterName = venice.getClusterNames()[0];
    VeniceControllerWrapper activeParentController = parentControllers.get(0);
    VeniceControllerWrapper passiveParentController = parentControllers.get(1);
    Assert.assertTrue(activeParentController.getVeniceAdmin().isParent());
    Assert.assertTrue(passiveParentController.getVeniceAdmin().isParent());
    Assert.assertEquals(activeParentController.getVeniceAdmin().getParentControllerRegionState(), ACTIVE);
    Assert.assertEquals(passiveParentController.getVeniceAdmin().getParentControllerRegionState(), PASSIVE);
    for (VeniceControllerWrapper parentController: parentControllers) {
      LOGGER.info(
          "{} parent controller is in region {}",
          parentController.getVeniceAdmin().getParentControllerRegionState(),
          parentController.getVeniceAdmin().getRegionName()); // TODO fix in VeniceControllerWrapper
    }

    ZkServerWrapper dc0ZkServerWrapper = venice.getZkServerByRegionName().get("dc-0");
    ZkServerWrapper dc1ZkServerWrapper = venice.getZkServerByRegionName().get("dc-1");
    D2Client dc0D2Client = D2ClientFactory.getD2Client(dc0ZkServerWrapper.getAddress(), Optional.empty());
    D2Client dc1D2Client = D2ClientFactory.getD2Client(dc1ZkServerWrapper.getAddress(), Optional.empty());
    List<D2Client> d2Clients = new ArrayList<>();
    d2Clients.add(dc0D2Client);
    d2Clients.add(dc1D2Client);

    String serviceName = VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
    String activeParentControllerURL = activeParentController.getControllerUrl();
    String passiveParentControllerURL = passiveParentController.getControllerUrl();
    try (
        D2ControllerClient d2ControllerClient =
            new D2ControllerClient(serviceName, clusterName, d2Clients, Optional.empty());
        ControllerClient activeParentControllerClient = new ControllerClient(clusterName, activeParentControllerURL);
        ControllerClient passiveParentControllerClient = new ControllerClient(clusterName, passiveParentControllerURL);
        ControllerClient dc0ControllerClient =
            new ControllerClient(clusterName, childDataCenters.get(0).getControllerConnectString());
        ControllerClient dc1ControllerClient =
            new ControllerClient(clusterName, childDataCenters.get(1).getControllerConnectString())) {

      String d2ControllerClientLeaderControllerUrl = d2ControllerClient.getLeaderControllerUrl();
      LOGGER.info("D2 controller client leader controller url: " + d2ControllerClientLeaderControllerUrl);
      String activeParentControllerClientLeaderControllerUrl = activeParentControllerClient.getLeaderControllerUrl();
      LOGGER.info(
          "Active parent controller client leader controller url: " + activeParentControllerClientLeaderControllerUrl);
      Assert.assertEquals(d2ControllerClientLeaderControllerUrl, activeParentControllerClientLeaderControllerUrl);
      Assert.assertThrows(VeniceException.class, () -> {
        LOGGER.info(
            "Passive parent controller client leader controller url: "
                + passiveParentControllerClient.getLeaderControllerUrl());
      });
      LOGGER.info("DC0 controller client leader controller url: " + dc0ControllerClient.getLeaderControllerUrl());
      LOGGER.info("DC1 controller client leader controller url: " + dc1ControllerClient.getLeaderControllerUrl());

      String storeName = "test-store";
      String keySchemaStr = "\"string\"";
      String valueSchemaStr = "\"string\"";

      // D2 controller client should successfully complete operations (createNewStore, emptyPush)
      assertCommand(d2ControllerClient.createNewStore(storeName, "", keySchemaStr, valueSchemaStr));
      VersionCreationResponse d2ControllerClientVCR1 =
          assertCommand(d2ControllerClient.emptyPush(storeName, "test-push-1", 1L));
      Assert.assertEquals(d2ControllerClientVCR1.getVersion(), 1);

      // Child controllers should be updated
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = assertCommand(dc0ControllerClient.getStore(storeName));
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = assertCommand(dc1ControllerClient.getStore(storeName));
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });

      // D2 controller client should successfully complete operation (emptyPush)
      VersionCreationResponse d2ControllerClientVCR2 =
          assertCommand(d2ControllerClient.emptyPush(storeName, "test-push-2", 1L));
      Assert.assertEquals(d2ControllerClientVCR2.getVersion(), 2);

      // Child controllers should be updated
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = assertCommand(dc0ControllerClient.getStore(storeName));
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = assertCommand(dc1ControllerClient.getStore(storeName));
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });

      // TODO: Refactor VeniceTwoLayerMultiRegionMultiClusterWrapper to support creating parent controllers with certain
      // properties
      // Creating a new ACTIVE parent controller in the PASSIVE parent controller region (dc-1)
      String[] clusterNames = new String[] { clusterName };
      List<String> zkServerAddressesList =
          Arrays.asList(dc0ZkServerWrapper.getAddress(), dc1ZkServerWrapper.getAddress());
      PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();
      Map<String, String> clusterToD2 = Collections.singletonMap(clusterName, "venice-0");
      Map<String, String> clusterToServerD2 =
          Collections.singletonMap(clusterName, parentControllers.get(0).getServiceName());
      Properties props = new Properties();
      props.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      props.put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES, "dc-0");
      props.put(NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES, "dc-0");
      props.put(AGGREGATE_REAL_TIME_SOURCE_REGION, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
      props.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, "dc-0,dc-1," + DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
      props.setProperty(CONTROLLER_PARENT_REGION_STATE, ACTIVE.name());
      Map<String, String> pubSubBrokerProps =
          PubSubBrokerWrapper.getBrokerDetailsForClients(venice.getAllPubSubBrokerWrappers());
      props.putAll(pubSubBrokerProps);
      VeniceControllerWrapper[] childControllers = childDataCenters.stream()
          .map(VeniceMultiClusterWrapper::getRandomController)
          .toArray(VeniceControllerWrapper[]::new);
      VeniceControllerWrapper newActiveParentController = ServiceFactory.getVeniceController(
          new VeniceControllerCreateOptions.Builder(clusterNames, dc1ZkServerWrapper, pubSubBrokerWrapper)
              .multiRegion(true)
              .veniceZkBasePath("/test-venice-parent")
              .replicationFactor(1)
              .childControllers(childControllers)
              .extraProperties(props)
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .regionName("dc-1")
              .parentControllerInChildRegion(true)
              .zkServerAddressesList(zkServerAddressesList)
              .build());

      String oldActiveParentControllerZkAddress = activeParentController.getZkAddress();
      ZkClient oldActiveParentControllerZkClient = ZkClientFactory.newZkClient(oldActiveParentControllerZkAddress);

      // Shut down old parent controllers
      Utils.closeQuietlyWithErrorLogged(activeParentController);
      Utils.closeQuietlyWithErrorLogged(passiveParentController);

      LOGGER.info(
          "New ACTIVE parent controller is ready and its state is: "
              + newActiveParentController.getVeniceAdmin().getParentControllerRegionState());
      Assert.assertEquals(newActiveParentController.getVeniceAdmin().getParentControllerRegionState(), ACTIVE);

      String newActiveParentControllerURL = newActiveParentController.getControllerUrl();
      ControllerClient newActiveParentControllerClient =
          new ControllerClient(clusterName, newActiveParentControllerURL);
      D2ControllerClient newActiveD2ControllerClient =
          new D2ControllerClient(serviceName, clusterName, d2Clients, Optional.empty());

      String newActiveParentControllerClientLeaderControllerURL =
          newActiveParentControllerClient.getLeaderControllerUrl();
      String newActiveD2ControllerClientLeaderControllerUrl = newActiveD2ControllerClient.getLeaderControllerUrl();
      Assert.assertEquals(
          newActiveD2ControllerClientLeaderControllerUrl,
          newActiveParentControllerClientLeaderControllerURL);

      String newActiveParentControllerZkAddress = newActiveParentController.getZkAddress();
      ZkClient newActiveParentControllerZkClient = ZkClientFactory.newZkClient(newActiveParentControllerZkAddress);

      // TODO: change to AdminTool command
      ZkCopier.migrateVenicePaths(
          oldActiveParentControllerZkClient,
          newActiveParentControllerZkClient,
          Collections.singleton(clusterName),
          "/test-venice-parent");

      LOGGER.info(oldActiveParentControllerZkClient.getChildren("/test-venice-parent"));
      LOGGER.info(newActiveParentControllerZkClient.getChildren("/test-venice-parent"));

      // TODO: Investigate why these commands throws an exception.
      // // D2 controller client should successfully complete operation (getStore)
      // assertCommand(newActiveD2ControllerClient.getStore(storeName));
      //
      // // D2 controller client should not be able to create new store since the store already exists
      // assertCommand(newActiveD2ControllerClient.createNewStore(storeName, "", keySchemaStr, valueSchemaStr));
      //
      // // D2 controller client should successfully complete operation (emptyPush)
      // VersionCreationResponse newD2ControllerClientVCR1 =
      // assertCommand(newActiveD2ControllerClient.emptyPush(storeName, "test-push-3", 1L));
      // Assert.assertEquals(newD2ControllerClientVCR1.getVersion(), 3);
    }
  }
}
