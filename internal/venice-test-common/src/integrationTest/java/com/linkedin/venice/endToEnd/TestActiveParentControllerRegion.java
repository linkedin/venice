package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.controller.ParentControllerRegionState.*;

import com.linkedin.venice.ZkCopier;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.serializer.ByteArraySerializer;
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
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentControllerInChildRegion(true)
            .build());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  /**
   * 1. Create two child regions with one ACTIVE parent controller in one region and one PASSIVE parent controller in the other region.
   * 2. Perform store operation and VPJ job to both parent controllers.
   * 3. Verify ACTIVE parent controller successfully completes operations and PASSIVE parent controller throws exception.
   * 4. Verify child controllers are updated.
   * 5. To imitate a region going down, we switch ACTIVE and PASSIVE states for both parent controllers
   * 6. Migrate metadata from old ACTIVE parent controller to the new ACTIVE parent controller
   * 7. Verify both parent controllers are synced.
   * 8. Perform new operation(i.e., createStore) to both parent controllers.
   * 9. Verify new ACTIVE parent controller successfully completes operation(getStore) and new PASSIVE parent controller throws exception.
   * 10. Verify child controllers are updated
   */
  @Test
  public void testActiveParentControllerRegion() {
    List<VeniceControllerWrapper> parentControllers = venice.getParentControllers();
    List<VeniceMultiClusterWrapper> childDatacenters = venice.getChildRegions();
    String clusterName = venice.getClusterNames()[0];
    Assert.assertEquals(parentControllers.size(), 2);
    Assert.assertEquals(childDatacenters.size(), 2);
    Assert.assertEquals(venice.getClusterNames().length, 1);
    Assert.assertEquals(childDatacenters.get(0).getClusters().size(), 1);
    VeniceControllerWrapper parentController0 = parentControllers.get(0);
    VeniceControllerWrapper parentController1 = parentControllers.get(1);
    for (VeniceControllerWrapper parentController: parentControllers) {
      LOGGER.info(
          "Parent controller {} is in region {}",
          parentController.getVeniceAdmin().getParentControllerRegionState(),
          parentController.getVeniceAdmin().getRegionName());
    }
    Assert.assertTrue(parentController0.getVeniceAdmin().isParent());
    Assert.assertTrue(parentController1.getVeniceAdmin().isParent());
    Assert.assertEquals(parentController0.getVeniceAdmin().getParentControllerRegionState(), ACTIVE);
    Assert.assertEquals(parentController1.getVeniceAdmin().getParentControllerRegionState(), PASSIVE);

    String parentControllerURL0 = parentController0.getControllerUrl();
    String parentControllerURL1 = parentController1.getControllerUrl();
    try (ControllerClient parentControllerClient0 = new ControllerClient(clusterName, parentControllerURL0);
        ControllerClient parentControllerClient1 = new ControllerClient(clusterName, parentControllerURL1);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      LOGGER.info("Parent0 call leader controller " + parentControllerClient0.getLeaderControllerUrl());
      try {
        LOGGER.info("Parent1 call leader controller " + parentControllerClient1.getLeaderControllerUrl());
        Assert.fail("Parent1 should not be able to call leader controller");
      } catch (Exception e) {
        LOGGER.info("Parent1 call leader controller failed as expected");
      }
      LOGGER.info("dc0 call leader controller " + dc0Client.getLeaderControllerUrl());
      LOGGER.info("dc1 call leader controller" + dc1Client.getLeaderControllerUrl());

      // steps 2 + 3
      String storeName = "test-store";
      String keySchemaStr = "\"string\"";
      String valueSchemaStr = "\"string\"";

      LOGGER.info("Creating new store in parent controller 0");
      NewStoreResponse parentController0NewStoreResponse =
          parentControllerClient0.createNewStore(storeName, "", keySchemaStr, valueSchemaStr);
      Assert.assertFalse(parentController0NewStoreResponse.isError());

      LOGGER.info("Error creating new store in parent controller 1");
      NewStoreResponse parentController1NewStoreResponse =
          parentControllerClient1.createNewStore(storeName, "", keySchemaStr, valueSchemaStr);
      Assert.assertTrue(parentController1NewStoreResponse.isError());

      LOGGER.info("Empty push to parent controller 0");
      VersionCreationResponse parentController0VersionCreationResponse =
          parentControllerClient0.emptyPush(storeName, "test-push-1", 1L);
      Assert.assertFalse(parentController0VersionCreationResponse.isError());
      Assert.assertEquals(parentController0VersionCreationResponse.getVersion(), 1);

      LOGGER.info("Error empty push to parent controller 1");
      VersionCreationResponse parentController1VersionCreationResponse =
          parentControllerClient1.emptyPush(storeName, "test-push-1", 1L);
      Assert.assertTrue(parentController1VersionCreationResponse.isError());
      // step 4
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc0Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc1Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
      });

      LOGGER.info("Empty push to parent controller 0");
      VersionCreationResponse parentController0VersionCreationResponseRepush =
          parentControllerClient0.emptyPush(storeName, "test-push-2", 1L);
      Assert.assertFalse(parentController0VersionCreationResponseRepush.isError());
      Assert.assertEquals(parentController0VersionCreationResponseRepush.getVersion(), 2);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc0Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = dc1Client.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 2);
      });

      String parentController0ZkAddress = parentController0.getZkAddress();
      String parentController1ZkAddress = parentController1.getZkAddress();
      // parentController0ZkAddress = parentController0ZkAddress.substring(0,
      // parentController0ZkAddress.lastIndexOf('/'));
      // parentController1ZkAddress = parentController1ZkAddress.substring(0,
      // parentController1ZkAddress.lastIndexOf('/'));
      ZkClient parent0ZkClient = ZkClientFactory.newZkClient(parentController0ZkAddress);
      ZkClient parent1ZkClient = ZkClientFactory.newZkClient(parentController1ZkAddress);

      String parentController1RegionName = parentController1.getVeniceAdmin().getRegionName();
      String[] clusterNames = new String[] { clusterName };
      ZkServerWrapper zkServerWrapper = venice.getZkServerByRegionName().get("dc-parent-0dc-1");
      PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();
      VeniceControllerWrapper[] childControllers = childDatacenters.stream()
          .map(VeniceMultiClusterWrapper::getRandomController)
          .toArray(VeniceControllerWrapper[]::new);
      Map<String, String> clusterToD2 = Collections.singletonMap(clusterName, "venice-0");
      Map<String, String> clusterToServerD2 =
          Collections.singletonMap(clusterName, parentController0.getVeniceAdmin().getServerD2Service(clusterName));
      Properties props = new Properties();
      props.put(CONTROLLER_PARENT_REGION_STATE, ACTIVE);
      props.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
      VeniceControllerWrapper newActiveParentController = ServiceFactory.getVeniceController(
          new VeniceControllerCreateOptions.Builder(clusterNames, zkServerWrapper, pubSubBrokerWrapper)
              .multiRegion(true)
              .replicationFactor(1)
              .childControllers(childControllers)
              .extraProperties(props)
              .clusterToD2(clusterToD2)
              .clusterToServerD2(clusterToServerD2)
              .regionName("dc1")
              .build());

      // kill parent controllers and zkservers
      Utils.closeQuietlyWithErrorLogged(parentController0);
      Utils.closeQuietlyWithErrorLogged(parentController1);
      // TODO: kill zkservers

      LOGGER.info(
          "new active parent controller is ready"
              + newActiveParentController.getVeniceAdmin().getParentControllerRegionState());
      ControllerClient newActiveParentControllerClient =
          new ControllerClient(clusterName, newActiveParentController.getControllerUrl());

      String newActiveParentControllerZkAddress = newActiveParentController.getZkAddress();
      // newActiveParentControllerZkAddress = newActiveParentControllerZkAddress.substring(0,
      // newActiveParentControllerZkAddress.lastIndexOf('/'));
      ZkClient newActiveParentZkClient = ZkClientFactory.newZkClient(newActiveParentControllerZkAddress);
      newActiveParentZkClient.setZkSerializer(new ByteArraySerializer());
      parent0ZkClient.setZkSerializer(new ByteArraySerializer());
      LOGGER.info("WWWW" + newActiveParentZkClient.getChildren("/"));
      // TODO: change to admintool
      ZkCopier
          .migrateVenicePaths(parent0ZkClient, newActiveParentZkClient, Collections.singleton("venice-cluster0"), "/");
    }
  }
}
