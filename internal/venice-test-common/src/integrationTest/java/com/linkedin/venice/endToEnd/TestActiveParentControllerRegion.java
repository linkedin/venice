package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static com.linkedin.venice.utils.TestUtils.assertCommand;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
          "{} Parent Controller is in region {}",
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
    }
  }
}
