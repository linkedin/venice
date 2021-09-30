package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestAdminTopicRemoteConsumption {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1);

    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAdminTopicRemoteConsumptionFeature() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = TestUtils.getUniqueString("store");

    // Test the admin channel with the regular KMM pipeline
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    ControllerClient parentControllerClient = ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());

    // Create a test store
    NewStoreResponse newStoreResponse = parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName,
        "", "\"string\"", "\"string\""));
    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    // Send UpdateStore to parent controller to update the state model config
    UpdateStoreQueryParams updateStoreParamsOnParent = new UpdateStoreQueryParams().setLeaderFollowerModel(true);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParamsOnParent);

    // The new store config should be reflected in the child region
    ControllerClient dc0Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    checkStoreConfig(dc0Client, parentControllerClient, storeName, true);

    // Replace the child controller in DC0 region with the new config to enable admin topic remote consumption
    VeniceClusterWrapper clusterInDc0 = childDatacenters.get(0).getClusters().get(clusterName);
    VeniceControllerWrapper addedControllerWrapper = null, originalControllerWrapper = null;
    List<VeniceControllerWrapper> controllers = clusterInDc0.getVeniceControllers();
    Assert.assertEquals(controllers.size(), 1, "There should only be one controller");
    originalControllerWrapper = controllers.get(0);

    // Create a controller with admin topic remote consumption enabled, and remove the previous controller
    KafkaBrokerWrapper parentKafka = multiColoMultiClusterWrapper.getParentKafkaBrokerWrapper();
    Properties controllerProps = new Properties();
    String adminTopicSourceRegion = "parent";
    controllerProps.put(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, "true");
    controllerProps.put(ADMIN_TOPIC_SOURCE_REGION, adminTopicSourceRegion);
    controllerProps.put(NATIVE_REPLICATION_FABRIC_WHITELIST, adminTopicSourceRegion);
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + adminTopicSourceRegion, parentKafka.getAddress());
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + adminTopicSourceRegion, parentKafka.getZkAddress());
    addedControllerWrapper = clusterInDc0.addVeniceController(controllerProps);
    List<VeniceControllerWrapper> newControllers = clusterInDc0.getVeniceControllers();
    Assert.assertEquals(newControllers.size(), 2, "There should be two controllers now");
    // Shutdown the original controller, now there is only one controller with config: admin topic remote consumption enabled.
    clusterInDc0.stopVeniceController(originalControllerWrapper.getPort());

    // Build a new controller client with the new child controller
    dc0Client.close();
    ControllerClient newDc0Client = ControllerClient.constructClusterControllerClient(clusterName, addedControllerWrapper.getControllerUrl());
    // Before sending any new admin messages, the L/F config should remain true
    checkStoreConfig(newDc0Client, parentControllerClient, storeName, true);

    // Turn off L/F config with the new admin channel
    updateStoreParamsOnParent = new UpdateStoreQueryParams().setLeaderFollowerModel(false);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParamsOnParent);
    checkStoreConfig(newDc0Client, parentControllerClient, storeName, false);
  }

  private static void checkStoreConfig(ControllerClient childControllerClient, ControllerClient parentControllerClient,
      String storeName, boolean expectedLeaderFollowerConfig) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      Assert.assertEquals(storeInfo.isLeaderFollowerModelEnabled(), expectedLeaderFollowerConfig);

      // The store config should be updated in parent too
      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      storeInfo = storeResponse.getStore();
      Assert.assertEquals(storeInfo.isLeaderFollowerModelEnabled(), expectedLeaderFollowerConfig);
    });
  }
}
