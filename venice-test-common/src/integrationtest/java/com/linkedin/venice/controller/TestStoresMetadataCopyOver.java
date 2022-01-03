package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoresMetadataCopyOver {
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
  public void testStoresMetadataCopyOver() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");

    // Test the admin channel with the regular KMM pipeline
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    ControllerClient parentControllerClient = ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());

    ControllerClient dc0Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    // ControllerClient dc1Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());

    // Create a test store only in dc0 colo
    NewStoreResponse newStoreResponse = dc0Client.retryableRequest(3, c -> c.createNewStore(storeName,
        "", "\"string\"", "\"string\""));
    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());
    checkStoreConfig(dc0Client, storeName);

    // Call metadata copy over to copy dc0's store configs to dc1
    parentControllerClient.copyOverStoresMetadata("dc-0","dc-1");
    ControllerClient dc1Client = ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    System.out.println("The dc0 controller url is " + childDatacenters.get(0).getControllerConnectString());
    System.out.println("The dc1 controller url is " + childDatacenters.get(1).getControllerConnectString());
    checkStoreConfig(dc1Client, storeName);
  }

  private static void checkStoreConfig(ControllerClient childControllerClient,
      String storeName) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      Assert.assertNotNull(storeInfo);
    });
  }
}
