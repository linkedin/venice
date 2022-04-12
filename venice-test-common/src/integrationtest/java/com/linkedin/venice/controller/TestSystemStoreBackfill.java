package com.linkedin.venice.controller;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestSystemStoreBackfill {

  private static final int TEST_TIMEOUT = 60_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2; // DO NOT CHANGE
  private static final int NUMBER_OF_CLUSTERS = 1;

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;

  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        2,
        2,
        2,
        2);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBackfillingOfDavinciPushStatusStore() throws Exception {
    String clusterName = multiColoMultiClusterWrapper.getClusters().get(0).getClusterNames()[0];
    String testStoreName = Utils.getUniqueString("test-store");

    VeniceControllerWrapper parentController = parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());
    ControllerClient dc0Client =
        ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1Client =
        ControllerClient.constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());

    // store shouldn't exist
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(testStoreName);
      assertTrue(storeResponse.isError());
    });

    // Create a test store
    NewStoreResponse newStoreResponse = parentControllerClient.retryableRequest(5,
        c -> c.createNewStore(testStoreName, "test", "\"string\"", "\"string\""));
    assertFalse(newStoreResponse.isError(), "Test store creation failed - " + newStoreResponse.getError());

    verifySystemStoreStatus(parentControllerClient, "parentController" , testStoreName, false);
    verifySystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, false);
    verifySystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, false);

    String[] adminToolArgs = {
        "--url", parentControllerClient.getLeaderControllerUrl(),
        "--cluster", clusterName,
        "--backfill-system-stores",
        "--system-store-type", "davinci_push_status_store"
    };
    AdminTool.main(adminToolArgs);

    adminToolArgs = new String[] {
        "--url", parentControllerClient.getLeaderControllerUrl(),
        "--cluster", clusterName,
        "--backfill-system-stores",
        "--system-store-type", "meta_store"
    };
    AdminTool.main(adminToolArgs);

    verifySystemStoreStatus(parentControllerClient, "parentController" , testStoreName, true);
    verifySystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifySystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    verifyListStoreContainsSystemStores(parentControllerClient, "parentController" , testStoreName);
    verifyListStoreContainsSystemStores(dc0Client, "dc0ControllerClient", testStoreName);
    verifyListStoreContainsSystemStores(dc1Client, "dc1ControllerClient", testStoreName);
  }

  private void verifySystemStoreStatus(ControllerClient controllerClient, String clientName, String testStoreName, boolean isEnabled) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = controllerClient.getStore(testStoreName);
      assertFalse(storeResponse.isError());
      assertEquals(storeResponse.getStore().isDaVinciPushStatusStoreEnabled(), isEnabled, "Meta store not enabled. Controller: " + clientName);
      assertEquals(storeResponse.getStore().isStoreMetaSystemStoreEnabled(), isEnabled, "Push status store not enabled. Controller: " + clientName);
    });
  }

  private void verifyListStoreContainsSystemStores(ControllerClient controllerClient, String clientName, String testStoreName) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(testStoreName);
    String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStoreName);
    MultiStoreResponse multiStoreResponse = controllerClient.queryStoreList();
    assertFalse(multiStoreResponse.isError());
    Set<String> allStores = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    assertTrue(allStores.contains(metaStoreName), metaStoreName + " is not present in list store response with " + clientName);
    assertTrue(allStores.contains(pushStatusStoreName), pushStatusStoreName + " is not present in list store response with " + clientName);
  }
}
