package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AdminToolBackfillTest {
  private static final int TEST_TIMEOUT = 300_000; // empty push on push status store takes a long time to finish

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2; // DO NOT CHANGE
  private static final int NUMBER_OF_CLUSTERS = 1;

  private String[] clusterNames;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  @BeforeClass
  public void setUp() {
    // Disable auto materialization here as we need to test the back-fill command.
    Properties parentControllerProperties = new Properties();
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        2,
        2,
        2,
        2,
        1,
        Optional.of(new VeniceProperties(parentControllerProperties)),
        Optional.empty(),
        Optional.empty(),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    clusterNames = multiRegionMultiClusterWrapper.getClusterNames();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMetaSystemStoreBackfill() throws Exception {
    String clusterName = clusterNames[0];
    String testStoreName = Utils.getUniqueString("test-store");

    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls);
    ControllerClient dc0Client = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1Client = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());

    // store shouldn't exist
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(testStoreName);
      assertTrue(storeResponse.isError());
    });

    // Create a test store
    NewStoreResponse newStoreResponse = parentControllerClient
        .retryableRequest(5, c -> c.createNewStore(testStoreName, "test", "\"string\"", "\"string\""));
    assertFalse(newStoreResponse.isError(), "Test store creation failed - " + newStoreResponse.getError());

    verifyMetaSystemStoreStatus(parentControllerClient, "parentController", testStoreName, false);
    verifyMetaSystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, false);
    verifyMetaSystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, false);

    String[] adminToolArgs = { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--backfill-system-stores", "--system-store-type", "meta_store" };
    AdminTool.main(adminToolArgs);

    verifyMetaSystemStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyMetaSystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyMetaSystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    verifyListStoreContainsMetaSystemStore(parentControllerClient, "parentController", testStoreName);
    verifyListStoreContainsMetaSystemStore(dc0Client, "dc0ControllerClient", testStoreName);
    verifyListStoreContainsMetaSystemStore(dc1Client, "dc1ControllerClient", testStoreName);

    /* Test - disable meta store */

    // try some random update store operation, and it shouldn't disable meta store
    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--bootstrap-to-online-timeout", "1", };
    AdminTool.main(adminToolArgs);

    verifyMetaSystemStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyMetaSystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyMetaSystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    // replicate all configs shouldn't disable davinci push status store
    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--replicate-all-configs", "true" };
    AdminTool.main(adminToolArgs);

    verifyMetaSystemStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyMetaSystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyMetaSystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--disable-meta-store", };
    AdminTool.main(adminToolArgs);

    verifyMetaSystemStoreStatus(parentControllerClient, "parentController", testStoreName, false);
    verifyMetaSystemStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, false);
    verifyMetaSystemStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPushStatusStoreBackfill() throws Exception {
    String clusterName = clusterNames[0];
    String testStoreName = Utils.getUniqueString("test-store");

    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls);
    ControllerClient dc0Client = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1Client = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());

    // store shouldn't exist
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(testStoreName);
      assertTrue(storeResponse.isError());
    });

    // Create a test store
    NewStoreResponse newStoreResponse = parentControllerClient
        .retryableRequest(5, c -> c.createNewStore(testStoreName, "test", "\"string\"", "\"string\""));
    assertFalse(newStoreResponse.isError(), "Test store creation failed - " + newStoreResponse.getError());

    verifyPushStatusStoreStatus(parentControllerClient, "parentController", testStoreName, false);
    verifyPushStatusStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, false);
    verifyPushStatusStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, false);

    String[] adminToolArgs = { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--backfill-system-stores", "--system-store-type", "davinci_push_status_store" };
    AdminTool.main(adminToolArgs);

    verifyPushStatusStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyPushStatusStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyPushStatusStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    verifyListStoreContainsPushStatusStore(parentControllerClient, "parentController", testStoreName);
    verifyListStoreContainsPushStatusStore(dc0Client, "dc0ControllerClient", testStoreName);
    verifyListStoreContainsPushStatusStore(dc1Client, "dc1ControllerClient", testStoreName);

    /* Test - disable push status store */

    // try some random update store operation, and it shouldn't disable davinci push status store
    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--bootstrap-to-online-timeout", "1", };
    AdminTool.main(adminToolArgs);

    verifyPushStatusStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyPushStatusStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyPushStatusStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    // replicate all configs shouldn't disable davinci push status store
    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--replicate-all-configs", "true" };
    AdminTool.main(adminToolArgs);

    verifyPushStatusStoreStatus(parentControllerClient, "parentController", testStoreName, true);
    verifyPushStatusStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, true);
    verifyPushStatusStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, true);

    adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
        "--store", testStoreName, "--update-store", "--disable-davinci-push-status-store", };
    AdminTool.main(adminToolArgs);

    verifyPushStatusStoreStatus(parentControllerClient, "parentController", testStoreName, false);
    verifyPushStatusStoreStatus(dc0Client, "dc0ControllerClient", testStoreName, false);
    verifyPushStatusStoreStatus(dc1Client, "dc1ControllerClient", testStoreName, false);
  }

  private void verifyMetaSystemStoreStatus(
      ControllerClient controllerClient,
      String clientName,
      String testStoreName,
      boolean isEnabled) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = controllerClient.getStore(testStoreName);
      assertFalse(storeResponse.isError());
      assertEquals(
          storeResponse.getStore().isStoreMetaSystemStoreEnabled(),
          isEnabled,
          "Meta store is not " + (isEnabled ? "enabled" : "disabled") + ". Controller: " + clientName);
    });
  }

  private void verifyPushStatusStoreStatus(
      ControllerClient controllerClient,
      String clientName,
      String testStoreName,
      boolean isEnabled) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = controllerClient.getStore(testStoreName);
      assertFalse(storeResponse.isError());
      assertEquals(
          storeResponse.getStore().isDaVinciPushStatusStoreEnabled(),
          isEnabled,
          "Push status store is not " + (isEnabled ? "enabled" : "disabled") + ". Controller: " + clientName);
    });
  }

  private void verifyListStoreContainsMetaSystemStore(
      ControllerClient controllerClient,
      String clientName,
      String testStoreName) {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(testStoreName);
    MultiStoreResponse multiStoreResponse = controllerClient.queryStoreList();
    assertFalse(multiStoreResponse.isError());
    Set<String> allStores = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    assertTrue(
        allStores.contains(metaStoreName),
        metaStoreName + " is not present in list store response with " + clientName);
  }

  private void verifyListStoreContainsPushStatusStore(
      ControllerClient controllerClient,
      String clientName,
      String testStoreName) {
    String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(testStoreName);
    MultiStoreResponse multiStoreResponse = controllerClient.queryStoreList();
    assertFalse(multiStoreResponse.isError());
    Set<String> allStores = new HashSet<>(Arrays.asList(multiStoreResponse.getStores()));
    assertTrue(
        allStores.contains(pushStatusStoreName),
        pushStatusStoreName + " is not present in list store response with " + clientName);
  }
}
