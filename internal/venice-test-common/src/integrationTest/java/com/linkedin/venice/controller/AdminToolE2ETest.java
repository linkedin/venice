package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.cli.AlreadySelectedException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AdminToolE2ETest {
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

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(2)
            .numberOfChildControllers(2)
            .numberOfServers(2)
            .numberOfRouters(2)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(parentControllerProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStoreMigrationStatus() throws Exception {
    String storeName = Utils.getUniqueString("testUpdateStoreMigrationStatus");
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String clusterName = clusterNames[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      TestUtils.assertCommand(
          parentControllerClient
              .retryableRequest(5, c -> c.createNewStore(storeName, "test", "\"string\"", "\"string\"")));

      // Ensure store migration status is false
      validateStoreMigrationStatus(parentControllerClient, storeName, false, "parentController");
      validateStoreMigrationStatusAcrossChildRegions(storeName, clusterName, false);

      // Update store migration status to true
      String[] adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster",
          clusterName, "--store", storeName, "--update-store", "--enable-store-migration", "true" };
      AdminTool.main(adminToolArgs);
      validateStoreMigrationStatus(parentControllerClient, storeName, true, "parentController");
      validateStoreMigrationStatusAcrossChildRegions(storeName, clusterName, true);

      // Set back status to false and validate
      adminToolArgs = new String[] { "--url", parentControllerClient.getLeaderControllerUrl(), "--cluster", clusterName,
          "--store", storeName, "--update-store", "--enable-store-migration", "false" };
      AdminTool.main(adminToolArgs);
      validateStoreMigrationStatus(parentControllerClient, storeName, false, "parentController");
      validateStoreMigrationStatusAcrossChildRegions(storeName, clusterName, false);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCleanExecutionIds() throws Exception {
    String storeName1 = Utils.getUniqueString("testCleanExecutionIds-1");
    String storeName2 = Utils.getUniqueString("testCleanExecutionIds-2");
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String clusterName = clusterNames[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    ControllerClient childControllerClient = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());

    createStore(parentControllerClient, childControllerClient, storeName1);
    createStore(parentControllerClient, childControllerClient, storeName2);

    String[] adminToolArgs = new String[] { "--url", childControllerClient.getLeaderControllerUrl(), "--cluster",
        clusterName, "--clean-execution-ids" };

    AdminTool.main(adminToolArgs);

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setEnableReads(false).setEnableWrites(false);
    TestWriteUtils.updateStore(storeName1, parentControllerClient, updateStoreParams);
    parentControllerClient.deleteStore(storeName1);
    ControllerResponse deleteStoreResponse = parentControllerClient.retryableRequest(5, c -> c.deleteStore(storeName1));
    Assert.assertFalse(
        deleteStoreResponse.isError(),
        "The DeleteStoreResponse returned an error: " + deleteStoreResponse.getError());

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse getStoreResponse = childControllerClient.getStore(storeName1);
      assertEquals(getStoreResponse.getErrorType(), ErrorType.STORE_NOT_FOUND);
    });

    AdminTool.main(adminToolArgs);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "skipAdminMessageOptions")
  public void testSkipAdminMessage(String[] extraArgs, boolean expectFailure) throws Exception {
    String storeName1 = Utils.getUniqueString("testSkipAdminMessage");
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String clusterName = clusterNames[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    ControllerClient childControllerClient = ControllerClient
        .constructClusterControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());

    createStore(parentControllerClient, childControllerClient, storeName1);

    // Build full args: base + data-driven extras
    List<String> args = new ArrayList<>();
    args.add("--url");
    args.add(childControllerClient.getLeaderControllerUrl());
    args.add("--cluster");
    args.add(clusterName);
    args.add("--skip-admin-message");
    args.addAll(Arrays.asList(extraArgs));
    String[] adminToolArgs = args.toArray(new String[0]);

    if (expectFailure) {
      try {
        AdminTool.main(adminToolArgs);
        fail("Expected failure for args: " + java.util.Arrays.toString(adminToolArgs));
      } catch (RuntimeException e) {
        // expected
      }
    } else {
      AdminTool.main(adminToolArgs);
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dumpAdminMessageOptions")
  public void testDumpAdminMessage(String[] extraArgs, Class<? extends Exception> expectedException) throws Exception {
    String clusterName = clusterNames[0];
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs)) {
      List<String> args = new ArrayList<>();
      args.add("--url");
      args.add(parentControllerClient.getLeaderControllerUrl());
      args.add("--cluster");
      args.add(clusterName);
      args.add("--kafka-bootstrap-servers");
      args.add(multiRegionMultiClusterWrapper.getControllerConnectString());
      args.add("--message_count");
      args.add("10");
      args.add("--dump-admin-messages");
      args.addAll(Arrays.asList(extraArgs));
      String[] adminToolArgs = args.toArray(new String[0]);

      if (expectedException != null) {
        Exception thrownException = expectThrows(expectedException, () -> AdminTool.main(adminToolArgs));
        assertEquals(
            thrownException.getClass(),
            expectedException,
            "Expected " + expectedException.getSimpleName() + " but got " + thrownException.getClass().getSimpleName()
                + " for args: " + Arrays.toString(adminToolArgs) + ". Exception: " + thrownException.getMessage());
      } else {
        try {
          AdminTool.main(adminToolArgs);
        } catch (Exception e) {
          fail("Unexpected failure for args: " + Arrays.toString(adminToolArgs) + ". Exception: " + e.getMessage(), e);
        }
      }
    }
  }

  private void createStore(
      ControllerClient parentControllerClient,
      ControllerClient childControllerClient,
      String storeName) {
    TestUtils.assertCommand(
        parentControllerClient
            .retryableRequest(5, c -> c.createNewStore(storeName, "test", "\"string\"", "\"string\"")));

    TestUtils.assertCommand(
        parentControllerClient
            .retryableRequest(5, c -> c.emptyPush(storeName, Utils.getUniqueString("empty-push-1"), 1L)));

    TestUtils.waitForNonDeterministicCompletion(
        100,
        TimeUnit.SECONDS,
        () -> childControllerClient.getStore(storeName).getStore().getCurrentVersion() > 0);

  }

  private void validateStoreMigrationStatusAcrossChildRegions(
      String storeName,
      String clusterName,
      boolean expectedStatus) {
    for (VeniceMultiClusterWrapper childRegion: childDatacenters) {
      try (ControllerClient childControllerClient =
          new ControllerClient(clusterName, childRegion.getControllerConnectString())) {
        validateStoreMigrationStatus(childControllerClient, storeName, expectedStatus, childRegion.getRegionName());
      }
    }
  }

  private void validateStoreMigrationStatus(
      ControllerClient controllerClient,
      String storeName,
      boolean expectedStatus,
      String region) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      StoreInfo storeInfo = Objects.requireNonNull(storeResponse.getStore(), "Store not found in " + region);
      if (expectedStatus) {
        assertTrue(storeInfo.isMigrating(), "Store::isMigrating should be true in " + region);
      } else {
        assertFalse(storeInfo.isMigrating(), "Store::isMigrating should be false in " + region);
      }
    });
  }

  @DataProvider(name = "skipAdminMessageOptions")
  public Object[][] skipAdminOptions() {
    return new Object[][] {
        // 1) execution-id only -> should pass
        { new String[] { "--execution-id", "10" }, false },
        // 2) position only -> should pass
        { new String[] { "--position", "0:0twI" }, false },
        // 3) neither provided -> should fail
        { new String[] {}, true },
        // 4) both provided -> should fail
        { new String[] { "--position", "0:0twI", "--execution-id", "10" }, true } };
  }

  @DataProvider(name = "dumpAdminMessageOptions")
  public Object[][] dumpAdminOptions() {
    return new Object[][] { { new String[] { "--starting_offset", "3" }, null },
        { new String[] { "--starting_position", "0:0twI" }, null }, { new String[] {}, RuntimeException.class },
        { new String[] { "--starting_offset", "10", "--starting_position", "0:0o1e" },
            AlreadySelectedException.class } };
  }
}
