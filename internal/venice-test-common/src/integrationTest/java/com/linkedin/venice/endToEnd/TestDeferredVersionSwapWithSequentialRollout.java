package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DEFERRED_VERSION_SWAP_FOR_EMPTY_PUSH_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFERRED_VERSION_SWAP_REGION_ROLL_FORWARD_ORDER;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.MockStoreLifecycleHooks;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapWithSequentialRollout {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final String REGION1 = "dc-0";
  private static final String REGION2 = "dc-1";
  private static final String REGION3 = "dc-2";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 180_000;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    controllerProps.put(DEFERRED_VERSION_SWAP_REGION_ROLL_FORWARD_ORDER, REGION1 + "," + REGION2 + "," + REGION3);
    controllerProps.put(DEFERRED_VERSION_SWAP_FOR_EMPTY_PUSH_ENABLED, true);
    Properties serverProperties = new Properties();

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwap() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Verify that final version is the same as the target version
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });
    }
  }

  @DataProvider(name = "validationsProvider")
  public Object[][] validationsProvider() {
    return new Object[][] { { StoreVersionLifecycleEventOutcome.PROCEED.toString(), 2 },
        { StoreVersionLifecycleEventOutcome.ROLLBACK.toString(), 1 } };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "validationsProvider")
  public void testDeferredVersionSwapWithValidationSequentialRollout(String validationOutcome, int targetVersion)
      throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> lifecycleHooksParams = new HashMap<>();
    lifecycleHooksParams.put("outcome", validationOutcome);
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), lifecycleHooksParams));
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setStoreLifecycleHooks(lifecycleHooks);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      try {
        IntegrationTestPushUtils.runVPJ(props);
      } catch (Exception e) {
        if (validationOutcome.equals(StoreVersionLifecycleEventOutcome.ROLLBACK.toString())) {
          Assert.assertTrue(e.getMessage().contains("rolled back after ingestion completed due to validation failure"));
        }
      }
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Verify that final version is the same as the target version
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, targetVersion);
        });
      });

      if (targetVersion == 1) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
          Assert.assertEquals(parentStore.getVersion(2).get().getStatus(), VersionStatus.KILLED);
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDvcDelayedIngestionWithTargetRegionSequentialRollout() throws Exception {
    // Setup job properties
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    String keySchemaStr = "\"int\"";
    String valueSchemaStr = "\"int\"";

    // Create store + start a normal push
    int keyCount = 100;
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema(inputDir, keyCount);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDvcDelayedIngestionWithTargetRegion");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, valueSchemaStr, props, storeParms).close();
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Version should only be swapped in all regions
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });
    }

    verifyThatPushStatusStoreIsOnline(storeName);
    verifyThatMetaStoreIsOnline(storeName);

    // Create dvc client in target region
    List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    VeniceClusterWrapper cluster1 = childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]);
    VeniceProperties backendConfig = DaVinciTestContext.getDaVinciPropertyBuilder(cluster1.getZk().getAddress())
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(LOCAL_REGION_NAME, REGION1)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .build();
    DaVinciClient<Object, Object> client1 =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster1, new DaVinciConfig(), backendConfig);
    client1.subscribeAll().get();

    // Check that v1 is ingested
    for (int i = 1; i <= keyCount; i++) {
      assertNotNull(client1.get(i).get());
    }

    // Do another push with target region enabled
    int keyCount2 = 200;
    File inputDir2 = getTempDataDirectory();
    String inputDirPath2 = "file://" + inputDir2.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema(inputDir2, keyCount2);
    Properties props2 =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath2, storeName);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      props2.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props2.put(TARGETED_REGION_PUSH_LIST, REGION1);
      IntegrationTestPushUtils.runVPJ(props2);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Data should be automatically ingested in target region for dvc
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int i = 101; i <= keyCount2; i++) {
          assertNotNull(client1.get(i).get());
        }
      });

      // Close dvc client in target region
      client1.close();

      // Create dvc client in non target region
      VeniceClusterWrapper cluster2 = childDatacenters.get(1).getClusters().get(CLUSTER_NAMES[0]);
      VeniceProperties backendConfig2 = DaVinciTestContext.getDaVinciPropertyBuilder(cluster2.getZk().getAddress())
          .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
          .put(LOCAL_REGION_NAME, REGION2)
          .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
          .build();
      DaVinciClient<Object, Object> client2 =
          ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster2, new DaVinciConfig(), backendConfig2);
      client2.subscribeAll().get();

      // Version should be swapped in all regions
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 2);
        });
      });

      // Check that v2 is ingested in dvc non target region
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int i = 101; i <= keyCount2; i++) {
          assertNotNull(client2.get(i).get());
        }
      });

      client2.close();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapForEmptyPush() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      parentControllerClient.emptyPush(storeName, "test", 100000);
      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse parentStore = parentControllerClient.getStore(storeName);
        Assert.assertNotNull(parentStore);
        StoreInfo storeInfo = parentStore.getStore();
        Assert.assertEquals(storeInfo.getVersion(1).get().getStatus(), VersionStatus.ONLINE);
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // Verify that we can create a new version
        VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
            storeName,
            1000,
            Version.PushType.BATCH,
            Version.guidBasedDummyPushId(),
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1);
        assertFalse(versionCreationResponse.isError());
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testVersionStatusOnControllerRestart() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testVersionStatusOnControllerRestart");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setTargetRegionSwapWaitTime(5);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job in separate thread so we don't wait until version is swapped
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      Thread runVpjThread = new Thread(() -> {
        IntegrationTestPushUtils.runVPJ(props);
      });
      runVpjThread.start();

      // Wait for ingestion to finish
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Get the dc-1 cluster and controller
      List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
      VeniceMultiClusterWrapper dc1Region = childDatacenters.get(1);
      VeniceClusterWrapper dc1Cluster = dc1Region.getClusters().get(CLUSTER_NAMES[0]);
      String dc1ControllerUrl = dc1Region.getControllerConnectString();

      // Create a controller client for dc-1 to check version status
      try (ControllerClient dc1ControllerClient = new ControllerClient(CLUSTER_NAMES[0], dc1ControllerUrl)) {

        // Verify version status is PUSHED in dc-1 BEFORE controller restart
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = dc1ControllerClient.getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Failed to get store: " + storeResponse.getError());
          StoreInfo storeInfo = storeResponse.getStore();
          Optional<Version> versionOpt = storeInfo.getVersion(1);
          Assert.assertTrue(versionOpt.isPresent(), "Version 1 should exist");
          VersionStatus statusBeforeRestart = versionOpt.get().getStatus();
          Assert.assertEquals(
              statusBeforeRestart,
              VersionStatus.PUSHED,
              "Version status in dc-1 should be PUSHED before controller restart");
        });

        // Restart the dc-1 controller to trigger ZK refresh
        VeniceControllerWrapper dc1Controller = dc1Cluster.getLeaderVeniceController();
        int controllerPort = dc1Controller.getPort();
        dc1Cluster.stopVeniceController(controllerPort);
        dc1Cluster.restartVeniceController(controllerPort);

        // Wait for controller to be ready
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse =
              new ControllerClient(CLUSTER_NAMES[0], dc1Region.getControllerConnectString()).getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Controller not ready: " + storeResponse.getError());
        });

        // Verify version status is still PUSHED in dc-1 AFTER controller restart
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse =
              new ControllerClient(CLUSTER_NAMES[0], dc1Region.getControllerConnectString()).getStore(storeName);
          Assert.assertFalse(storeResponse.isError(), "Failed to get store: " + storeResponse.getError());
          StoreInfo storeInfo = storeResponse.getStore();
          Optional<Version> versionOpt = storeInfo.getVersion(1);
          Assert.assertTrue(versionOpt.isPresent(), "Version 1 should exist");
          VersionStatus statusAfterRestart = versionOpt.get().getStatus();
          Assert.assertEquals(statusAfterRestart, VersionStatus.PUSHED);
        });

      }
    }
  }

  private void verifyThatPushStatusStoreIsOnline(String storeName) {
    for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
      String pushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
      ControllerClient childControllerClient =
          new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(pushStatusStoreName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, pushStatusStoreName + " is not ready");
      });
    }
  }

  private void verifyThatMetaStoreIsOnline(String storeName) {
    for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
      String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      ControllerClient childControllerClient =
          new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(metaStoreName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, metaStoreName + " is not ready");
      });
    }
  }
}
