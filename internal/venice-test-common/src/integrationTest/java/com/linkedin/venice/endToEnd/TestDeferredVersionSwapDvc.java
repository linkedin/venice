package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreMigrationTestUtil;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests deferred version swap with hybrid store migration and DaVinci client ingestion.
 * Separated from {@link TestDeferredVersionSwap} because these heavy tests (store migration,
 * DaVinci clients, multiple pushes) need their own fresh embedded cluster.
 *
 * Uses explicit BeforeClass/AfterClass (not AbstractMultiRegionTest) to match the original
 * cluster configuration: 100ms deferred swap polling, no auto-materialize meta system store.
 */
public class TestDeferredVersionSwapDvc {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final String REGION1 = "dc-0";
  private static final String REGION2 = "dc-1";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 120_000;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
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
  public void testDeferredVersionSwapInHybridStoreThenMigrateStore() throws Exception {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapInHybridStoreThenMigrateStore");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setHybridOffsetLagThreshold(TEST_TIMEOUT)
        .setHybridRewindSeconds(2L)
        .setActiveActiveReplicationEnabled(true)
        .setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    String srcClusterName = CLUSTER_NAMES[0];
    String destClusterName = CLUSTER_NAMES[1];
    try (ControllerClient parentControllerClient = new ControllerClient(srcClusterName, parentControllerURLs)) {
      createStoreForJob(srcClusterName, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, REGION1);
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Version should be swapped in all regions
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.ONLINE);
      });

      // Check that child version status is marked as ONLINE if it didn't fail
      for (VeniceMultiClusterWrapper childDatacenter: multiRegionMultiClusterWrapper.getChildRegions()) {
        ControllerClient childControllerClient =
            new ControllerClient(srcClusterName, childDatacenter.getControllerConnectString());
        StoreResponse store = childControllerClient.getStore(storeName);
        Optional<Version> version = store.getStore().getVersion(1);
        assertNotNull(version);
        assertEquals(version.get().getStatus(), VersionStatus.ONLINE);
      }
    }

    // Do a store migration
    VeniceMultiClusterWrapper multiClusterWrapper = multiRegionMultiClusterWrapper.getChildRegions().get(0);
    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      try {
        StoreMigrationTestUtil.startMigration(parentControllerURLs, storeName, srcClusterName, destClusterName);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      StoreMigrationTestUtil.completeMigration(
          parentControllerURLs,
          storeName,
          srcClusterName,
          destClusterName,
          multiRegionMultiClusterWrapper.getChildRegionNames());

      try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerURLs)) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          ControllerResponse discoveryResponse = destParentControllerClient.discoverCluster(storeName);
          Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
        });
      }

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        int key = ThreadLocalRandom.current().nextInt(20) + 1;
        client.get(Integer.toString(key)).get();

        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testDvcDelayedIngestionWithTargetRegion() throws Exception {
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    String keySchemaStr = "\"int\"";
    String valueSchemaStr = "\"int\"";

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
