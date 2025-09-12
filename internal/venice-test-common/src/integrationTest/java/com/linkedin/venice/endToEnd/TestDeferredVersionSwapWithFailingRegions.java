package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapWithFailingRegions {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final String FAILED_REGION = "dc-1";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 120_000;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testDeferredVersionSwapWithFailedPushInTargetRegions() throws IOException {
    setUpCluster();

    multiRegionMultiClusterWrapper.failPushInRegion(FAILED_REGION);

    // Setup job properties
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapWithFailedPushInTargetRegions");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled and check that it fails
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, FAILED_REGION);
      try {
        IntegrationTestPushUtils.runVPJ(props);
      } catch (Exception e) {
        assertEquals(e.getClass(), VeniceException.class);
      }

      // Wait for job to fail
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
        JobStatusQueryResponse jobStatusQueryResponse = assertCommand(
            parentControllerClient.queryOverallJobStatus(Version.composeKafkaTopic(storeName, 1), Optional.empty()));
        ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
        assertEquals(executionStatus, ExecutionStatus.ERROR);
      });

      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 0);
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.KILLED);
      });

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
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDvcDelayedIngestionWithFailingPushInTargetRegion() throws Exception {
    setUpCluster();

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
        .put(LOCAL_REGION_NAME, FAILED_REGION)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .build();
    DaVinciClient<Object, Object> client1 =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster1, new DaVinciConfig(), backendConfig);
    client1.subscribeAll().get();

    // Check that v1 is ingested
    for (int i = 1; i <= keyCount; i++) {
      assertNotNull(client1.get(i).get());
    }

    // Fail push in target region
    multiRegionMultiClusterWrapper.failPushInRegion(FAILED_REGION);

    // Do another push with target region enabled
    int keyCount2 = 200;
    File inputDir2 = getTempDataDirectory();
    String inputDirPath2 = "file://" + inputDir2.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema(inputDir2, keyCount2);
    Properties props2 =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath2, storeName);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      props2.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props2.put(TARGETED_REGION_PUSH_LIST, FAILED_REGION);
      try {
        IntegrationTestPushUtils.runVPJ(props2);
      } catch (Exception e) {
        assertEquals(e.getClass(), VeniceException.class);
      }

      // Version shouldn't be swapped in any region
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });

      // Version status should be ERROR
      TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(2).get().getStatus(), VersionStatus.KILLED);
      });

      // verify that dvc client did not ingest the version
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int i = 101; i <= keyCount2; i++) {
          assertNull(client1.get(i).get());
        }
      });

      // Wait for push completion
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        String kafkaTopicName = Version.composeKafkaTopic(storeName, 2);
        JobStatusQueryResponse response =
            parentControllerClient.queryOverallJobStatus(kafkaTopicName, Optional.empty());
        assertEquals(response.getStatus(), ExecutionStatus.ERROR.toString());
      });
    }

    client1.close();
  }

  public void setUpCluster() {
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
