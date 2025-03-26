package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestDeferredVersionSwap {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final String REGION1 = "dc-0";
  private static final String REGION2 = "dc-1";
  private static final String REGION3 = "dc-2";
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 120_000;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass
  public void setUp() {
    Properties controllerProps = new Properties();
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 30000);
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

  @DataProvider(name = "regionsProvider")
  public Object[][] regionsProvider() {
    Set<String> singleRegion = new HashSet<>();
    singleRegion.add(REGION1);

    Set<String> twoRegions = new HashSet<>();
    twoRegions.add(REGION1);
    twoRegions.add(REGION2);

    return new Object[][] { { RegionUtils.composeRegionList(singleRegion) },
        { RegionUtils.composeRegionList(twoRegions) }, };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "regionsProvider")
  public void testDeferredVersionSwap(String targetRegions) throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    Set<String> targetRegionsList = RegionUtils.parseRegionsFilterList(targetRegions);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, targetRegions);
      TestWriteUtils.runPushJob("Test push job", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Version should only be swapped in the target region
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          if (targetRegionsList.contains(colo)) {
            Assert.assertEquals((int) version, 1);
          } else {
            Assert.assertEquals((int) version, 0);
          }
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.PUSHED);
      });

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
      for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
        ControllerClient childControllerClient =
            new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
        StoreResponse store = childControllerClient.getStore(storeName);
        Optional<Version> version = store.getStore().getVersion(1);
        assertNotNull(version);
        assertEquals(version.get().getStatus(), VersionStatus.ONLINE);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "regionsProvider")
  public void testDeferredVersionSwapMultiplePushes(String targetRegions) throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapMultiplePushes");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    Set<String> targetRegionsList = RegionUtils.parseRegionsFilterList(targetRegions);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, storeParms).close();

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      props.put(TARGETED_REGION_PUSH_LIST, targetRegions);
      TestWriteUtils.runPushJob("Test push job", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Version should only be swapped in the target region
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          if (targetRegionsList.contains(colo)) {
            Assert.assertEquals((int) version, 1);
          } else {
            Assert.assertEquals((int) version, 0);
          }
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.PUSHED);
      });

      // Start a normal push
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, false);
      props.remove(TARGETED_REGION_PUSH_LIST);
      TestWriteUtils.runPushJob("Test push job 2", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Verify that the latest version is 2
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 2);
        });
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2, dataProvider = "regionsProvider")
  public void testDeferredVersionSwapWithFailedPushInNonTargetRegions(String failingRegions) throws IOException {
    // Always mark the status as ERROR in regions
    Set<String> failingRegionsList = RegionUtils.parseRegionsFilterList(failingRegions);
    for (String failingRegion: failingRegionsList) {
      multiRegionMultiClusterWrapper.failPushInRegion(failingRegion);
    }

    // Setup job properties
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapWithFailedPushInNonTargetRegions");
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
      try {
        TestWriteUtils.runPushJob("Test push job", props);
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
          if (!failingRegionsList.contains(colo)) {
            Assert.assertEquals((int) version, 1);
          } else {
            Assert.assertEquals((int) version, 0);
          }
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.PARTIALLY_ONLINE);
      });

      // Check that child version status is marked as ONLINE if it didn't fail
      for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
        ControllerClient childControllerClient =
            new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
        if (!failingRegionsList.contains(childDatacenter.getRegionName())) {
          StoreResponse store = childControllerClient.getStore(storeName);
          Optional<Version> version = store.getStore().getVersion(1);
          assertNotNull(version);
          assertEquals(version.get().getStatus(), VersionStatus.ONLINE);
        }
      }

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

  @Test(timeOut = TEST_TIMEOUT * 2, dataProvider = "regionsProvider")
  public void testDeferredVersionSwapWithFailedPushInTargetRegions(String failingRegions) throws IOException {
    // Always mark the status as ERROR in regions
    Set<String> failingRegionsList = RegionUtils.parseRegionsFilterList(failingRegions);
    for (String failingRegion: failingRegionsList) {
      multiRegionMultiClusterWrapper.failPushInRegion(failingRegion);
    }

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
      props.put(TARGETED_REGION_PUSH_LIST, REGION1 + ", " + REGION2);
      try {
        TestWriteUtils.runPushJob("Test push job", props);
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

      if (failingRegionsList.size() == 2) {
        TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
          Map<String, Integer> coloVersions =
              parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

          coloVersions.forEach((colo, version) -> {
            Assert.assertEquals((int) version, 0);
          });
        });
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
          Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.ERROR);
        });
      } else if (failingRegionsList.size() == 1) {
        TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
          Map<String, Integer> coloVersions =
              parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

          coloVersions.forEach((colo, version) -> {
            if (!failingRegionsList.contains(colo)) {
              Assert.assertEquals((int) version, 1);
            } else {
              Assert.assertEquals((int) version, 0);
            }
          });
        });

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
          Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.PARTIALLY_ONLINE);
        });

        // Check that child version status is marked as ONLINE if it didn't fail
        for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
          ControllerClient childControllerClient =
              new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
          if (!failingRegionsList.contains(childDatacenter.getRegionName())) {
            StoreResponse store = childControllerClient.getStore(storeName);
            Optional<Version> version = store.getStore().getVersion(1);
            assertNotNull(version);
            assertEquals(version.get().getStatus(), VersionStatus.ONLINE);
          }
        }
      }

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
  public void testDeferredVersionSwapWithHybridStore() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema(inputDir, 10);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapWithHybridStore");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"int\"";
    String valueSchemaStr = "\"int\"";
    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true)
        .setHybridOffsetLagThreshold(TEST_TIMEOUT)
        .setHybridRewindSeconds(2L)
        .setActiveActiveReplicationEnabled(true)
        .setTargetRegionSwapWaitTime(1);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAMES[0], keySchemaStr, valueSchemaStr, props, storeParams).close();

      // Start push job with target region push enabled
      props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
      TestWriteUtils.runPushJob("Test push job", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Version should only be swapped in the target region
      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          if (colo.equals(REGION3)) {
            Assert.assertEquals((int) version, 1);
          } else {
            Assert.assertEquals((int) version, 0);
          }
        });
      });

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(parentStore.getVersion(1).get().getStatus(), VersionStatus.PUSHED);
      });

      // Version should be swapped in all regions
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });
    }
  }

}
