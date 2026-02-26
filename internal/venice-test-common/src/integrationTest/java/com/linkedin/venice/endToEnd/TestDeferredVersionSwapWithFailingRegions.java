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

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
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
import java.io.File;
import java.io.IOException;
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
  }
}
