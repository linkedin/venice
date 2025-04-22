package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDeferredVersionSwap {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
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
            .numberOfServers(3)
            .numberOfRouters(1)
            .replicationFactor(3)
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
  public void testDeferredVersionSwapWithoutTargetRegionPush() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testDeferredVersionSwapWithoutTargetRegionPush");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAMES[0], parentControllerURLs)) {
      createStoreForJob(
          CLUSTER_NAMES[0],
          keySchemaStr,
          NAME_RECORD_V3_SCHEMA.toString(),
          props,
          new UpdateStoreQueryParams()).close();

      VeniceClusterWrapper veniceClusterWrapper =
          childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1).getClusters().get(CLUSTER_NAMES[0]);

      int oldVersion = 0;
      int newVersion = 1;
      // Start push job with version swap
      props.put(DEFER_VERSION_SWAP, true);
      TestWriteUtils.runPushJob("Test push job", props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, newVersion),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Check that child version status is marked as ONLINE
      for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
        ControllerClient childControllerClient =
            new ControllerClient(CLUSTER_NAMES[0], childDatacenter.getControllerConnectString());
        StoreResponse store = childControllerClient.getStore(storeName);
        Optional<Version> version = store.getStore().getVersion(newVersion);
        assertNotNull(version);
        VersionStatus versionStatus = version.get().getStatus();
        assertEquals(versionStatus, VersionStatus.ONLINE, "versionStatus should be ONLINE, but was: " + versionStatus);
      }

      // validate that the current version is still 0 before rolling forward
      Assert.assertEquals(
          veniceClusterWrapper.getRandomVeniceController()
              .getVeniceAdmin()
              .getCurrentVersion(CLUSTER_NAMES[0], storeName),
          oldVersion,
          "version before roll forward should be " + oldVersion);

      // roll forward
      parentControllerClient.rollForwardToFutureVersion(
          storeName,
          String.join(",", multiRegionMultiClusterWrapper.getChildRegionNames()));

      // validate that the current version is rolled forward to 1
      int finalNewVersion = newVersion;
      TestUtils.waitForNonDeterministicAssertion(50, TimeUnit.SECONDS, () -> {
        int version = veniceClusterWrapper.getRandomVeniceController()
            .getVeniceAdmin()
            .getCurrentVersion(CLUSTER_NAMES[0], storeName);
        Assert.assertEquals(version, finalNewVersion, "version should be " + finalNewVersion + ", but was: " + version);
      });

      // use a thin client to verify the data in the push
      veniceClusterWrapper.refreshAllRouterMetaData();
      MetricsRepository metricsRepository = new MetricsRepository();
      try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(veniceClusterWrapper.getRandomRouterURL())
              .setMetricsRepository(metricsRepository))) {

        for (int i = 1; i <= 100; i++) {
          try {
            Assert.assertNotNull(avroClient.get(Integer.toString(i)).get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
