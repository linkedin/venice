package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DELAY_TO_REBALANCE_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Instance;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test class is used to test the deferred version swap feature without targeted region push enabled.
 * These are for the manual deferred swaps which will be deprecated once automatic swap service is rolled out.
 */
public class TestDeferredVersionSwapWithoutTargetedRegionPush {
  private static final Logger LOGGER = LogManager.getLogger(TestDeferredVersionSwapWithoutTargetedRegionPush.class);
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
    // set delay rebalance to 0
    controllerProps.put(DELAY_TO_REBALANCE_MS, 0);
    Properties serverProperties = new Properties();

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(3)
            .numberOfRouters(1)
            .replicationFactor(2)
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

  /**
   * This test will create a store with a version swap enabled, start a push job with deferred version
   * swap enabled, then stop the original standby server for one of the partition to trigger rebalance,
   * find the new standby server assigned for that partition, check if the new standby server is ready
   * to serve in CV and verify whether its state is updated to STANDBY in EV by then, roll forward to
   * the new version and finally verify the data in the push using a thin client.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapWithRebalancing() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100000, 100);
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

      // validate that the current version is still 0 as roll forward is not yet called
      assertEquals(
          veniceClusterWrapper.getRandomVeniceController()
              .getVeniceAdmin()
              .getCurrentVersion(CLUSTER_NAMES[0], storeName),
          oldVersion,
          "version before roll forward should be " + oldVersion);

      // take partition 0 for testing
      int testPartition = 0;

      // find the current standby instance for testPartition: as the RF is 2, there should be 1 LEADER and 1 STANDBY
      VeniceHelixAdmin veniceHelixAdmin = veniceClusterWrapper.getLeaderVeniceController().getVeniceHelixAdmin();
      String newVersionTopic = Version.composeKafkaTopic(storeName, newVersion);
      AtomicReference<VeniceServerWrapper> originalStandbyServer = new AtomicReference<>();
      List<VeniceServerWrapper> servers = veniceClusterWrapper.getVeniceServers();

      // As the version would have been made online with RF - 1 as min required RF, add extra time
      // to get the standby replica
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Instance standbyServerInstance = veniceHelixAdmin.getHelixVeniceClusterResources(CLUSTER_NAMES[0])
            .getRoutingDataRepository()
            .getPartitionAssignments(newVersionTopic)
            .getPartition(testPartition)
            .getAllInstancesByHelixState()
            .get(HelixState.STANDBY)
            .get(0);

        assertNotNull(
            standbyServerInstance,
            "Standby server instance should not be null for partition: " + testPartition);

        for (VeniceServerWrapper server: servers) {
          if (server.getPort() == standbyServerInstance.getPort()) {
            originalStandbyServer.set(server);
            break;
          }
        }
        assertNotNull(
            originalStandbyServer.get(),
            "Original standby server should not be null for partition: " + testPartition);
      });

      Instance originalStandbyServerInstance =
          Instance.fromHostAndPort(originalStandbyServer.get().getHost(), originalStandbyServer.get().getPort());

      LOGGER.info("Stopping the original standby server: {}", originalStandbyServerInstance);
      veniceClusterWrapper.stopVeniceServer(originalStandbyServer.get().getPort());

      // find the new standby instance for testPartition: as the original standby is stopped
      AtomicReference<Instance> newStandbyInstanceFromEV = new AtomicReference<>();
      TestUtils.waitForNonDeterministicAssertion(50, TimeUnit.SECONDS, () -> {
        List<Instance> standbyInstanceListFromEV = veniceHelixAdmin.getHelixVeniceClusterResources(CLUSTER_NAMES[0])
            .getRoutingDataRepository()
            .getPartitionAssignments(newVersionTopic)
            .getPartition(testPartition)
            .getAllInstancesByHelixState()
            .get(HelixState.STANDBY);
        assertEquals(
            standbyInstanceListFromEV.size(),
            1,
            "There should be only one standby instance for partition: " + testPartition + ", but found: "
                + standbyInstanceListFromEV);
        newStandbyInstanceFromEV.set(standbyInstanceListFromEV.get(0));

        assertNotNull(
            newStandbyInstanceFromEV.get(),
            "New standby instance from EV should not be null for partition: " + testPartition);
        assertFalse(
            newStandbyInstanceFromEV.get().equals(originalStandbyServerInstance),
            "New standby instance from EV: " + newStandbyInstanceFromEV.get()
                + " should not be the same as the stopped original standby instance: " + originalStandbyServerInstance);
      });

      // as the newStandbyInstanceFromEV is now marked STANDBY in EV, immediately check
      // whether it is marked ready to serve in CV
      HelixCustomizedViewOfflinePushRepository customizedViewRepository =
          veniceHelixAdmin.getHelixVeniceClusterResources(CLUSTER_NAMES[0]).getCustomizedViewRepository();
      assertTrue(
          customizedViewRepository.getReadyToServeInstances(newVersionTopic, 0)
              .contains(newStandbyInstanceFromEV.get()),
          "Ready to serve instances: " + customizedViewRepository.getReadyToServeInstances(newVersionTopic, 0)
              + " should contain the new newStandbyInstanceFromEV: " + newStandbyInstanceFromEV.get());

      // roll forward to the new version
      parentControllerClient.rollForwardToFutureVersion(
          storeName,
          String.join(",", multiRegionMultiClusterWrapper.getChildRegionNames()));

      // validate that the current version is rolled forward to 1
      int finalNewVersion = newVersion;
      TestUtils.waitForNonDeterministicAssertion(50, TimeUnit.SECONDS, () -> {
        int version = veniceClusterWrapper.getRandomVeniceController()
            .getVeniceAdmin()
            .getCurrentVersion(CLUSTER_NAMES[0], storeName);
        assertEquals(version, finalNewVersion, "version should be " + finalNewVersion + ", but was: " + version);
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
            assertNotNull(avroClient.get(Integer.toString(i)).get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
