package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DELAY_TO_REBALANCE_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
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
import org.testng.annotations.Test;


/**
 * This test class is used to test the deferred version swap feature without targeted region push enabled.
 * These are for the manual deferred swaps which will be deprecated once automatic swap service is rolled out.
 */
public class TestDeferredVersionSwapWithoutTargetedRegionPush extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestDeferredVersionSwapWithoutTargetedRegionPush.class);
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int TEST_TIMEOUT = 180_000;
  private static final int OLD_VERSION = 0;
  private static final int NEW_VERSION = 1;
  private static final int testPartition = 0;
  private static final String clusterName = CLUSTER_NAMES[0];

  @Override
  protected int getNumberOfRegions() {
    return 3;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(DELAY_TO_REBALANCE_MS, 0);
    return controllerProps;
  }

  /**
   * This test will create a store with a version swap enabled, start a push job with deferred version
   * swap enabled, then stop the original standby server for one of the partition to trigger rebalance,
   * find the new standby server assigned for that partition, check if the new standby server is ready
   * to serve in CV and verify whether its state is updated to STANDBY in EV by then, roll forward to
   * the new version and finally verify the data in the push using a thin client.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapWithFutureVersionSlowStateTransition() throws Exception {
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    ControllerClient parentClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
    VeniceClusterWrapper cluster = childDatacenters.get(childDatacenters.size() - 1).getClusters().get(clusterName);

    // setup and validation
    prepareAndPushWithDeferredSwap(storeName, 100, 100, parentClient);
    // still serving the old version because of deferred swap
    assertCurrentServingVersion(cluster, storeName, OLD_VERSION);

    // stop original standby and add new server triggering rebalance
    Instance oldStandby = stopInstance(cluster, storeName, HelixState.STANDBY);
    cluster.addVeniceServer(new Properties());

    // wait for new standby in EV
    AtomicReference<Instance> newStandby = new AtomicReference<>();
    TestUtils.waitForNonDeterministicAssertion(50, TimeUnit.SECONDS, () -> {
      Partition partition = cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(Version.composeKafkaTopic(storeName, NEW_VERSION))
          .getPartition(testPartition);
      assertNotNull(partition);
      List<Instance> standbyList = partition.getAllInstancesByHelixState().get(HelixState.STANDBY);
      assertEquals(standbyList.size(), 1);
      Instance candidate = standbyList.get(0);
      assertNotEquals(candidate, oldStandby);
      newStandby.set(candidate);
    });

    // verify ready-to-serve immediately in CV
    assertCompleted(cluster, storeName, testPartition, newStandby.get());

    // roll forward and verify new version
    parentClient
        .rollForwardToFutureVersion(storeName, String.join(",", multiRegionMultiClusterWrapper.getChildRegionNames()));
    TestUtils.waitForNonDeterministicAssertion(
        50,
        TimeUnit.SECONDS,
        () -> assertCurrentServingVersion(cluster, storeName, NEW_VERSION));

    validateData(cluster, storeName, 100);
  }

  /** This test will fail the roll forward due to insufficient replicas, but eventually succeed */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDeferredVersionSwapFailureWithEventualRollForward() throws Exception {
    String storeName = Utils.getUniqueString("testDeferredVersionSwap");
    ControllerClient parentClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
    VeniceClusterWrapper cluster = childDatacenters.get(childDatacenters.size() - 1).getClusters().get(clusterName);

    // setup and validation
    prepareAndPushWithDeferredSwap(storeName, 100000, 100, parentClient);
    // still serving the old version because of deferred swap
    assertCurrentServingVersion(cluster, storeName, OLD_VERSION);

    // stop both standby and leader
    stopInstance(cluster, storeName, HelixState.STANDBY);
    stopInstance(cluster, storeName, HelixState.LEADER);

    // wait until all replicas are not ready to serve
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Partition partition = cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(Version.composeKafkaTopic(storeName, NEW_VERSION))
          .getPartition(testPartition);
      assertTrue(
          partition == null || (partition.getAllInstancesByHelixState().get(HelixState.LEADER).isEmpty()
              && partition.getAllInstancesByHelixState().get(HelixState.STANDBY).isEmpty()));
    });

    // attempt roll forward (expected to fail due to missing ready to serve replicas)
    ControllerResponse rollForwardResponse = parentClient
        .rollForwardToFutureVersion(storeName, String.join(",", multiRegionMultiClusterWrapper.getChildRegionNames()));
    assertTrue(rollForwardResponse.isError(), "Roll forward should have failed due to insufficient replicas");
    assertTrue(
        rollForwardResponse.getError().contains("Roll forward failed for store"),
        "Unexpected error message: " + rollForwardResponse.getError());

    // add new servers and wait for eventual success of roll forward as each child controllers will
    // be retrying the roll forward until success
    cluster.addVeniceServer(new Properties());
    cluster.addVeniceServer(new Properties());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      parentClient.rollForwardToFutureVersion(
          storeName,
          String.join(",", multiRegionMultiClusterWrapper.getChildRegionNames()));
      assertCurrentServingVersion(cluster, storeName, NEW_VERSION);
    });

    validateData(cluster, storeName, 100);
  }

  private void prepareAndPushWithDeferredSwap(
      String storeName,
      int recordCount,
      int keyCount,
      ControllerClient parentClient) throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, recordCount, keyCount);

    Properties props = IntegrationTestPushUtils
        .defaultVPJProps(multiRegionMultiClusterWrapper, "file://" + inputDir.getAbsolutePath(), storeName);
    props.put(DEFER_VERSION_SWAP, true);

    createStoreForJob(
        clusterName,
        "\"string\"",
        NAME_RECORD_V3_SCHEMA.toString(),
        props,
        new UpdateStoreQueryParams().setPartitionCount(1)).close();

    IntegrationTestPushUtils.runVPJ(props);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, NEW_VERSION),
        parentClient,
        30,
        TimeUnit.SECONDS);

    for (VeniceMultiClusterWrapper dc: childDatacenters) {
      try (ControllerClient c = new ControllerClient(clusterName, dc.getControllerConnectString())) {
        StoreResponse resp = c.getStore(storeName);
        Optional<com.linkedin.venice.meta.Version> v = resp.getStore().getVersion(NEW_VERSION);
        assertTrue(v.isPresent());
        assertEquals(v.get().getStatus(), VersionStatus.ONLINE);
      }
    }
  }

  private void assertCurrentServingVersion(VeniceClusterWrapper cluster, String storeName, int expected) {
    int actual = cluster.getRandomVeniceController().getVeniceAdmin().getCurrentVersion(clusterName, storeName);
    assertEquals(actual, expected);
  }

  private Instance stopInstance(VeniceClusterWrapper cluster, String storeName, HelixState state) {
    String topic = Version.composeKafkaTopic(storeName, NEW_VERSION);
    Partition partition = cluster.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(clusterName)
        .getRoutingDataRepository()
        .getPartitionAssignments(topic)
        .getPartition(testPartition);
    Instance helixInst = partition.getAllInstancesByHelixState().get(state).get(0);
    // find matching server wrapper to stop
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      if (server.getPort() == helixInst.getPort()) {
        Instance inst = Instance.fromHostAndPort(server.getHost(), server.getPort());
        LOGGER.info("Stopping {} instance: {}", state, inst);
        cluster.stopVeniceServer(server.getPort());
        return inst;
      }
    }
    throw new IllegalStateException("No server wrapper found for Helix instance " + helixInst);
  }

  private void assertCompleted(VeniceClusterWrapper cluster, String storeName, int partitionId, Instance inst) {
    Partition partition = cluster.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(clusterName)
        .getCustomizedViewRepository()
        .getResourceAssignment()
        .getPartition(Version.composeKafkaTopic(storeName, NEW_VERSION), partitionId);
    List<Instance> completed = partition.getAllInstancesByExecutionStatus().get(ExecutionStatus.COMPLETED);
    assertTrue(completed.contains(inst));
  }

  /** validate data using thin client **/
  private void validateData(VeniceClusterWrapper cluster, String storeName, int expectedCount) throws Exception {
    cluster.refreshAllRouterMetaData();
    MetricsRepository metricsRepo = new MetricsRepository();
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(cluster.getRandomRouterURL())
            .setMetricsRepository(metricsRepo))) {
      for (int i = 1; i <= expectedCount; i++) {
        assertNotNull(client.get(Integer.toString(i)).get());
      }
    }
  }
}
