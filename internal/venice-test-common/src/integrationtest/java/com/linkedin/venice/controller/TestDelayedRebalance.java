package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class TestDelayedRebalance {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 2;
  int numberOfServer = 3;
  long testTimeOutMS = 10 * Time.MS_PER_SECOND;
  // Ensure delayed rebalance time out is larger than test timeout to avoid doing rebalance due to
  // waitForNonDeterministicCompletion
  long delayRebalanceMS = testTimeOutMS * 2;
  int minActiveReplica = replicaFactor - 1;

  @BeforeMethod
  public void setUp() {
    // Start a cluster with enabling delayed rebalance.
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(numberOfServer)
        .numberOfRouters(1)
        .replicationFactor(replicaFactor)
        .partitionSize(partitionSize)
        .rebalanceDelayMs(delayRebalanceMS)
        .minActiveReplica(minActiveReplica)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFailOneServerWithDelayedRebalance() throws InterruptedException {
    // Test the case that fail one server with enable delayed rebalance. Helix will not move the partition to other
    // server.
    // After restart the failed server, replica would be recovered correctly.
    String topicName = createVersionAndPushData();
    int failServerPort = stopAServer(topicName);
    // Wait one server disconnected, helix just set replica to OFFLINE but shouldn't rebalance
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 1);
    });
    // restart failed server
    cluster.restartVeniceServer(failServerPort);
    // OFFLINE replica become ONLINE again and is on the restarted server.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
    });
  }

  /**
   * Fail one server with enabling delayed rebalance, but do not restart server before timeout. Helix should move the
   * partition to other server.
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFailOneServerWithDelayedRebalanceTimeout() throws InterruptedException {
    String topicName = createVersionAndPushData();
    int failServerPort = stopAServer(topicName);
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      cluster.refreshAllRouterMetaData();
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      int readyToServeInstances = routingDataRepository.getReadyToServeInstances(topicName, 0).size();
      Assert.assertEquals(
          readyToServeInstances,
          1,
          "Right after taking down a server, the number of live instances should not have dropped to 0");
    });
    Thread.sleep(delayRebalanceMS / 2);
    Assert.assertEquals(
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getReadyToServeInstances(topicName, 0).size(),
        1,
        "With delayed rebalance, helix should not move the partition to other machine during the delayed rebalance time.");

    Thread.sleep(delayRebalanceMS / 2);
    // Wait rebalance happen due to timeout.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
    });
    // nothing left for the failed instance
    PartitionAssignment partitionAssignment =
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    Assert.assertNull(
        partitionAssignment.getPartition(0)
            .getHelixStateByInstanceId(Utils.getHelixNodeIdentifier(Utils.getHostName(), failServerPort)));
  }

  @Test
  public void testModifyDelayedRebalanceTime() {
    // Test the case that set the shorter delayed time for a cluster, to check whether helix will do the rebalance
    // earlier.
    String topicName = createVersionAndPushData();
    // shorter delayed time
    cluster.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .setDelayedRebalanceTime(cluster.getClusterName(), testTimeOutMS / 2);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()),
        testTimeOutMS / 2);
    stopAServer(topicName);

    // Helix do not do the rebalance immediately
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 1);
    });
    // Before test time out, helix do the rebalance.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
    });
  }

  @Test()
  public void testDisableRebalanceTemporarily() throws InterruptedException {
    // Test the cases that fail one server after disabling delayed rebalance of the cluster. Helix will move the
    // partition immediately.
    // Then enable the delayed rebalance again to test whether helix will do rebalance immediately.
    String topicName = createVersionAndPushData();
    // disable delayed rebalance
    cluster.getLeaderVeniceController().getVeniceHelixAdmin().setDelayedRebalanceTime(cluster.getClusterName(), 0);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()),
        0);
    // wait the config change come into effect
    Thread.sleep(testTimeOutMS);
    int failServerPort = stopAServer(topicName);
    // Wait rebalance happen immediately and all replica become ONLINE again. Ensure the replica moved to other server.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
      String instanceId = Utils.getHelixNodeIdentifier(Utils.getHostName(), failServerPort);
      Assert.assertNull(
          routingDataRepository.getPartitionAssignments(topicName)
              .getPartition(0)
              .getHelixStateByInstanceId(instanceId));
    });
  }

  @Test
  public void testEnableDelayedRebalance() throws InterruptedException {
    String topicName = createVersionAndPushData();
    // disable delayed rebalance
    cluster.getLeaderVeniceController().getVeniceHelixAdmin().setDelayedRebalanceTime(cluster.getClusterName(), 0);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()),
        0);

    // Enable delayed rebalance
    cluster.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .setDelayedRebalanceTime(cluster.getClusterName(), delayRebalanceMS);
    // wait the config change come into effect
    Thread.sleep(testTimeOutMS);
    // Fail on server
    int failServerPort = stopAServer(topicName);

    // Wait one server disconnected, helix just set replica to OFFLINE but do not do the rebalance
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 1);
    });
    // restart failed server
    cluster.restartVeniceServer(failServerPort);
    // OFFLINE replica become ONLINE again.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
    });

    PartitionAssignment partitionAssignment =
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    // The restart server get the original replica and become ONLINE again.
    Assert.assertEquals(
        partitionAssignment.getPartition(0)
            .getExecutionStatusByInstanceId(Utils.getHelixNodeIdentifier(Utils.getHostName(), failServerPort)),
        ExecutionStatus.COMPLETED);
  }

  private int stopAServer(String topicName) {
    RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
    int failServerPort = routingDataRepository.getReadyToServeInstances(topicName, 0).get(0).getPort();
    cluster.stopVeniceServer(failServerPort);
    return failServerPort;
  }

  private String createVersionAndPushData() {
    String storeName = Utils.getUniqueString("TestDelayedRebalance");
    int partitionCount = 1;

    cluster.getNewStore(storeName);
    cluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte((long) partitionCount * partitionSize));
    VersionCreationResponse response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait push completed and all replica become ready to serve.
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, true, true, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, 0).size(), 2);
    });

    return topicName;
  }
}
