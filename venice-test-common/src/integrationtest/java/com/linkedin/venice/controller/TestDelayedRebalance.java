package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;


@Test
public class TestDelayedRebalance {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 2;
  int numberOfServer = 3;
  long testTimeOutMS = 3 * Time.MS_PER_SECOND;
  //Ensure delayed rebalance time out is larger than test timeout to avoid doing rebalance due to
  // waitForNonDeterministicCompletion
  long delayRebalanceMS = testTimeOutMS * 2;
  int minActiveReplica = replicaFactor - 1;

  @BeforeMethod
  public void setUp() {
    int numberOfController = 1;
    int numberOfRouter = 1;
    // Start a cluster with enabling delayed rebalance.
    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter, replicaFactor,
        partitionSize, false, false, delayRebalanceMS, minActiveReplica, false, false);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void  testFailOneServerWithDelayedRebalance()
      throws InterruptedException {
    // Test the case that fail one server with enable delayed rebalance. Helix will not move the partition to other server.
    // After restart the failed server, replica would be recoverd correctly.
    String topicName = createVersionAndPushData();
    int failServerPort = stopAServer(topicName);
    //Wait one server disconnected, helix just set replica to OFFLINE but do not do the rebalance
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 1
          && assignment.getPartition(0).getBootstrapInstances().size() == 0;
    });
    // restart failed server
    cluster.restartVeniceServer(failServerPort);
    //OFFLINE replica become ONLINE again.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2;
    });

    PartitionAssignment partitionAssignment =
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    // The restart server get the original replica and become ONLINE again.
    Assert.assertTrue(
        partitionAssignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)).equals(HelixState.STANDBY_STATE)
            ||         partitionAssignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)).equals(HelixState.LEADER_STATE));
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void tesFailOneServerWithDelayedRebalanceTimeout()
      throws InterruptedException {
    // Test the cases that fail one server with enabling delayed rebalance. But do not restart server before timeout. So
    // helix would move the partition to other server.
    String topicName = createVersionAndPushData();
    int failServerPort = stopAServer(topicName);
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      cluster.refreshAllRouterMetaData();
      PartitionAssignment partitionAssignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      int readyToServeInstances = partitionAssignment.getPartition(0).getWorkingInstances().size();
      Assert.assertTrue(readyToServeInstances > 0,
          "Right after taking down a server, the number of live instances should not have dropped to 0");
      if (readyToServeInstances == 1) {
        return true;
      } else {
        boolean serverStillAlive = partitionAssignment.getPartition(0).getWorkingInstances().stream()
            .anyMatch(i -> i.getPort() == failServerPort);
        Assert.assertFalse(serverStillAlive,
            "Right after taking down a server, the number of live instances should have dropped to 1.");
        // The server is not completely stopped yet since it's still a part of the ready to server instances.
        return false;
      }
    });
    Thread.sleep(delayRebalanceMS / 2);

    PartitionAssignment partitionAssignment = cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    Assert.assertEquals(partitionAssignment.getPartition(0).getWorkingInstances().size(), 1,
        "With delayed reblance, helix should not move the partition to other machine during the delayed reblance time.");

    Thread.sleep(delayRebalanceMS / 2);
    // Wait rebalance happen due to timeout.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2;
    });
    partitionAssignment =
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);

    // nothing left for the failed instance
    Assert.assertNull(
        partitionAssignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)));
  }

  @Test
  public void testModifyDelayedRebalanceTime() {
    // Test the case that set the shorter delayed time for a cluster, to check whether helix will do the rebalance earlier.
    String topicName = createVersionAndPushData();
    // shorter delayed time
    cluster.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .setDelayedRebalanceTime(cluster.getClusterName(), testTimeOutMS / 2);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()),
        testTimeOutMS / 2);
    stopAServer(topicName);

    // Helix do not do the relanace immediately
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 1;
    });
    // Before test time out, helix do the rebalance.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2;
    });
  }

  @Test()
  public void testDisableRebalanceTemporarily()
      throws InterruptedException {
    // Test the cases that fail one server after disabling delayed rebalance of the cluster. Helix will move the partition immediately.
    // Then enable the delayed rebalance again to test whether helix will do rebalance immediately.
    String topicName = createVersionAndPushData();
    // disable delayed rebalance
    cluster.getLeaderVeniceController().getVeniceHelixAdmin().setDelayedRebalanceTime(cluster.getClusterName(), 0);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()), 0);
    // wait the config change come into effect
    Thread.sleep(testTimeOutMS);
    int failServerPort = stopAServer(topicName);
    // Wait rebalance happen immediately and all replica become ONLINE again. Ensure the replica moved to other server.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2
          && assignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)) == null;
    });
  }

  @Test
  public void testEnableDelayedRebalance()
      throws InterruptedException {
    String topicName = createVersionAndPushData();
    // disable delayed rebalance
    cluster.getLeaderVeniceController().getVeniceHelixAdmin().setDelayedRebalanceTime(cluster.getClusterName(), 0);
    Assert.assertEquals(
        cluster.getLeaderVeniceController().getVeniceHelixAdmin().getDelayedRebalanceTime(cluster.getClusterName()), 0);

    //Enable delayed rebalance
    cluster.getLeaderVeniceController().getVeniceHelixAdmin().setDelayedRebalanceTime(cluster.getClusterName(), delayRebalanceMS);
    // wait the config change come into effect
    Thread.sleep(testTimeOutMS);
    // Fail on server
    int failServerPort = stopAServer(topicName);

    //Wait one server disconnected, helix just set replica to OFFLINE but do not do the rebalance
    TestUtils.waitForNonDeterministicAssertion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      Assert.assertTrue(assignment.getPartition(0).getWorkingInstances().size() == 1
          && assignment.getPartition(0).getBootstrapInstances().size() == 0);
    });
    // restart failed server
    cluster.restartVeniceServer(failServerPort);
    //OFFLINE replica become ONLINE again.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2;
    });

    PartitionAssignment partitionAssignment = cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    // The restart server get the original replica and become ONLINE again.
    Assert.assertTrue(
        partitionAssignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)).equals(HelixState.STANDBY_STATE)
        ||         partitionAssignment.getPartition(0).getInstanceStatusById(Utils.getHelixNodeIdentifier(failServerPort)).equals(HelixState.LEADER_STATE));
  }

  private int stopAServer(String topicName) {
    PartitionAssignment partitionAssignment =
        cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
    int failServerPort = partitionAssignment.getPartition(0).getWorkingInstances().get(0).getPort();
    cluster.stopVeniceServer(failServerPort);
    return failServerPort;
  }

  private String createVersionAndPushData() {
    String storeName = Utils.getUniqueString("TestDelayedRebalance");
    int partitionCount = 1;
    int dataSize = partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    //Wait push completed and all replica become ONLINE
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PartitionAssignment assignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      return assignment.getPartition(0).getWorkingInstances().size() == 2;
    });

    return topicName;
  }
}
