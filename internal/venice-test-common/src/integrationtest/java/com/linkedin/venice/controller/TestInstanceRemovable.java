package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestInstanceRemovable {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;
  int numberOfServer = 3;

  @BeforeMethod
  public void setUp() {
    int numberOfController = 1;

    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(
        numberOfController,
        numberOfServer,
        numberOfRouter,
        replicaFactor,
        partitionSize,
        false,
        false);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableDuringPush() {
    String storeName = Utils.getUniqueString("testIsInstanceRemovableDuringPush");
    int partitionCount = 2;
    long storageQuota = (long) partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    VersionCreationResponse response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());

      TestUtils.waitForNonDeterministicCompletion(
          3,
          TimeUnit.SECONDS,
          () -> cluster.getLeaderVeniceController()
              .getVeniceAdmin()
              .getOffLinePushStatus(cluster.getClusterName(), topicName)
              .getExecutionStatus()
              .equals(ExecutionStatus.STARTED));

      // All of replica in BOOTSTRAP
      String clusterName = cluster.getClusterName();
      String urls = cluster.getAllControllersURLs();
      int serverPort1 = cluster.getVeniceServers().get(0).getPort();
      int serverPort2 = cluster.getVeniceServers().get(1).getPort();
      int serverPort3 = cluster.getVeniceServers().get(2).getPort();

      try (ControllerClient client = new ControllerClient(clusterName, urls)) {
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)).isRemovable());
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)).isRemovable());

        /*
         * This is the same scenario as we would do later in the following test steps.
         * If hosts serverPort1 and serverPort2 were stopped, host serverPort3 would still be removable.
         */
        Assert
            .assertTrue(
                client
                    .isNodeRemovable(
                        Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3),
                        Arrays.asList(
                            Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1),
                            Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)))
                    .isRemovable());

        // stop a server during push
        cluster.stopVeniceServer(serverPort1);
        TestUtils.waitForNonDeterministicCompletion(
            3,
            TimeUnit.SECONDS,
            () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
        // could remove the rest of nodes as well
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)).isRemovable());
        // stop one more server
        cluster.stopVeniceServer(serverPort2);
        TestUtils.waitForNonDeterministicCompletion(
            3,
            TimeUnit.SECONDS,
            () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 2);
        // Even if there are no alive storage nodes, push should not fail.
        Assert.assertTrue(
            client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)).isRemovable());
        // Add the storage servers back and the ingestion should still be able to complete.
        cluster.addVeniceServer(new Properties(), new Properties());
        cluster.addVeniceServer(new Properties(), new Properties());
        veniceWriter.broadcastEndOfPush(new HashMap<>());
        TestUtils.waitForNonDeterministicCompletion(
            3,
            TimeUnit.SECONDS,
            () -> cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicName)
                .getExecutionStatus()
                .equals(ExecutionStatus.COMPLETED));
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableAfterPush() {
    String storeName = Utils.getUniqueString("testIsInstanceRemovableAfterPush");
    int partitionCount = 2;
    long storageQuota = (long) partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    VersionCreationResponse response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    String clusterName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();

    /*
     * This is the same scenario as we would do later in the following test steps.
     * 1. Host serverPort1 is removable.
     * 2. If serverPort1 were stopped, host serverPort2 is non-removable.
     * 3. If serverPort1 were stopped, host serverPort3 is non-removable.
     */
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      Assert.assertTrue(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)).isRemovable());
      Assert.assertFalse(
          client
              .isNodeRemovable(
                  Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2),
                  Arrays.asList(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)))
              .isRemovable());
      Assert.assertFalse(
          client
              .isNodeRemovable(
                  Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3),
                  Arrays.asList(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)))
              .isRemovable());
    }

    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
    // Can not remove node cause, it will trigger re-balance.
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      Assert.assertTrue(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)).isRemovable());
      Assert.assertFalse(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
      Assert.assertFalse(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)).isRemovable());

      VeniceServerWrapper newServer = cluster.addVeniceServer(false, false);
      int serverPort4 = newServer.getPort();
      TestUtils.waitForNonDeterministicCompletion(
          3,
          TimeUnit.SECONDS,
          () -> cluster.getLeaderVeniceController()
              .getVeniceAdmin()
              .getReplicasOfStorageNode(clusterName, Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4))
              .size() == 2);
      // After replica number back to 3, all of node could be removed.
      Assert.assertTrue(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
      Assert.assertTrue(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)).isRemovable());
      Assert.assertTrue(
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4)).isRemovable());

      // After adding a new server, all servers are removable, even serverPort1 is still stopped.
      Assert.assertTrue(
          client
              .isNodeRemovable(
                  Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2),
                  Arrays.asList(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)))
              .isRemovable());
      Assert.assertTrue(
          client
              .isNodeRemovable(
                  Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3),
                  Arrays.asList(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)))
              .isRemovable());
      Assert.assertTrue(
          client
              .isNodeRemovable(
                  Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4),
                  Arrays.asList(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1)))
              .isRemovable());

      // Test if all instances of a partition are removed via a combination of locked instances and the requested node.
      Assert
          .assertFalse(
              client
                  .isNodeRemovable(
                      Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4),
                      Arrays.asList(
                          Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1),
                          Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2),
                          Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3)))
                  .isRemovable());
    }
  }
}
