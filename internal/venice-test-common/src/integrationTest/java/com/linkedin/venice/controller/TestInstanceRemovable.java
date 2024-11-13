package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoppableNodeStatusResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class TestInstanceRemovable {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;

  private void setupCluster(int numberOfServer) {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED, "false");
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(numberOfServer)
            .numberOfRouters(1)
            .replicationFactor(replicaFactor)
            .partitionSize(partitionSize)
            .extraProperties(properties)
            .build());
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableDuringPush() throws Exception {
    setupCluster(3);
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
        String server1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
        String server2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
        StoppableNodeStatusResponse statuses =
            client.getAggregatedHealthStatus(clusterName, Arrays.asList(server2, server1), Collections.emptyList());
        Assert.assertEquals(statuses.getStoppableInstances().size(), 2);
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
  public void testIsInstanceRemovableAfterPush() throws Exception {
    setupCluster(3);
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
        30,
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
          client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
      String server1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
      String server2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);

      StoppableNodeStatusResponse statuses =
          client.getAggregatedHealthStatus(clusterName, Collections.singletonList(server1), Collections.emptyList());
      Assert.assertEquals(statuses.getStoppableInstances(), Collections.singletonList(server1));

      statuses =
          client.getAggregatedHealthStatus(clusterName, Arrays.asList(server1, server2), Collections.emptyList());
      Assert.assertEquals(statuses.getStoppableInstances(), Collections.singletonList(server1));
      Assert.assertTrue(statuses.getNonStoppableInstancesWithReasons().containsKey(server2));
      Assert.assertTrue(
          statuses.getNonStoppableInstancesWithReasons()
              .containsValue(NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.name()));

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
      TestUtils.waitForNonDeterministicCompletion(
          3,
          TimeUnit.SECONDS,
          () -> client.isNodeRemovable(Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2)).isRemovable());
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

  @Test
  public void testIsInstanceRemovableOnOldVersion() throws Exception {
    setupCluster(1);
    int partitionCount = 2;
    int replicaCount = 1;
    long storageQuota = (long) partitionCount * partitionSize;
    String storeName = "testIsInstanceRemovableOnOldVersion";
    String clusterName = cluster.getClusterName();
    VeniceHelixAdmin veniceAdmin = (VeniceHelixAdmin) cluster.getLeaderVeniceController().getVeniceAdmin();

    cluster.getNewStore(storeName);
    cluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota).setReplicationFactor(replicaCount));

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
            .getOffLinePushStatus(cluster.getClusterName(), Version.composeKafkaTopic(storeName, 1))
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    String nodeID = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
    Assert.assertFalse(veniceAdmin.isInstanceRemovable(clusterName, nodeID, new ArrayList<>()).isRemovable());
    // Add a new node and increase the replica count to 2.
    cluster.addVeniceServer(new Properties(), new Properties());
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getReplicas(clusterName, Version.composeKafkaTopic(storeName, 1))
            .size() == 2);

    int newVersionReplicaCount = 2;
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setReplicationFactor(newVersionReplicaCount));
    response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), Version.composeKafkaTopic(storeName, 2))
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));
    // The old instance should now be removable because its replica is no longer the current version.
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeID, new ArrayList<>()).isRemovable());
  }

  @Test
  public void testIsInstanceRemovable() throws Exception {
    setupCluster(2);
    int partitionCount = 2;
    int replicationFactor = 2;
    String storeName = "testMovable";
    String clusterName = cluster.getClusterName();
    VeniceHelixAdmin veniceAdmin = (VeniceHelixAdmin) cluster.getLeaderVeniceController().getVeniceAdmin();

    cluster.getNewStore(storeName);
    cluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(1000).setReplicationFactor(replicationFactor));

    Version version = veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        replicationFactor);
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    String nodeId1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    String nodeId2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getWorkingInstances().size() != replicationFactor) {
          return false;
        }
      }
      return true;
    });

    // Make version ONLINE
    ReadWriteStoreRepository storeRepository =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    storeRepository.updateStore(store);

    // Enough number of replicas, any of instance is able to moved out.
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId1, Collections.emptyList()).isRemovable());
    Assert.assertTrue(veniceAdmin.isInstanceRemovable(clusterName, nodeId2, Collections.emptyList()).isRemovable());

    // Shutdown one instance
    cluster.stopVeniceServer(serverPort1);
    store.setCurrentVersion(1);
    storeRepository.updateStore(store);
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      return partitionAssignment.getPartition(0).getWorkingInstances().size() == 1;
    });

    Assert.assertTrue(
        veniceAdmin.isInstanceRemovable(clusterName, nodeId1, new ArrayList<>()).isRemovable(),
        "Instance is shutdown.");
    NodeRemovableResult result = veniceAdmin.isInstanceRemovable(clusterName, nodeId2, new ArrayList<>());
    Assert.assertFalse(result.isRemovable(), "Only one instance is alive, can not be moved out.");
    Assert.assertEquals(result.getBlockingReason(), NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA.toString());
  }

}
