package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NodeStatusResponse;
import com.linkedin.venice.controllerapi.StoppableNodeStatusResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.Replica;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class TestInstanceRemovable {
  private static final Logger LOGGER = LogManager.getLogger(TestInstanceRemovable.class);
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;

  private void setupCluster(int numberOfServer) {
    Properties properties = new Properties();
    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(numberOfServer)
            .numberOfRouters(1)
            .replicationFactor(replicaFactor)
            .partitionSize(partitionSize)
            .extraProperties(properties)
            .build());

    LOGGER.info("Finished setting up cluster with " + numberOfServer + " servers and RF " + replicaFactor + ".");
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
      String server1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
      String server2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
      String server3 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3);

      try (ControllerClient client = new ControllerClient(clusterName, urls)) {
        assertServerRemovability(client, server1, true);
        assertServerRemovability(client, server2, true);
        assertServerRemovability(client, server3, true);
        StoppableNodeStatusResponse statuses =
            client.getAggregatedHealthStatus(clusterName, Arrays.asList(server2, server1), Collections.emptyList());
        Assert.assertEquals(statuses.getStoppableInstances().size(), 2);
        /*
         * This is the same scenario as we would do later in the following test steps.
         * If hosts serverPort1 and serverPort2 were stopped, host serverPort3 would still be removable.
         */
        assertServerRemovability(client, server3, Arrays.asList(server1, server2), true);

        // stop a server during push
        cluster.stopVeniceServer(serverPort1);
        TestUtils.waitForNonDeterministicCompletion(
            3,
            TimeUnit.SECONDS,
            () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
        // could remove the rest of nodes as well
        assertServerRemovability(client, server2, true);
        assertServerRemovability(client, server3, true);
        // stop one more server
        cluster.stopVeniceServer(serverPort2);
        TestUtils.waitForNonDeterministicCompletion(
            3,
            TimeUnit.SECONDS,
            () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 2);
        // Even if there are no alive storage nodes, push should not fail.
        assertServerRemovability(client, server3, true);
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

  private void assertServerRemovability(ControllerClient client, String serverName, boolean removable) {
    assertServerRemovability(client, serverName, null, removable);
  }

  private void assertServerRemovability(
      ControllerClient client,
      String serverName,
      List<String> lockedServers,
      boolean removable) {
    NodeStatusResponse response =
        lockedServers == null ? client.isNodeRemovable(serverName) : client.isNodeRemovable(serverName, lockedServers);
    String expectation = "should " + (removable ? "" : "NOT ") + "be removable";
    assertEquals(
        response.isRemovable(),
        removable,
        "server '" + serverName + "' " + expectation + ", but instead got: " + response.getDetails() + "...");
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableAfterPush() throws Exception {
    setupCluster(3);
    String storeName = Utils.getUniqueString("testIsInstanceRemovableAfterPush");
    int partitionCount = 2;
    long storageQuota = (long) partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    VersionCreationResponse response = assertCommand(cluster.getNewVersion(storeName));
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        true,
        () -> assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicName)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED));

    String clusterName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();
    String server1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
    String server2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
    String server3 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3);
    List<String> server1SingletonList = Collections.singletonList(server1);

    /*
     * This is the same scenario as we would do later in the following test steps.
     * 1. Host serverPort1 is removable.
     * 2. If serverPort1 were stopped, host serverPort2 is non-removable.
     * 3. If serverPort1 were stopped, host serverPort3 is non-removable.
     */
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertServerRemovability(client, server2, true));

      StoppableNodeStatusResponse statuses =
          client.getAggregatedHealthStatus(clusterName, server1SingletonList, Collections.emptyList());
      List<String> stoppableInstances = statuses.getStoppableInstances();
      Assert.assertEquals(
          stoppableInstances,
          server1SingletonList,
          "The stoppable instances list is not as expected! It should contain only server 1, but instead has: "
              + stoppableInstances.toString() + ".");

      statuses =
          client.getAggregatedHealthStatus(clusterName, Arrays.asList(server1, server2), Collections.emptyList());
      Assert.assertEquals(statuses.getStoppableInstances(), server1SingletonList);
      Assert.assertEquals(statuses.getNonStoppableInstancesWithReasons().size(), 1);
      Assert.assertTrue(
          statuses.getNonStoppableInstancesWithReasons()
              .get(server2)
              .startsWith(NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.name()));

      assertServerRemovability(client, server2, server1SingletonList, false);
      assertServerRemovability(client, server3, server1SingletonList, false);
    }

    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
    // Can not remove node cause, it will trigger re-balance.
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      assertServerRemovability(client, server1, true);
      assertServerRemovability(client, server2, false);
      assertServerRemovability(client, server3, false);

      VeniceServerWrapper newServer = cluster.addVeniceServer(false, false);
      int serverPort4 = newServer.getPort();
      String server4 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        List<Replica> replicasHostedOnServer4 = cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getReplicasOfStorageNode(clusterName, server4)
            .stream()
            .filter(replica -> !VeniceSystemStoreUtils.isParticipantStore(replica.getResource()))
            .collect(Collectors.toList());
        assertEquals(
            replicasHostedOnServer4.size(),
            2,
            "Expected 2 replicas hosted on server4 (" + server4 + ") but instead got: " + replicasHostedOnServer4
                + ".");
      });
      // After replica number back to 3, all of node could be removed.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        assertServerRemovability(client, server2, true);
        assertServerRemovability(client, server3, true);
        assertServerRemovability(client, server4, true);
      });

      // After adding a new server, all servers are removable, even serverPort1 is still stopped.
      assertServerRemovability(client, server2, server1SingletonList, true);
      assertServerRemovability(client, server3, server1SingletonList, true);
      assertServerRemovability(client, server4, server1SingletonList, true);

      // Test if all instances of a partition are removed via a combination of locked instances and the requested node.
      assertServerRemovability(client, server4, Arrays.asList(server1, server2, server3), false);
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
