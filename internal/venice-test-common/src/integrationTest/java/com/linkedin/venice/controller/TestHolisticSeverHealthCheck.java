package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessResponse;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessState;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHolisticSeverHealthCheck {
  private VeniceClusterWrapper cluster;
  protected ControllerClient controllerClient;
  int replicaFactor = 2;
  int partitionSize = 1000;

  @BeforeClass
  public void setUp() {
    int numOfController = 1;
    cluster = ServiceFactory.getVeniceCluster(numOfController, 2, 1, replicaFactor, partitionSize, false, false);

    Properties routerProperties = new Properties();
    cluster.addVeniceRouter(routerProperties);

    controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
  }

  @AfterClass
  public void cleanUp() {
    cluster.close();
  }

  private void verifyNodeReplicasState(String nodeId, NodeReplicasReadinessState state) {
    NodeReplicasReadinessResponse response = assertCommand(controllerClient.nodeReplicasReadiness(nodeId));
    assertEquals(response.getNodeState(), state);
  }

  private boolean verifyNodeIsError(String nodeId) {
    NodeReplicasReadinessResponse response = controllerClient.nodeReplicasReadiness(nodeId);
    return response.isError();
  }

  private void verifyNodesAreReady() {
    String wrongNodeId = "incorrect_node_id";
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());
      verifyNodeReplicasState(nodeId, NodeReplicasReadinessState.READY);
      Assert.assertTrue(verifyNodeIsError(wrongNodeId));
    }
  }

  private void verifyNodesAreInExpectedState(NodeReplicasReadinessState state) {
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());
      verifyNodeReplicasState(nodeId, state);
    }
  }

  private void verifyNodesAreInanimate() {
    verifyNodesAreInExpectedState(NodeReplicasReadinessState.INANIMATE);
  }

  private void verifyNodesAreUnready() {
    verifyNodesAreInExpectedState(NodeReplicasReadinessState.UNREADY);
  }

  /**
   * HealthServiceAfterServerRestart test does the following steps:
   *
   * 1.  Create a Venice cluster with 1 controller, 1 router, and 2 servers (customized view is enabled).
   * 2.  Verify both nodes are in the ready state.
   * 3.  Create a new store and push data.
   * 4.  Wait for the push job to complete.
   * 5.  Verify that both nodes are in the ready state.
   * 6.  Stop both servers and wait for them to fully stopped.
   * 7.  Verity that both nodes are in the inanimate state.
   * 8.  Restart both servers.
   * 9.  Verify that both nodes can come back in the ready state again.
   * 10. Mock CustomizedView so that getReadyToServeInstances returns an empty list.
   * 11. Verify that both servers are in the unready state
   */

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testHealthServiceAfterServerRestart() throws Exception {
    String storeName = Utils.getUniqueString("testHealthServiceAfterServerRestart");
    long storageQuota = 2000;

    // Assert both servers are in the ready state before the push.
    verifyNodesAreReady();

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    VersionCreationResponse response = cluster.getNewVersion(storeName);

    String topicName = response.getKafkaTopic();
    Assert.assertEquals(response.getReplicas(), replicaFactor);
    Assert.assertEquals(response.getPartitions(), storageQuota / partitionSize);

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1).get();
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait until push is completed.
    TestUtils.waitForNonDeterministicCompletion(
        120,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    // Assert both servers are in ready state.
    verifyNodesAreReady();

    // Stop both servers.
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      cluster.stopVeniceServer(server.getPort());
    }

    // Wait until the server is shutdown completely from the router's point of view.
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertFalse(cluster.getRandomVeniceRouter().getRoutingDataRepository().containsKafkaTopic(topicName));
    });

    // Verify that both servers are in the inanimate state.
    verifyNodesAreInanimate();

    // Restart both servers.
    for (VeniceServerWrapper restartServer: cluster.getVeniceServers()) {
      cluster.restartVeniceServer(restartServer.getPort());
    }

    // Wait until the servers are in the ready state again.
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());
      TestUtils.waitForNonDeterministicAssertion(
          120,
          TimeUnit.SECONDS,
          () -> verifyNodeReplicasState(nodeId, NodeReplicasReadinessState.READY));
    }

    // Mock CustomizedView so that getReadyToServeInstances returns an empty list.
    VeniceHelixAdmin admin = (VeniceHelixAdmin) cluster.getLeaderVeniceController().getVeniceAdmin();
    ResourceAssignment resourceAssignment = admin.getHelixVeniceClusterResources(cluster.getClusterName())
        .getCustomizedViewRepository()
        .getResourceAssignment();

    HelixCustomizedViewOfflinePushRepository mockedCvRepo = mock(HelixCustomizedViewOfflinePushRepository.class);
    when(mockedCvRepo.getReadyToServeInstances((PartitionAssignment) any(), anyInt()))
        .thenReturn(Collections.emptyList());
    when(mockedCvRepo.getResourceAssignment()).thenReturn(resourceAssignment);
    admin.getHelixVeniceClusterResources(cluster.getClusterName()).setCustomizedViewRepository(mockedCvRepo);

    // Verify that both servers are in the unready state.
    verifyNodesAreUnready();
  }
}
