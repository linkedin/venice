package com.linkedin.venice.restart;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRestartServer {
  private VeniceClusterWrapper cluster;
  int replicaFactor = 2;
  int partitionSize = 1000;

  @BeforeClass
  public void setUp() {
    int numberOfController = 1;
    int numberOfServer = 2;
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

  @AfterClass
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testRestartServerAfterPushCompleted() {
    String storeName = Utils.getUniqueString("testRestartServerAfterPushCompleted");
    int dataSize = 2000;
    int partitionCount = dataSize / partitionSize;
    cluster.getNewStore(storeName);
    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);

    String topicName = response.getKafkaTopic();
    Assert.assertEquals(response.getReplicas(), replicaFactor);
    Assert.assertEquals(response.getPartitions(), dataSize / partitionSize);

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(
        20,
        TimeUnit.SECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    // restart servers
    for (VeniceServerWrapper failedServer: cluster.getVeniceServers()) {
      cluster.stopVeniceServer(failedServer.getPort());
    }

    // After all server are shutdown, not partition assigned.
    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
      Assert.assertFalse(cluster.getRandomVeniceRouter().getRoutingDataRepository().containsKafkaTopic(topicName));
    });

    for (VeniceServerWrapper restartServer: cluster.getVeniceServers()) {
      cluster.restartVeniceServer(restartServer.getPort());
    }

    // After restart, all replicas become ready to serve again.
    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(topicName));
      for (int i = 0; i < partitionCount; i++) {
        Assert.assertEquals(routingDataRepository.getReadyToServeInstances(topicName, i).size(), replicaFactor);
      }
    });
  }
}
