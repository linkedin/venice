package com.linkedin.venice.restart;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.TestUtils;
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
  long testTimeOutMS = 20000;

  @BeforeClass
  public void setup() {
    int numberOfController = 1;
    int numberOfServer = 2;
    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter, replicaFactor,
        partitionSize, false, false);
  }

  @AfterClass
  public void cleanup() {
    cluster.close();
  }

  @Test
  public void testRestartServerAfterPushCompleted() {
    String storeName = TestUtils.getUniqueString("testRestartServerAfterPushCompleted");
    int dataSize = 2000;
    int partitionCount = dataSize / partitionSize;
    cluster.getNewStore(storeName);
    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);

    String topicName = response.getKafkaTopic();
    Assert.assertEquals(response.getReplicas(), replicaFactor);
    Assert.assertEquals(response.getPartitions(), dataSize / partitionSize);

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("test", "test", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    //restart servers
    for (VeniceServerWrapper failedServer : cluster.getVeniceServers()) {
      cluster.stopVeniceServer(failedServer.getPort());
    }

    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      // Ensure all of server are shutdown, not partition assigned.
      return partitionAssignment.getAssignedNumberOfPartitions() == 0;
    });

    for (VeniceServerWrapper restartServer : cluster.getVeniceServers()) {
      cluster.restartVeniceServer(restartServer.getPort());
    }

    // After restart, all of replica become ONLINE again.
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topicName);
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getReadyToServeInstances().size() != replicaFactor) {
          return false;
        }
      }
      return true;
    });
  }
}
