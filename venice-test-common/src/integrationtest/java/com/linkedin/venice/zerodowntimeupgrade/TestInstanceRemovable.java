package com.linkedin.venice.zerodowntimeupgrade;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestInstanceRemovable {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 2;
  int numberOfServer = 2;
  long testTimeOutMS = 3000;

  @BeforeClass
  public void setup() {
    int numberOfController = 2;

    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter, replicaFactor,
        partitionSize);
  }

  @AfterClass
  public void cleanup() {
    cluster.close();
  }

  @Test
  public void testIsInstanceRemovable() {
    String storeName = TestUtils.getUniqueString("testMasterControllerFailover");
    int partitionCount = 2;
    int dataSize = partitionCount * partitionSize;

    cluster.getNewStore(storeName, dataSize);

    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();
    int versionNum = response.getVersion();

    VeniceWriter<String, String> veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());

    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLineJobStatus(cluster.getClusterName(), topicName)
            .equals(ExecutionStatus.STARTED));

    //All of replica in BOOTSTRAP
    String cluserName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    ControllerClient client = new ControllerClient(cluserName, urls);
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort1)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
    veniceWriter.put("test", "test", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLineJobStatus(cluster.getClusterName(), topicName)
            .equals(ExecutionStatus.COMPLETED));

    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(cluserName, topicName).size() == 2);

    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort1)).isRemovable());
    Assert.assertFalse(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
  }
}
