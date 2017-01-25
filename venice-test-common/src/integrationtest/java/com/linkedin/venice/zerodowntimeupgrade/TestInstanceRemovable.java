package com.linkedin.venice.zerodowntimeupgrade;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
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
  long testTimeOutMS = 3000;

  @BeforeMethod
  public void setup() {
    int numberOfController = 1;

    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter, replicaFactor,
        partitionSize);
  }

  @AfterMethod
  public void cleanup() {
    cluster.close();
  }

  @Test
  public void testIsInstanceRemovableDuringPush() {
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
            .getExecutionStatus()
            .equals(ExecutionStatus.STARTED));

    //All of replica in BOOTSTRAP
    String clusterName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();

    ControllerClient client = new ControllerClient(clusterName, urls);
    Assert.assertTrue(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort1)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());

    // stop a server during push
    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
    // could remove the rest of nodes as well
    Assert.assertTrue(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());
    // stop one more server
    cluster.stopVeniceServer(serverPort2);
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 2);
    // can not remove the only one bootstrap server
    Assert.assertFalse(client.isNodeRemovable(clusterName, Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());
  }

  @Test
  public void testIsInstanceRemovableAfterPush() {
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
    veniceWriter.put("test", "test", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLineJobStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

    String cluserName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();

    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(cluserName, topicName).size() == 4);
    // Can not remove node cause, it will trigger re-balance.
    ControllerClient client = new ControllerClient(cluserName, urls);
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort1)).isRemovable());
    Assert.assertFalse(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
    Assert.assertFalse(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());

    VeniceServerWrapper newServer = cluster.addVeniceServer(false, false);
    int serverPort4 = newServer.getPort();
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getReplicasOfStorageNode(cluserName, Utils.getHelixNodeIdentifier(serverPort4)).size() == 2);
    // After replica number back to 3, all of node could be removed.
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());
    Assert.assertTrue(client.isNodeRemovable(cluserName, Utils.getHelixNodeIdentifier(serverPort4)).isRemovable());

  }
}
