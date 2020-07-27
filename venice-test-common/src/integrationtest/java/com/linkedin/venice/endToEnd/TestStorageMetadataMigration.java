package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestStorageMetadataMigration {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;
  long testTimeOutMS = 3000;

  @BeforeMethod
  public void setup() {
    int numberOfController = 1;

    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, 0, numberOfRouter, replicaFactor,
        partitionSize, false, false);
    Properties disableRocksDBMetadata = new Properties();
    // Start servers with BDB metadata service
    disableRocksDBMetadata.put(SERVER_ENABLE_ROCKSDB_METADATA, false);
    cluster.addVeniceServer(new Properties(), disableRocksDBMetadata);
    cluster.addVeniceServer(new Properties(), disableRocksDBMetadata);
    cluster.addVeniceServer(new Properties(), disableRocksDBMetadata);
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

    cluster.getNewStore(storeName);

    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());

    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.STARTED));

    String clusterName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();

    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      // stop a server during push
      cluster.stopVeniceServer(serverPort1);
      TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
          () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 4);
      Assert.assertTrue(client.isNodeRemovable(Utils.getHelixNodeIdentifier(serverPort2)).isRemovable());
      Assert.assertTrue(client.isNodeRemovable(Utils.getHelixNodeIdentifier(serverPort3)).isRemovable());
    }

    // stop one more server
    cluster.stopVeniceServer(serverPort2);
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size() == 2);

    // Add the storage servers with rocksDB metadata config on back and the ingestion should complete.
    Properties enableRocksDBMetadata = new Properties();
    enableRocksDBMetadata.put(SERVER_ENABLE_ROCKSDB_METADATA, true);
    cluster.addVeniceServer(new Properties(), enableRocksDBMetadata);
    cluster.addVeniceServer(new Properties(), enableRocksDBMetadata);

    veniceWriter.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
        () -> cluster.getMasterVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));
  }
}
