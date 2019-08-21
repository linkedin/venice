package com.linkedin.venice.restart;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestRestartController {
  private VeniceClusterWrapper cluster;
  int testTimeOutMS = 3000;

  @BeforeClass
  public void setup() {
    int numberOfController = 2;
    int numberOfServer = 1;
    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter);
  }

  @AfterClass
  public void cleanup() {
    cluster.close();
  }

  /**
   * Scenario is 1. stop the original master; 2. create new version to test master failover. 3. restart the failed one.
   * 4. complete push to test master could
   */
  @Test
  public void testMasterControllerFailover() {
    String storeName = TestUtils.getUniqueString("testMasterControllerFailover");
    int dataSize = 1000;

    cluster.getNewStore(storeName);

    VersionCreationResponse response = cluster.getNewVersion(storeName, dataSize);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();
    int versionNum = response.getVersion();

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getRandomRouterURL());
    Assert.assertEquals(
        controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());

    // push some data
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("1", "1", 1);

    // Stop the original master
    int port = cluster.stopMasterVeniceControler();

    // Push rest of data.
    veniceWriter.put("2", "2", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // After stopping origin master, the new master could handle the push status report correctly.
    TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
      JobStatusQueryResponse jobStatusQueryResponse =
          controllerClient.queryJobStatus(topicName);
      if (jobStatusQueryResponse.getError() != null) {
        return false;
      }
      return jobStatusQueryResponse.getStatus().equals(ExecutionStatus.COMPLETED.toString());
    });

    response = createNewVersionWithRetry(storeName, dataSize);

    String newTopicName = response.getKafkaTopic();
    // As we have not push any data, the job status should be hanged on STARTED.
    Assert.assertEquals(
        controllerClient.queryJobStatus(newTopicName).getStatus(), ExecutionStatus.STARTED.toString());
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getVersion(), versionNum + 1);

    // restart controller
    cluster.restartVeniceController(port);
    response = createNewVersionWithRetry(storeName, dataSize);

    newTopicName = response.getKafkaTopic();
    // As we have not push any data, the job status should be hanged on STARTED.
    Assert.assertEquals(
        controllerClient.queryJobStatus(newTopicName).getStatus(), ExecutionStatus.STARTED.toString());
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getVersion(), versionNum + 2);
  }

  private VersionCreationResponse createNewVersionWithRetry(String storeName, int dataSize) {
    // After restart, regardless who is master, there should be only one master and could handle request correctly.
    VersionCreationResponse response = null;
    try {
      response = cluster.getNewVersion(storeName, dataSize);
    } catch (VeniceException e) {
      // Some times, the master controller would be changed after getting it from cluster. Just retry on the new master.
      cluster.getMasterVeniceController();
      response = cluster.getNewVersion(storeName, dataSize);
    }
    return response;
  }
}
