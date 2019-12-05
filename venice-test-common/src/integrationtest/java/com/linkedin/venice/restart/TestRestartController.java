package com.linkedin.venice.restart;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Test(singleThreaded = true)
public class TestRestartController {
  private static final int OPERATION_TIMEOUT_MS = 3000;
  private VeniceClusterWrapper cluster;

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
    ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    Assert.assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());

    // push some data
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("1", "1", 1);

    // Stop the original master
    int port = cluster.stopMasterVeniceControler();

    // Push rest of data.
    veniceWriter.put("2", "2", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // After stopping origin master, the new master could handle the push status report correctly.
    TestUtils.waitForNonDeterministicPushCompletion(topicName, controllerClient, OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, Optional.empty());
    VersionCreationResponse responseV2 = createNewVersionWithRetry(storeName, dataSize);
    Assert.assertFalse(responseV2.isError());
    Assert.assertEquals(responseV2.getVersion(), versionNum + 1);

    // As we have not push any data, the job status should be hanged on STARTED.
    String topicNameV2 = responseV2.getKafkaTopic();
    Assert.assertEquals(controllerClient.queryJobStatus(topicNameV2).getStatus(), ExecutionStatus.STARTED.toString());

    // restart controller
    cluster.restartVeniceController(port);

    // As we have not push any data, the job status should be hanged on STARTED.
    TestUtils.waitForNonDeterministicAssertion(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, () ->
      Assert.assertEquals(controllerClient.queryJobStatus(topicNameV2).getStatus(), ExecutionStatus.STARTED.toString()));

    // Finish the push and verify that it completes under the newly elected controller.
    veniceWriter = cluster.getVeniceWriter(topicNameV2);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicPushCompletion(topicNameV2, controllerClient, OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, Optional.empty());

    // Check it one more time for good measure with a third and final push
    VersionCreationResponse responseV3 = createNewVersionWithRetry(storeName, dataSize);
    Assert.assertFalse(responseV3.isError());
    Assert.assertEquals(responseV3.getVersion(), versionNum + 2);

    // As we have not push any data, the job status should be hanged on STARTED.
    String topicNameV3 = responseV3.getKafkaTopic();
    Assert.assertEquals(controllerClient.queryJobStatus(topicNameV3).getStatus(), ExecutionStatus.STARTED.toString());

    // Broadcast end of push and verify it completes
    veniceWriter = cluster.getVeniceWriter(topicNameV3);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicPushCompletion(topicNameV3, controllerClient, OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, Optional.empty());
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
