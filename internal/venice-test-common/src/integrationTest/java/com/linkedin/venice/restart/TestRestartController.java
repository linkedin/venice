package com.linkedin.venice.restart;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRestartController {
  private static final int OPERATION_TIMEOUT_MS = 3 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper cluster;

  @BeforeMethod
  public void setUp() {
    int numberOfController = 2;
    int numberOfServer = 1;
    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter);
  }

  @AfterMethod
  public void cleanUp() {
    cluster.close();
  }

  /**
   * Scenario is 1. stop the original leader; 2. create new version to test leader failover. 3. restart the failed one.
   * 4. complete push to test leader could
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testLeaderControllerFailover() {
    String storeName = Utils.getUniqueString("testLeaderControllerFailover");
    cluster.getNewStore(storeName);

    VersionCreationResponse response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();
    int versionNum = response.getVersion();

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    Assert.assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());

    // push some data
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("1", "1", 1);

    // Stop the original leader
    int port = cluster.stopLeaderVeniceController();

    // Push rest of data.
    veniceWriter.put("2", "2", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // After stopping origin leader, the new leader could handle the push status report correctly.
    TestUtils.waitForNonDeterministicPushCompletion(
        topicName,
        controllerClient,
        OPERATION_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);
    VersionCreationResponse responseV2 = createNewVersionWithRetry(storeName);
    Assert.assertFalse(responseV2.isError());
    Assert.assertEquals(responseV2.getVersion(), versionNum + 1);

    // As we have not push any data, the job status should be hanged on STARTED.
    String topicNameV2 = responseV2.getKafkaTopic();
    Assert.assertEquals(controllerClient.queryJobStatus(topicNameV2).getStatus(), ExecutionStatus.STARTED.toString());

    // restart controller
    cluster.restartVeniceController(port);

    TestUtils.waitForNonDeterministicAssertion(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS, false, true, () -> {
      VeniceHelixAdmin admin = cluster.getLeaderVeniceController().getVeniceHelixAdmin();
      int liveRoutersCount = admin.getHelixVeniceClusterResources(cluster.getClusterName())
          .getRoutersClusterManager()
          .getLiveRoutersCount();
      Assert.assertEquals(liveRoutersCount, 1);
    });
    // As we have not push any data, the job status should be hanged on STARTED.
    TestUtils.waitForNonDeterministicAssertion(
        OPERATION_TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            controllerClient.queryJobStatus(topicNameV2).getStatus(),
            ExecutionStatus.STARTED.toString()));

    // Finish the push and verify that it completes under the newly elected controller.
    veniceWriter = cluster.getVeniceWriter(topicNameV2);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicPushCompletion(
        topicNameV2,
        controllerClient,
        OPERATION_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);

    // Check it one more time for good measure with a third and final push
    VersionCreationResponse responseV3 = createNewVersionWithRetry(storeName);
    Assert.assertFalse(responseV3.isError());
    Assert.assertEquals(responseV3.getVersion(), versionNum + 2);

    // As we have not push any data, the job status should be hanged on STARTED.
    String topicNameV3 = responseV3.getKafkaTopic();
    Assert.assertEquals(controllerClient.queryJobStatus(topicNameV3).getStatus(), ExecutionStatus.STARTED.toString());

    // Broadcast end of push and verify it completes
    veniceWriter = cluster.getVeniceWriter(topicNameV3);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    TestUtils.waitForNonDeterministicPushCompletion(
        topicNameV3,
        controllerClient,
        OPERATION_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);

  }

  /**
   * Objective of the test is to verify on controller restart the new leader controller is able to update the
   * successful_push_duration_sec_gauge metrics with the last successful push duration for the store.
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testControllerRestartFetchesLastSuccessfulPushDuration() {
    String storeName = Utils.getUniqueString("testControllerRestartFetchesLastSuccessfulPushDuration");
    cluster.getNewStore(storeName);
    VersionCreationResponse response = cluster.getNewVersion(storeName);
    Assert.assertFalse(response.isError());
    String topicName = response.getKafkaTopic();

    VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName);
    ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    Assert.assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    // push some data
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put("1", "1", 1);
    // Push rest of data.
    veniceWriter.put("2", "2", 1);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // After stopping origin leader, the new leader could handle the push status report correctly.
    TestUtils.waitForNonDeterministicPushCompletion(
        topicName,
        controllerClient,
        OPERATION_TIMEOUT_MS,
        TimeUnit.MILLISECONDS);
    VeniceControllerWrapper controllerWrapper = cluster.getLeaderVeniceController();
    double duration = controllerWrapper.getMetricRepository()
        .getMetric("." + storeName + "--successful_push_duration_sec_gauge.Gauge")
        .value();

    int oldLeaderPort = cluster.getLeaderVeniceController().getPort();
    int newLeaderPort = 0;
    for (VeniceControllerWrapper cw: cluster.getVeniceControllers()) {
      if (cw.getPort() != oldLeaderPort) {
        newLeaderPort = cw.getPort();
        break;
      }
    }
    cluster.stopLeaderVeniceController();
    controllerWrapper = cluster.getLeaderVeniceController();
    Assert.assertEquals(controllerWrapper.getPort(), newLeaderPort);
    double duration1 = controllerWrapper.getMetricRepository()
        .getMetric("." + storeName + "--successful_push_duration_sec_gauge.Gauge")
        .value();
    Assert.assertEquals(duration, duration1);

    cluster.restartVeniceController(oldLeaderPort);
    controllerWrapper = cluster.getLeaderVeniceController();
    duration1 = controllerWrapper.getMetricRepository()
        .getMetric("." + storeName + "--successful_push_duration_sec_gauge.Gauge")
        .value();
    Assert.assertEquals(duration, duration1);

  }

  private VersionCreationResponse createNewVersionWithRetry(String storeName) {
    // After restart, regardless who is leader, there should be only one leader and could handle request correctly.
    VersionCreationResponse response = null;
    try {
      response = cluster.getNewVersion(storeName);
    } catch (VeniceException e) {
      // Some times, the leader controller would be changed after getting it from cluster. Just retry on the new leader.
      cluster.getLeaderVeniceController();
      response = cluster.getNewVersion(storeName);
    }
    return response;
  }
}
