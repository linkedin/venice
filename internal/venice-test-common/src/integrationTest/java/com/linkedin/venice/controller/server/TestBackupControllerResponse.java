package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.*;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerTransport;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBackupControllerResponse {
  @Test
  public void backupControllerThrows421() throws Exception {
    String clusterName = "backupControllerThrows421";
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        PubSubBrokerWrapper pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
            new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer)
                .setRegionName(VeniceClusterWrapperConstants.STANDALONE_REGION_NAME)
                .build());
        ControllerTransport transport = new ControllerTransport(Optional.empty());
        VeniceControllerWrapper controller1 = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(
                clusterName,
                zkServer,
                pubSubBrokerWrapper,
                Collections
                    .singletonMap(STANDALONE_REGION_NAME, D2TestUtils.getAndStartD2Client(zkServer.getAddress())))
                        .regionName(VeniceClusterWrapperConstants.STANDALONE_REGION_NAME)
                        .build());
        VeniceControllerWrapper controller2 = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(
                clusterName,
                zkServer,
                pubSubBrokerWrapper,
                Collections
                    .singletonMap(STANDALONE_REGION_NAME, D2TestUtils.getAndStartD2Client(zkServer.getAddress())))
                        .regionName(VeniceClusterWrapperConstants.STANDALONE_REGION_NAME)
                        .build())) {
      // Wait for leader election to complete before identifying leader/non-leader controllers.
      // Without this, both controllers may report as non-leader, and requests to a controller
      // that hasn't finished leader election can return 421 from the wrong code path.
      TestUtils.waitForNonDeterministicCompletion(
          30,
          TimeUnit.SECONDS,
          () -> controller1.isLeaderController(clusterName) || controller2.isLeaderController(clusterName));
      VeniceControllerWrapper leaderController =
          controller1.isLeaderController(clusterName) ? controller1 : controller2;
      VeniceControllerWrapper nonLeaderController =
          controller1.isLeaderController(clusterName) ? controller2 : controller1;
      try {
        transport.request(
            nonLeaderController.getControllerUrl(),
            REQUEST_TOPIC,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
      } catch (VeniceHttpException e) {
        Assert.assertEquals(e.getHttpStatusCode(), HttpConstants.SC_MISDIRECTED_REQUEST);
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      try {
        transport.request(
            nonLeaderController.getControllerUrl(),
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
      } catch (VeniceHttpException e) {
        Assert.assertEquals(e.getHttpStatusCode(), HttpConstants.SC_MISDIRECTED_REQUEST);
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      // Send to the leader controller so the request is actually processed (not instantly
      // rejected with 421). A non-leader would return 421 faster than the 1ms timeout.
      try {
        int timeoutMs = 1;
        transport.request(
            leaderController.getControllerUrl(),
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class,
            timeoutMs,
            null);
        Assert.fail("Expected TimeoutException did not happen");
      } catch (TimeoutException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      try {
        String invalidControllerUrl = "http://0.0.0.0";
        transport.request(
            invalidControllerUrl,
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
        Assert.fail("Expected ExecutionException did not happen");
      } catch (ExecutionException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      String deadControllerUrl = controller1.getControllerUrl();
      controller1.close();
      controller2.close();
      try {
        transport.request(
            deadControllerUrl,
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
        Assert.fail("Expected ExecutionException did not happen");
      } catch (ExecutionException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }
    }
  }
}
