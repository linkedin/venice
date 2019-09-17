package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.HelixControllerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.*;



public class TestHAASController {
  private static final String CONTROLLER_CLUSTER_DEFAULT_NAME = "venice-controllers";

  @Test
  public void testStartPureHelixControllerAsControllerClusterLeader() {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(0, 0, 0, 1);
    HelixControllerWrapper helixControllerWrapper = ServiceFactory.getHelixController(venice.getZk().getAddress(),
        CONTROLLER_CLUSTER_DEFAULT_NAME);
    assertEquals(helixControllerWrapper.getClusterLeader().getInstanceName(), HelixControllerWrapper.HELIX_INSTANCE_NAME);
    Properties properties = new Properties();
    properties.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
    VeniceControllerWrapper controllerWrapper = venice.addVeniceController(properties);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertTrue(controllerWrapper.isMasterControllerForControllerCluster(),
            "The only Venice controller should be appointed the colo master"));
    assertTrue(controllerWrapper.isMasterController(venice.getClusterName()),
        "The only Venice controller should be the leader of cluster " + venice.getClusterName());
    assertEquals(helixControllerWrapper.getClusterLeader().getInstanceName(), HelixControllerWrapper.HELIX_INSTANCE_NAME,
        "The leader of the controller cluster should still be the pure Helix controller");
    venice.addVeniceServer(new Properties(), new Properties());
    VersionCreationResponse response = venice.getNewStoreVersion();
    assertFalse(response.isError());
    venice.getControllerClient().writeEndOfPush(response.getName(),
        Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.COMPLETED.toString()));

    helixControllerWrapper.close();
    venice.close();
  }

  @Test
  public void testTransitionToHAASControllerAsControllerClusterLeader() {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(3, 1, 0, 1);
    VersionCreationResponse response = venice.getNewStoreVersion();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.STARTED.toString()));
    HelixControllerWrapper helixControllerWrapper = ServiceFactory.getHelixController(venice.getZk().getAddress(),
        CONTROLLER_CLUSTER_DEFAULT_NAME);
    List<VeniceControllerWrapper> oldControllers = venice.getVeniceControllers();
    List<VeniceControllerWrapper> newControllers = new ArrayList<>();
    // Start the rolling bounce process
    Properties properties = new Properties();
    properties.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
    for (VeniceControllerWrapper oldController : oldControllers) {
      venice.stopVeniceController(oldController.getPort());
      oldController.close();
      newControllers.add(venice.addVeniceController(properties));
    }
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertEquals(helixControllerWrapper.getClusterLeader().getInstanceName(),
            HelixControllerWrapper.HELIX_INSTANCE_NAME));
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS,
        () -> {
          for (VeniceControllerWrapper newController : newControllers) {
            if (newController.isMasterController(venice.getClusterName())) {
              assertTrue(newController.isMasterControllerForControllerCluster(),
                  "The colo master should be the leader of the only cluster");
              return true;
            }
          }
          return false;
        });
    // Make sure the previous ongoing push can be completed.
    venice.getControllerClient().writeEndOfPush(response.getName(),
        Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.COMPLETED.toString()));

    helixControllerWrapper.close();
    venice.close();
  }
}
