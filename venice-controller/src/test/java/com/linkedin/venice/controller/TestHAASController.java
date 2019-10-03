package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.HelixAsAServiceWrapper;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;



public class TestHAASController {
  private Properties enableHAASProperties;

  @BeforeClass
  public void setup() {
    enableHAASProperties = new Properties();
    enableHAASProperties.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
    enableHAASProperties.put(ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME, HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME);
  }

  @Test
  public void testStartHAASHelixControllerAsControllerClusterLeader() {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(0, 0, 0, 1);
    HelixAsAServiceWrapper helixAsAServiceWrapper = ServiceFactory.getHelixController(venice.getZk().getAddress());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true,
        () -> assertNotNull(helixAsAServiceWrapper.getSuperClusterLeader(),
            "Helix super cluster doesn't have a leader yet"));
    VeniceControllerWrapper controllerWrapper = venice.addVeniceController(enableHAASProperties);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertTrue(controllerWrapper.isMasterControllerForControllerCluster(),
            "The only Venice controller should be appointed the colo master"));
    assertTrue(controllerWrapper.isMasterController(venice.getClusterName()),
        "The only Venice controller should be the leader of cluster " + venice.getClusterName());
    venice.addVeniceServer(new Properties(), new Properties());
    VersionCreationResponse response = venice.getNewStoreVersion();
    assertFalse(response.isError());
    venice.getControllerClient().writeEndOfPush(response.getName(),
        Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()));
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.COMPLETED.toString()));
    String zk = helixAsAServiceWrapper.getZkAddress();
    helixAsAServiceWrapper.close();
    venice.close();
  }

  @Test
  public void testTransitionToHAASControllerAsControllerClusterLeader() {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(3, 1, 0, 1);
    HelixAsAServiceWrapper helixAsAServiceWrapper = ServiceFactory.getHelixController(venice.getZk().getAddress());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true,
        () -> assertNotNull(helixAsAServiceWrapper.getSuperClusterLeader(),
            "Helix super cluster doesn't have a leader yet"));
    VersionCreationResponse response = venice.getNewStoreVersion();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.STARTED.toString()));
    List<VeniceControllerWrapper> oldControllers = venice.getVeniceControllers();
    List<VeniceControllerWrapper> newControllers = new ArrayList<>();
    // Start the rolling bounce process
    for (VeniceControllerWrapper oldController : oldControllers) {
      venice.stopVeniceController(oldController.getPort());
      oldController.close();
      newControllers.add(venice.addVeniceController(enableHAASProperties));
    }
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
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> assertEquals(venice.getControllerClient().queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.COMPLETED.toString()));
    helixAsAServiceWrapper.close();
    venice.close();
  }
}
