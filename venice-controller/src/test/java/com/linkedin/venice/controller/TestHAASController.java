package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.HelixAsAServiceWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.LiveInstance;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TestHAASController {
  private Properties enableControllerClusterHAASProperties;
  private Properties enableControllerAndStorageClusterHAASProperties;

  @BeforeClass
  public void setUp() {
    enableControllerClusterHAASProperties = new Properties();
    enableControllerClusterHAASProperties.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
    enableControllerClusterHAASProperties.put(ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME,
        HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME);
    enableControllerAndStorageClusterHAASProperties = (Properties) enableControllerClusterHAASProperties.clone();
    enableControllerAndStorageClusterHAASProperties.put(ConfigKeys.VENICE_STORAGE_CLUSTER_LEADER_HAAS,
        String.valueOf(true));
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testStartHAASHelixControllerAsControllerClusterLeader() {
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(0, 0, 0, 1);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(enableControllerClusterHAASProperties);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
          () -> assertTrue(controllerWrapper.isLeaderControllerOfControllerCluster(),
              "The only Venice controller should be appointed the colo leader"));
      assertTrue(controllerWrapper.isLeaderController(venice.getClusterName()),
          "The only Venice controller should be the leader of Venice cluster " + venice.getClusterName());
      venice.addVeniceServer(new Properties(), new Properties());
      VersionCreationResponse response = venice.getNewStoreVersion();
      assertFalse(response.isError());
      venice.useControllerClient(controllerClient -> {
        controllerClient.writeEndOfPush(response.getName(), Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()));
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
            () -> assertEquals(controllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(), ExecutionStatus.COMPLETED.toString())
        );
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testTransitionToHAASControllerAsControllerClusterLeader() {
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(3, 1, 0, 1);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
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
        newControllers.add(venice.addVeniceController(enableControllerClusterHAASProperties));
      }
      TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS,
          () -> {
            for (VeniceControllerWrapper newController : newControllers) {
              if (newController.isLeaderController(venice.getClusterName())) {
                assertTrue(newController.isLeaderControllerOfControllerCluster(),
                    "The colo leader Venice controller should be the leader of the only Venice cluster");
                return true;
              }
            }
            return false;
          });

      // Make sure the previous ongoing push can be completed.
      venice.useControllerClient(controllerClient -> {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () ->
            TestUtils.assertCommand(
                controllerClient.writeEndOfPush(response.getName(), Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()))
            )
        );
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
            () -> assertEquals(controllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(), ExecutionStatus.COMPLETED.toString())
        );
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testStartHAASControllerAsStorageClusterLeader() {
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(0, 0, 0, 1);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerWrapper controllerWrapper =
          venice.addVeniceController(enableControllerAndStorageClusterHAASProperties);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
          () -> assertTrue(controllerWrapper.isLeaderController(venice.getClusterName()),
              "The only Venice controller should become the leader of the only Venice cluster"));
      venice.addVeniceServer(new Properties(), new Properties());
      venice.addVeniceServer(new Properties(), new Properties());
      VersionCreationResponse response = venice.getNewStoreVersion();
      venice.useControllerClient(controllerClient -> {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
            () -> assertEquals(controllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(),
                ExecutionStatus.STARTED.toString()));
        controllerClient.writeEndOfPush(response.getName(), Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()));
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
            () -> assertEquals(controllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(),
                ExecutionStatus.COMPLETED.toString()));
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testTransitionToHAASControllerAsStorageClusterLeader() {
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(3, 1, 0, 1);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VersionCreationResponse response = venice.getNewStoreVersion();
      venice.useControllerClient(controllerClient -> {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS,
            () -> assertEquals(controllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(), ExecutionStatus.STARTED.toString()));
      });

      List<VeniceControllerWrapper> oldControllers = venice.getVeniceControllers();
      List<VeniceControllerWrapper> newControllers = new ArrayList<>();
      LiveInstance clusterLeader = helixAsAServiceWrapper.getClusterLeader(venice.getClusterName());
      assertNotNull(clusterLeader,
          "Could not find the cluster leader from HAAS!");
      assertFalse(clusterLeader.getId().startsWith(HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX),
          "The cluster leader should not start with: " + HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX);

      // Start the rolling bounce process
      for (VeniceControllerWrapper oldController : oldControllers) {
        venice.stopVeniceController(oldController.getPort());
        oldController.close();
        newControllers.add(venice.addVeniceController(enableControllerAndStorageClusterHAASProperties));
      }

      TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        LiveInstance newClusterLeader = helixAsAServiceWrapper.getClusterLeader(venice.getClusterName());
        assertNotNull(newClusterLeader,
            "Could not find the cluster leader from HAAS after the rolling bounce of all controllers!");
        assertTrue(newClusterLeader.getId().startsWith(HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX),
            "The cluster leader should start with: " + HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX);
      });

      venice.useControllerClient(controllerClient -> {
        // Make sure the previous ongoing push can be completed.
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true,
            () -> TestUtils.assertCommand(
                controllerClient.writeEndOfPush(response.getName(), Version.parseVersionFromKafkaTopicName(response.getKafkaTopic()))
            )
        );
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          JobStatusQueryResponse jobStatusQueryResponse = controllerClient.queryJobStatus(response.getKafkaTopic());
          assertFalse(jobStatusQueryResponse.isError(), "JobStatusQueryResponse error: " + jobStatusQueryResponse.getError());
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.COMPLETED.toString());
        });
      });
    }
  }

  private static class InitTask implements Callable<Void> {

    private final HelixAdminClient client;
    private final HashMap<String, String> helixClusterProperties;

    public InitTask(HelixAdminClient client) {
      this.client = client;
      helixClusterProperties = new HashMap<>();
      helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    }

    @Override
    public Void call() throws Exception {
      client.createVeniceControllerCluster(false);
      client.addClusterToGrandCluster("venice-controllers");
      for (int i = 0; i < 10; i++) {
        String clusterName = "cluster-" + String.valueOf(i);
        client.createVeniceStorageCluster(clusterName, new HashMap<>(), false);
        client.addClusterToGrandCluster(clusterName);
        client.addVeniceStorageClusterToControllerCluster(clusterName);
      }
      return null;
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testConcurrentClusterInitialization() throws InterruptedException, ExecutionException {
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(0, 0, 0, 1);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerMultiClusterConfig controllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
      doReturn(helixAsAServiceWrapper.getZkAddress()).when(controllerMultiClusterConfig).getZkAddress();
      doReturn(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME).when(controllerMultiClusterConfig).getControllerHAASSuperClusterName();
      doReturn("venice-controllers").when(controllerMultiClusterConfig).getControllerClusterName();
      doReturn(3).when(controllerMultiClusterConfig).getControllerClusterReplica();
      ExecutorService
          executorService = new ThreadPoolExecutor(3, 3, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new DaemonThreadFactory("test-concurrent-cluster-init"));
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        tasks.add(new InitTask(new ZkHelixAdminClient(controllerMultiClusterConfig, new MetricsRepository())));
      }
      List<Future<Void>> results = executorService.invokeAll(tasks);
      for (Future<Void> result : results) {
        result.get();
      }
    }
  }

  private HelixAsAServiceWrapper startAndWaitForHAASToBeAvailable(String zkAddress) {
    HelixAsAServiceWrapper helixAsAServiceWrapper = ServiceFactory.getHelixController(zkAddress);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true,
        () -> assertNotNull(helixAsAServiceWrapper.getSuperClusterLeader(),
            "Helix super cluster doesn't have a leader yet"));
    return helixAsAServiceWrapper;
  }
}
