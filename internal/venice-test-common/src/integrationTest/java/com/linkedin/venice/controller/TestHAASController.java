package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigConstants.CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.shutdownExecutor;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.HelixAsAServiceWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyKey;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHAASController {
  private Properties enableControllerClusterHAASProperties;
  private Properties enableControllerAndStorageClusterHAASProperties;

  @BeforeClass
  public void setUp() {
    enableControllerClusterHAASProperties = new Properties();
    enableControllerClusterHAASProperties.put(ConfigKeys.CONTROLLER_CLUSTER_LEADER_HAAS, String.valueOf(true));
    enableControllerClusterHAASProperties
        .put(ConfigKeys.CONTROLLER_HAAS_SUPER_CLUSTER_NAME, HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME);
    enableControllerAndStorageClusterHAASProperties = (Properties) enableControllerClusterHAASProperties.clone();
    enableControllerAndStorageClusterHAASProperties
        .put(ConfigKeys.VENICE_STORAGE_CLUSTER_LEADER_HAAS, String.valueOf(true));
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testClusterResourceInstanceTag_updateOnRestart() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      String instanceTag = "GENERAL";
      String newInstanceTag = "NEW";
      String controllerClusterName = "venice-controllers";

      Properties clusterProperties = (Properties) enableControllerAndStorageClusterHAASProperties.clone();
      clusterProperties.put(ConfigKeys.CONTROLLER_RESOURCE_INSTANCE_GROUP_TAG, instanceTag);
      clusterProperties.put(ConfigKeys.CONTROLLER_INSTANCE_TAG_LIST, instanceTag);

      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(clusterProperties);

      HelixAdmin helixAdmin = controllerWrapper.getVeniceHelixAdmin().getHelixAdmin();
      String instanceName = controllerWrapper.getHost() + "_" + controllerWrapper.getPort();
      List<String> resources = helixAdmin.getResourcesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(resources.size(), 1);
      List<String> instances = helixAdmin.getInstancesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(instances.size(), 1);
      // Stop controller
      venice.stopVeniceController(controllerWrapper.getPort());

      // Modify tags in ZK directly
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(controllerClusterName, instanceName);
      instanceConfig.removeTag(instanceTag);
      instanceConfig.addTag(newInstanceTag);
      helixAdmin.setInstanceConfig(controllerClusterName, instanceName, instanceConfig);

      Assert.assertTrue(
          helixAdmin.getInstanceConfig(controllerClusterName, instanceName)
              .getTags()
              .equals(Arrays.asList(newInstanceTag)),
          newInstanceTag + " tag should be added to the instance config");

      venice.restartVeniceController(controllerWrapper.getPort());

      // Wait for the controller to rejoin the cluster and use the tag declared in it's clusterProperties
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        InstanceConfig updatedConfig = helixAdmin.getInstanceConfig(controllerClusterName, instanceName);
        List<String> tags = updatedConfig.getTags();
        Assert.assertTrue(
            tags.equals(Arrays.asList(instanceTag)),
            "The instance tag should be updated to the one in the cluster properties");
      });
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testClusterResourceInstanceTag() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      String instanceTag = "GENERAL";
      String controllerClusterName = "venice-controllers";

      Properties clusterProperties = (Properties) enableControllerAndStorageClusterHAASProperties.clone();
      clusterProperties.put(ConfigKeys.CONTROLLER_RESOURCE_INSTANCE_GROUP_TAG, instanceTag);
      clusterProperties.put(ConfigKeys.CONTROLLER_INSTANCE_TAG_LIST, instanceTag);

      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(clusterProperties);

      HelixAdmin helixAdmin = controllerWrapper.getVeniceHelixAdmin().getHelixAdmin();
      List<String> resources = helixAdmin.getResourcesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(resources.size(), 1);
      List<String> instances = helixAdmin.getInstancesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(instances.size(), 1);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testClusterResourceEmptyInstanceTag() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      String instanceTag = "";
      String controllerClusterName = "venice-controllers";

      Properties clusterProperties = (Properties) enableControllerAndStorageClusterHAASProperties.clone();
      clusterProperties.put(ConfigKeys.CONTROLLER_RESOURCE_INSTANCE_GROUP_TAG, instanceTag);
      clusterProperties.put(ConfigKeys.CONTROLLER_INSTANCE_TAG_LIST, instanceTag);

      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(clusterProperties);

      HelixAdmin helixAdmin = controllerWrapper.getVeniceHelixAdmin().getHelixAdmin();
      List<String> resources = helixAdmin.getResourcesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(resources.size(), 0);
      List<String> instances = helixAdmin.getInstancesInClusterWithTag(controllerClusterName, instanceTag);
      assertEquals(instances.size(), 0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testStartHAASHelixControllerAsControllerClusterLeader() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(enableControllerClusterHAASProperties);
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertTrue(
              controllerWrapper.isLeaderControllerOfControllerCluster(),
              "The only Venice controller should be appointed the colo leader"));
      assertTrue(
          controllerWrapper.isLeaderController(venice.getClusterName()),
          "The only Venice controller should be the leader of Venice cluster " + venice.getClusterName());
      venice.addVeniceServer(new Properties(), new Properties());
      NewStoreResponse response = assertCommand(venice.getNewStore(Utils.getUniqueString("venice-store")));
      venice.useControllerClient(
          controllerClient -> assertCommand(
              controllerClient
                  .sendEmptyPushAndWait(response.getName(), Utils.getUniqueString(), 100, 30 * Time.MS_PER_SECOND)));
      String topicName = Version.composeKafkaTopic(response.getName(), 1);
      venice.useControllerClient(
          controllerClient -> waitForNonDeterministicAssertion(
              60,
              TimeUnit.SECONDS,
              () -> assertEquals(
                  assertCommand(controllerClient.queryJobStatus(topicName)).getStatus(),
                  ExecutionStatus.COMPLETED.toString())));
    }
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testTransitionToHAASControllerAsControllerClusterLeader() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(3)
        .numberOfServers(1)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      NewStoreResponse response = venice.getNewStore(Utils.getUniqueString("venice-store"));
      venice.useControllerClient(
          controllerClient -> assertCommand(
              controllerClient
                  .sendEmptyPushAndWait(response.getName(), Utils.getUniqueString(), 100, 30 * Time.MS_PER_MINUTE)));
      List<VeniceControllerWrapper> oldControllers = venice.getVeniceControllers();
      List<VeniceControllerWrapper> newControllers = new ArrayList<>();
      // Start the rolling bounce process
      for (VeniceControllerWrapper oldController: oldControllers) {
        venice.stopVeniceController(oldController.getPort());
        oldController.close();
        newControllers.add(venice.addVeniceController(enableControllerClusterHAASProperties));
      }
      waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
        for (VeniceControllerWrapper newController: newControllers) {
          if (newController.isLeaderController(venice.getClusterName())) {
            assertTrue(
                newController.isLeaderControllerOfControllerCluster(),
                "The colo leader Venice controller should be the leader of the only Venice cluster");
            return true;
          }
        }
        return false;
      });

      // Make sure the previous ongoing push can be completed.
      String topicName = Version.composeKafkaTopic(response.getName(), 1);
      venice.useControllerClient(
          controllerClient -> waitForNonDeterministicAssertion(
              30,
              TimeUnit.SECONDS,
              () -> assertEquals(
                  assertCommand(controllerClient.queryJobStatus(topicName)).getStatus(),
                  ExecutionStatus.COMPLETED.toString())));
    }
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testStartHAASControllerAsStorageClusterLeader() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerWrapper controllerWrapper =
          venice.addVeniceController(enableControllerAndStorageClusterHAASProperties);
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> assertTrue(
              controllerWrapper.isLeaderController(venice.getClusterName()),
              "The only Venice controller should become the leader of the only Venice cluster"));
      venice.addVeniceServer(new Properties(), new Properties());
      venice.addVeniceServer(new Properties(), new Properties());
      NewStoreResponse response = venice.getNewStore(Utils.getUniqueString("venice-store"));
      venice.useControllerClient(
          controllerClient -> assertCommand(
              controllerClient
                  .sendEmptyPushAndWait(response.getName(), Utils.getUniqueString(), 100, 30 * Time.MS_PER_MINUTE)));
      String topicName = Version.composeKafkaTopic(response.getName(), 1);
      venice.useControllerClient(
          controllerClient -> waitForNonDeterministicAssertion(
              30,
              TimeUnit.SECONDS,
              () -> assertEquals(
                  assertCommand(controllerClient.queryJobStatus(topicName)).getStatus(),
                  ExecutionStatus.COMPLETED.toString())));
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testTransitionToHAASControllerAsStorageClusterLeader() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(3)
        .numberOfServers(1)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {

      NewStoreResponse response = venice.getNewStore(Utils.getUniqueString("venice-store"));
      venice.useControllerClient(
          controllerClient -> assertCommand(
              controllerClient
                  .sendEmptyPushAndWait(response.getName(), Utils.getUniqueString(), 100, 30 * Time.MS_PER_MINUTE)));
      List<VeniceControllerWrapper> oldControllers = venice.getVeniceControllers();
      LiveInstance clusterLeader = helixAsAServiceWrapper.getClusterLeader(venice.getClusterName());
      assertNotNull(clusterLeader, "Could not find the cluster leader from HAAS!");
      assertFalse(
          clusterLeader.getId().startsWith(HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX),
          "The cluster leader should not start with: " + HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX);

      // Start the rolling bounce process
      for (VeniceControllerWrapper oldController: oldControllers) {
        venice.stopVeniceController(oldController.getPort());
        oldController.close();
        venice.addVeniceController(enableControllerAndStorageClusterHAASProperties);
      }

      waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
        LiveInstance newClusterLeader = helixAsAServiceWrapper.getClusterLeader(venice.getClusterName());
        assertNotNull(
            newClusterLeader,
            "Could not find the cluster leader from HAAS after the rolling bounce of all controllers!");
        assertTrue(
            newClusterLeader.getId().startsWith(HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX),
            "The cluster leader should start with: " + HelixAsAServiceWrapper.HELIX_INSTANCE_NAME_PREFIX);
      });

      venice.useControllerClient(controllerClient -> {
        // Make sure the previous ongoing push can be completed.
        String topicName = Version.composeKafkaTopic(response.getName(), 1);
        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          JobStatusQueryResponse jobStatusQueryResponse = assertCommand(controllerClient.queryJobStatus(topicName));
          assertEquals(jobStatusQueryResponse.getStatus(), ExecutionStatus.COMPLETED.toString());
        });
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testRebalancePreferenceAndCapacityKeys() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(0).numberOfServers(0).numberOfRouters(0).build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      String controllerClusterName = "venice-controllers";

      int helixRebalancePreferenceEvenness = 10;
      int helixRebalancePreferenceLessMovement = 2;
      int helixRebalancePreferenceForceBaselineConverge = 1;
      int helixInstanceCapacity = 1000;
      int helixResourceCapacityWeight = 10;

      Properties clusterProperties = (Properties) enableControllerAndStorageClusterHAASProperties.clone();
      clusterProperties
          .put(ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_EVENNESS, helixRebalancePreferenceEvenness);
      clusterProperties
          .put(ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_LESS_MOVEMENT, helixRebalancePreferenceLessMovement);
      clusterProperties.put(
          ConfigKeys.CONTROLLER_HELIX_REBALANCE_PREFERENCE_FORCE_BASELINE_CONVERGE,
          helixRebalancePreferenceForceBaselineConverge);
      clusterProperties.put(ConfigKeys.CONTROLLER_HELIX_INSTANCE_CAPACITY, helixInstanceCapacity);
      clusterProperties.put(ConfigKeys.CONTROLLER_HELIX_RESOURCE_CAPACITY_WEIGHT, helixResourceCapacityWeight);

      VeniceControllerWrapper controllerWrapper = venice.addVeniceController(clusterProperties);

      VeniceHelixAdmin veniceHelixAdmin = controllerWrapper.getVeniceHelixAdmin();

      SafeHelixManager helixManager = veniceHelixAdmin.getHelixManager();
      SafeHelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
      PropertyKey.Builder propertyKeyBuilder = new PropertyKey.Builder(controllerClusterName);
      ClusterConfig clusterConfig = helixDataAccessor.getProperty(propertyKeyBuilder.clusterConfig());

      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreference =
          clusterConfig.getGlobalRebalancePreference();
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS),
          helixRebalancePreferenceEvenness);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT),
          helixRebalancePreferenceLessMovement);
      assertEquals(
          (int) globalRebalancePreference.get(ClusterConfig.GlobalRebalancePreferenceKey.FORCE_BASELINE_CONVERGE),
          helixRebalancePreferenceForceBaselineConverge);

      List<String> instanceCapacityKeys = clusterConfig.getInstanceCapacityKeys();
      assertEquals(instanceCapacityKeys.size(), 1);

      Map<String, Integer> defaultInstanceCapacityMap = clusterConfig.getDefaultInstanceCapacityMap();
      assertEquals(
          (int) defaultInstanceCapacityMap.get(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY),
          helixInstanceCapacity);

      Map<String, Integer> defaultPartitionWeightMap = clusterConfig.getDefaultPartitionWeightMap();
      assertEquals(
          (int) defaultPartitionWeightMap.get(CONTROLLER_DEFAULT_HELIX_RESOURCE_CAPACITY_KEY),
          helixResourceCapacityWeight);
    }
  }

  private void initializeClusters(HelixAdminClient client, int parallelism) throws InterruptedException {
    ExecutorService executorService = new ThreadPoolExecutor(
        parallelism,
        parallelism,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("test-concurrent-cluster-init"));
    try {
      client.createVeniceControllerCluster();
      client.addClusterToGrandCluster("venice-controllers");
      CompletableFuture[] futures = new CompletableFuture[10];
      for (int i = 0; i < 10; i++) {
        String clusterName = "cluster-" + i;
        futures[i] = CompletableFuture.runAsync(() -> {
          client.createVeniceStorageCluster(clusterName, new ClusterConfig(clusterName), null);
          client.addClusterToGrandCluster(clusterName);
          client.addVeniceStorageClusterToControllerCluster(clusterName);
        }, executorService);
      }
      CompletableFuture.allOf(futures).join();
    } finally {
      shutdownExecutor(executorService);
    }
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testConcurrentClusterInitialization() throws InterruptedException {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer();
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(zk.getAddress())) {
      VeniceControllerMultiClusterConfig controllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
      doReturn(helixAsAServiceWrapper.getZkAddress()).when(controllerMultiClusterConfig).getZkAddress();
      doReturn(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME).when(controllerMultiClusterConfig)
          .getControllerHAASSuperClusterName();
      doReturn("venice-controllers").when(controllerMultiClusterConfig).getControllerClusterName();

      VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
      doReturn(3).when(clusterConfig).getControllerClusterReplica();
      doReturn("").when(clusterConfig).getControllerResourceInstanceGroupTag();
      doReturn(clusterConfig).when(controllerMultiClusterConfig).getControllerConfig(anyString());

      initializeClusters(new ZkHelixAdminClient(controllerMultiClusterConfig, new MetricsRepository()), 3);
    }
  }

  private HelixAsAServiceWrapper startAndWaitForHAASToBeAvailable(String zkAddress) {
    HelixAsAServiceWrapper helixAsAServiceWrapper = null;
    try {
      helixAsAServiceWrapper = ServiceFactory.getHelixController(zkAddress);
      final HelixAsAServiceWrapper finalHaas = helixAsAServiceWrapper;
      waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          true,
          () -> assertNotNull(finalHaas.getSuperClusterLeader(), "Helix super cluster doesn't have a leader yet"));
      return helixAsAServiceWrapper;
    } catch (Exception e) {
      Utils.closeQuietlyWithErrorLogged(helixAsAServiceWrapper);
      throw e;
    }
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testCloudConfig() throws InterruptedException {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer();
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(zk.getAddress())) {
      VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
      VeniceControllerMultiClusterConfig controllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

      List<String> cloudInfoSources = new ArrayList<>();
      cloudInfoSources.add("TestSource");

      when(commonConfig.isControllerClusterHelixCloudEnabled()).thenReturn(true);
      when(commonConfig.isStorageClusterHelixCloudEnabled()).thenReturn(true);
      CloudConfig cloudConfig = HelixUtils.getCloudConfig(
          CloudProvider.CUSTOMIZED,
          "NA",
          cloudInfoSources,
          "com.linkedin.venice.controller.helix",
          "TestProcessor");
      when(commonConfig.getHelixCloudConfig()).thenReturn(cloudConfig);

      doReturn(helixAsAServiceWrapper.getZkAddress()).when(controllerMultiClusterConfig).getZkAddress();
      doReturn(HelixAsAServiceWrapper.HELIX_SUPER_CLUSTER_NAME).when(controllerMultiClusterConfig)
          .getControllerHAASSuperClusterName();
      doReturn("venice-controllers").when(controllerMultiClusterConfig).getControllerClusterName();
      doReturn(3).when(commonConfig).getControllerClusterReplica();
      doReturn("").when(commonConfig).getControllerResourceInstanceGroupTag();
      doReturn(commonConfig).when(controllerMultiClusterConfig).getControllerConfig(anyString());
      doReturn(commonConfig).when(controllerMultiClusterConfig).getCommonConfig();

      ZkHelixAdminClient client = new ZkHelixAdminClient(controllerMultiClusterConfig, new MetricsRepository());
      initializeClusters(client, 1);
    }
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testHelixUnknownInstanceOperation() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(0)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .build();
    try (VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(options);
        HelixAsAServiceWrapper helixAsAServiceWrapper = startAndWaitForHAASToBeAvailable(venice.getZk().getAddress())) {
      VeniceControllerWrapper controllerWrapper =
          venice.addVeniceController(enableControllerAndStorageClusterHAASProperties);
      Properties serverProperties = new Properties();
      serverProperties.put(ConfigKeys.SERVER_HELIX_JOIN_AS_UNKNOWN, true);
      venice.addVeniceServer(new Properties(), serverProperties);

      HelixAdmin helixAdmin = controllerWrapper.getVeniceHelixAdmin().getHelixAdmin();
      String clusterName = venice.getClusterName();
      List<String> instances = helixAdmin.getInstancesInCluster(clusterName);
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instances.get(0));
      assertEquals(instanceConfig.getInstanceOperation().getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
    }
  }
}
