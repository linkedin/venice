package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.utils.MockTestStateModel;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test case for HelixRoutingDataRepository.
 */
public class TestHelixExternalViewRepository {
  // Test behavior configuration
  private static final int WAIT_TIME = 1000; // FIXME: Non-deterministic. Will lead to flaky tests.

  private SafeHelixManager manager;
  private SafeHelixManager controller;
  private HelixAdmin admin;
  private String clusterName = "UnitTestCLuster";
  private String resourceName = "UnitTest";
  private String zkAddress;
  private int httpPort;
  private int adminPort;
  private ZkServerWrapper zkServerWrapper;
  private HelixExternalViewRepository repository;
  private SafeHelixManager readManager;

  @BeforeMethod(alwaysRun = true)
  public void setupHelix() throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(clusterName);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(clusterName, MockTestStateModel.UNIT_TEST_STATE_MODEL, MockTestStateModel.getDefinition());

    admin.addResource(
        clusterName,
        resourceName,
        1,
        MockTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 1);

    httpPort = 50000 + (int) (System.currentTimeMillis() % 10000); // port never actually used
    adminPort = 50000 + (int) (System.currentTimeMillis() % 10000) + 1; // port never actually used
    controller = new SafeHelixManager(
        HelixControllerMain.startHelixController(
            zkAddress,
            clusterName,
            Utils.getHelixNodeIdentifier(Utils.getHostName(), adminPort),
            HelixControllerMain.STANDALONE));

    manager = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort),
        zkAddress,
        httpPort,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    manager.connect();
    // Waiting essential notification from ZK. TODO: use a listener to find out when ZK is ready
    Thread.sleep(WAIT_TIME);

    readManager = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, "reader", InstanceType.SPECTATOR, zkAddress));
    readManager.connect();
    repository = new HelixExternalViewRepository(readManager);
    repository.refresh();
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> repository.containsKafkaTopic(resourceName));
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupHelix() {
    manager.disconnect();
    readManager.disconnect();
    controller.disconnect();
    admin.dropCluster(clusterName);
    admin.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetInstances() throws Exception {

    List<Instance> instances = repository.getReadyToServeInstances(resourceName, 0);
    Assert.assertEquals(1, instances.size());
    Instance instance = instances.get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getPort());
    // Participant become offline.
    manager.disconnect();
    // Wait for notification.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = repository.getReadyToServeInstances(resourceName, 0);
      Assert.assertEquals(0, instancesList.size());
    });
    // No online instance now.
    instances = repository.getReadyToServeInstances(resourceName, 0);
    Assert.assertEquals(0, instances.size());
    int newHttpPort = httpPort + 10;
    SafeHelixManager newManager = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), newHttpPort),
        zkAddress,
        newHttpPort,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    newManager.connect();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = repository.getReadyToServeInstances(resourceName, 0);
      Assert.assertEquals(instancesList.size(), 1);
      Assert.assertEquals(instancesList.get(0).getPort(), newHttpPort);
    });
    newManager.disconnect();
  }

  @Test
  public void testGetNumberOfPartitions() throws Exception {
    Assert.assertEquals(1, repository.getNumberOfPartitions(resourceName));
    // Participant become offline.
    manager.disconnect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    // Result should be same.
    Assert.assertEquals(1, repository.getNumberOfPartitions(resourceName));
  }

  @Test(groups = { "flaky" })
  public void testGetNumberOfPartitionsWhenResourceDropped() throws Exception {
    Assert.assertTrue(admin.getResourcesInCluster(clusterName).contains(resourceName));
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    admin.dropResource(clusterName, resourceName);
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertFalse(admin.getResourcesInCluster(clusterName).contains(resourceName));
    try {
      // Should not find the resource.
      repository.getNumberOfPartitions(resourceName);
      Assert.fail("IAE should be thrown because resource does not exist now.");
    } catch (VeniceException iae) {
      // expected
    }
  }

  @Test
  public void testGetPartitions() throws Exception {
    PartitionAssignment partitionAssignment = repository.getPartitionAssignments(resourceName);
    Assert.assertEquals(1, partitionAssignment.getAssignedNumberOfPartitions());
    Assert.assertEquals(1, partitionAssignment.getPartition(0).getWorkingInstances().size());
    Assert.assertEquals(1, partitionAssignment.getPartition(0).getWorkingInstances().size());

    Instance instance = partitionAssignment.getPartition(0).getWorkingInstances().get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort, instance.getPort());

    Instance liveInstance = partitionAssignment.getPartition(0).getWorkingInstances().get(0);
    Assert.assertEquals(liveInstance, instance);

    // Participant become offline.
    manager.disconnect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    partitionAssignment = repository.getPartitionAssignments(resourceName);
    // No online partition now
    Assert.assertEquals(0, partitionAssignment.getAssignedNumberOfPartitions());
  }

  @Test
  public void testListeners() throws Exception {
    final boolean[] isNoticed = { false };
    RoutingDataRepository.RoutingDataChangedListener listener = new RoutingDataRepository.RoutingDataChangedListener() {
      @Override
      public void onExternalViewChange(PartitionAssignment partitionAssignment) {
        isNoticed[0] = true;
      }

      @Override
      public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
        isNoticed[0] = true;
      }

      @Override
      public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
        isNoticed[0] = true;
      }

      @Override
      public void onRoutingDataDeleted(String kafkaTopic) {
        isNoticed[0] = true;
      }
    };

    repository.subscribeRoutingDataChange(resourceName, listener);
    // Participant become offline.
    manager.disconnect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(isNoticed[0], true, "Can not get notification from repository.");

    isNoticed[0] = false;
    repository.unSubscribeRoutingDataChange(resourceName, listener);
    manager.connect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(isNoticed[0], false, "Should not get notification after un-registering.");
  }

  @Test
  public void testControllerChanged() throws Exception {
    Instance leaderController = repository.getLeaderController();
    Assert.assertEquals(leaderController.getHost(), Utils.getHostName());
    Assert.assertEquals(leaderController.getPort(), adminPort);

    // Start up stand by controller by different port
    int newAdminPort = adminPort + 1;
    SafeHelixManager newLeader = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(
            clusterName,
            Utils.getHelixNodeIdentifier(Utils.getHostName(), newAdminPort),
            InstanceType.CONTROLLER,
            zkAddress));
    newLeader.connect();
    // Stop leader and wait stand by become leader
    controller.disconnect();
    Thread.sleep(1000l);
    leaderController = repository.getLeaderController();
    Assert.assertEquals(leaderController.getHost(), Utils.getHostName());
    Assert.assertEquals(leaderController.getPort(), newAdminPort);

    newLeader.disconnect();
  }

  @Test
  public void testNodeChanged() {
    // Test initial conditions
    Assert.assertTrue(repository.getReadyToServeInstances(resourceName, 0).size() > 0);
    Assert.assertTrue(repository.getPartitionAssignments(resourceName).getAssignedNumberOfPartitions() > 0);

    manager.disconnect();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(repository.getReadyToServeInstances(resourceName, 0).size(), 0);
      Assert.assertEquals(repository.getPartitionAssignments(resourceName).getAssignedNumberOfPartitions(), 0);
    });
  }

  // @Test
  // TODO: Either delete this test or fix the getBootstrapping instances api to work with L/F (or make an equivalent new
  // api)
  public void testGetBootstrapInstances() throws Exception {
    manager.disconnect();
    VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor = new VeniceOfflinePushMonitorAccessor(
        clusterName,
        new ZkClient(zkAddress),
        new HelixAdapterSerializer(),
        3,
        1000);
    MockTestStateModelFactory factory = new MockTestStateModelFactory(offlinePushStatusAccessor);
    factory.setBlockTransition(true);
    manager = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort + 1),
        zkAddress,
        httpPort + 1,
        factory,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    manager.connect();
    Thread.sleep(WAIT_TIME);

    // because bootstrap to online transition is blocked, so there is only one bootstrap instance.
    Assert.assertEquals(
        repository.getReadyToServeInstances(resourceName, 0).size(),
        0,
        "Transition should be delayed, so there is no online instance.");
    Assert.assertEquals(repository.getPartitionAssignments(resourceName).getAssignedNumberOfPartitions(), 1);
    Assert.assertEquals(
        repository.getPartitionAssignments(resourceName).getPartition(0).getWorkingInstances().size(),
        1,
        "One bootstrap instance should be found");
    // make bootstrap to online transition completed, now there is one online instance.
    factory.makeTransitionCompleted(resourceName, 0);
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(
        repository.getReadyToServeInstances(resourceName, 0).size(),
        1,
        "One online instance should be found");
    Assert.assertEquals(repository.getPartitionAssignments(resourceName).getAssignedNumberOfPartitions(), 1);
    Assert.assertEquals(
        repository.getPartitionAssignments(resourceName).getPartition(0).getWorkingInstances().size(),
        1,
        "One online instance should be found");
    Assert.assertEquals(
        repository.getPartitionAssignments(resourceName).getPartition(0).getWorkingInstances().size(),
        1,
        "One online instance should be found");
  }

  @Test
  public void testPartitionMove() throws Exception {
    String resourceName = "testPartitionMove";
    admin.addResource(
        clusterName,
        resourceName,
        6,
        MockTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 1);

    SafeHelixManager newManager = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort + 1000),
        zkAddress,
        httpPort + 1000,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    newManager.connect();

    Thread.sleep(3000);
    System.out.println(httpPort);
    for (int i = 0; i < 6; i++) {
      System.out.println(repository.getReadyToServeInstances(resourceName, i).get(0).getNodeId());
    }

    admin.dropResource(clusterName, resourceName);

    Thread.sleep(3000);
    newManager.disconnect();
  }
}
