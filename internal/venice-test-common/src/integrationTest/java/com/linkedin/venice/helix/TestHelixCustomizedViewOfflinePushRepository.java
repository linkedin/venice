package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test case for {@link HelixCustomizedViewOfflinePushRepository} and {@link HelixHybridStoreQuotaRepository}
 */
public class TestHelixCustomizedViewOfflinePushRepository {
  // Test behavior configuration
  private static final int WAIT_TIME = 1000; // FIXME: Non-deterministic. Will lead to flaky tests.

  private SafeHelixManager manager0, manager1;
  private SafeHelixManager controller;
  private HelixAdmin admin;
  private String clusterName = "UnitTestCLuster";
  private String resourceName = "storeName_v1";
  private String storeName = "storeName";
  private String zkAddress;
  private int httpPort0, httpPort1;
  private int adminPort;
  private int partitionId0, partitionId1, partitionId2;
  private ZkServerWrapper zkServerWrapper;
  private HelixHybridStoreQuotaRepository hybridStoreQuotaOnlyRepository;
  private HelixCustomizedViewOfflinePushRepository offlinePushOnlyRepository;
  private SafeHelixManager readManager;
  private HelixPartitionStatusAccessor accessor0, accessor1;

  @BeforeMethod(alwaysRun = true)
  public void setupHelix() throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(clusterName);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

    admin.addResource(
        clusterName,
        resourceName,
        3,
        LeaderStandbySMD.name,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 2);

    // Build customized state config and update to Zookeeper
    HelixUtils.setupCustomizedStateConfig(admin, clusterName);

    partitionId0 = 0;
    partitionId1 = 1;
    partitionId2 = 2;
    httpPort0 = 50000 + (int) (System.currentTimeMillis() % 10000);
    httpPort1 = 50000 + (int) (System.currentTimeMillis() % 10000) + 1;
    adminPort = 50000 + (int) (System.currentTimeMillis() % 10000) + 2;

    controller = new SafeHelixManager(
        HelixControllerMain.startHelixController(
            zkAddress,
            clusterName,
            Utils.getHelixNodeIdentifier(Utils.getHostName(), adminPort),
            HelixControllerMain.STANDALONE));

    manager0 = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort0),
        zkAddress,
        httpPort0,
        LeaderStandbySMD.name);
    manager0.connect();
    Thread.sleep(WAIT_TIME);
    accessor0 = new HelixPartitionStatusAccessor(manager0.getOriginalManager(), manager0.getInstanceName(), false);

    manager1 = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort1),
        zkAddress,
        httpPort1,
        LeaderStandbySMD.name);
    manager1.connect();

    Thread.sleep(WAIT_TIME);
    accessor1 = new HelixPartitionStatusAccessor(manager1.getOriginalManager(), manager1.getInstanceName(), true);

    readManager = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, "reader", InstanceType.SPECTATOR, zkAddress));
    readManager.connect();
    hybridStoreQuotaOnlyRepository = new HelixHybridStoreQuotaRepository(readManager);
    ZkClient zkClient = ZkClientFactory.newZkClient(zkAddress);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    HelixReadWriteStoreRepository writeStoreRepository = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        clusterName,
        Optional.empty(),
        new ClusterLockManager(clusterName));
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setPartitionCount(3);
    Version version = new VersionImpl(storeName, 1, "pushId");
    version.setPartitionCount(3);
    store.addVersion(version);
    writeStoreRepository.addStore(store);
    offlinePushOnlyRepository = new HelixCustomizedViewOfflinePushRepository(readManager, writeStoreRepository, false);
    hybridStoreQuotaOnlyRepository.refresh();
    offlinePushOnlyRepository.refresh();
    // Update customized state for each partition on each instance
    accessor0.updateReplicaStatus(resourceName, partitionId0, ExecutionStatus.COMPLETED);
    accessor0.updateReplicaStatus(resourceName, partitionId1, ExecutionStatus.END_OF_PUSH_RECEIVED);
    accessor1.updateReplicaStatus(resourceName, partitionId0, ExecutionStatus.END_OF_PUSH_RECEIVED);
    accessor1.updateReplicaStatus(resourceName, partitionId1, ExecutionStatus.COMPLETED);

    accessor0.updateHybridQuotaReplicaStatus(resourceName, partitionId0, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
    accessor0.updateHybridQuotaReplicaStatus(resourceName, partitionId1, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
    accessor1.updateHybridQuotaReplicaStatus(resourceName, partitionId1, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
    accessor1.updateHybridQuotaReplicaStatus(resourceName, partitionId2, HybridStoreQuotaStatus.QUOTA_VIOLATED);

    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> offlinePushOnlyRepository.containsKafkaTopic(resourceName)
            && offlinePushOnlyRepository.getReplicaStates(resourceName, partitionId0).size() == 2
            && offlinePushOnlyRepository.getReplicaStates(resourceName, partitionId1).size() == 2);

  }

  @AfterMethod(alwaysRun = true)
  public void cleanupHelix() {
    manager0.disconnect();
    manager1.disconnect();
    readManager.disconnect();
    controller.disconnect();
    admin.dropCluster(clusterName);
    admin.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetQuotaExceedStores() throws Exception {
    String fakeResourceName = "FakeUnitTest";
    Assert.assertEquals(
        HybridStoreQuotaStatus.UNKNOWN,
        hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(fakeResourceName));
    Assert.assertEquals(1, hybridStoreQuotaOnlyRepository.getHybridQuotaViolatedStores().size());
    Assert.assertEquals(resourceName, hybridStoreQuotaOnlyRepository.getHybridQuotaViolatedStores().get(0));
    Assert.assertEquals(
        HybridStoreQuotaStatus.QUOTA_VIOLATED,
        hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(resourceName));

    // Change resource from quota violated to quota not violated.
    accessor1.updateHybridQuotaReplicaStatus(resourceName, partitionId2, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(0, hybridStoreQuotaOnlyRepository.getHybridQuotaViolatedStores().size());
    Assert.assertEquals(
        HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED,
        hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(resourceName));

    // Change resource from quota not violated to quota violated.
    accessor0.updateHybridQuotaReplicaStatus(resourceName, partitionId1, HybridStoreQuotaStatus.QUOTA_VIOLATED);
    Thread.sleep(WAIT_TIME);
    Assert.assertEquals(0, hybridStoreQuotaOnlyRepository.getHybridQuotaViolatedStores().size());
    // But accessor0 can not report hybrid quota status, since helix hybrid store quota is not enabled.
    Assert.assertEquals(
        HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED,
        hybridStoreQuotaOnlyRepository.getHybridStoreQuotaStatus(resourceName));
  }

  @Test
  public void testGetInstances() throws Exception {
    List<Instance> instances = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId0);
    Assert.assertEquals(1, instances.size());
    Instance instance = instances.get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort0, instance.getPort());

    instances = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId1);
    Assert.assertEquals(1, instances.size());
    instance = instances.get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort1, instance.getPort());

    accessor1.updateReplicaStatus(resourceName, partitionId0, ExecutionStatus.COMPLETED);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId0);
      Assert.assertEquals(instancesList.size(), 2);
      Assert.assertEquals(
          new HashSet<>(Arrays.asList(instancesList.get(0).getPort(), instancesList.get(1).getPort())),
          new HashSet<>(Arrays.asList(httpPort0, httpPort1)));
    });

    accessor0.updateReplicaStatus(resourceName, partitionId1, ExecutionStatus.COMPLETED);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId1);
      Assert.assertEquals(instancesList.size(), 2);
      Assert.assertEquals(
          new HashSet<>(Arrays.asList(instancesList.get(0).getPort(), instancesList.get(1).getPort())),
          new HashSet<>(Arrays.asList(httpPort0, httpPort1)));
    });

    // Participant become offline.
    manager0.disconnect();
    // Wait for notification.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId0);
      Assert.assertEquals(1, instancesList.size());
      instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId1);
      Assert.assertEquals(1, instancesList.size());
    });

    // Add a new participant
    int newHttpPort = httpPort0 + 10;
    SafeHelixManager newManager = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), newHttpPort),
        zkAddress,
        newHttpPort,
        LeaderStandbySMD.name);
    newManager.connect();
    HelixPartitionStatusAccessor newAccessor =
        new HelixPartitionStatusAccessor(newManager.getOriginalManager(), newManager.getInstanceName(), true);
    newAccessor.updateReplicaStatus(resourceName, partitionId0, ExecutionStatus.COMPLETED);
    newAccessor.updateReplicaStatus(resourceName, partitionId1, ExecutionStatus.COMPLETED);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Instance> instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId0);
      Assert.assertEquals(instancesList.size(), 2);
      instancesList = offlinePushOnlyRepository.getReadyToServeInstances(resourceName, partitionId1);
      Assert.assertEquals(instancesList.size(), 2);
    });
    newManager.disconnect();
  }

  @Test
  public void testGetReplicaStates() {
    List<ReplicaState> replicaStates = offlinePushOnlyRepository.getReplicaStates(resourceName, partitionId0);
    Assert.assertEquals(replicaStates.size(), 2, "Unexpected replication factor");
    for (ReplicaState replicaState: replicaStates) {
      Assert.assertEquals(replicaState.getPartition(), partitionId0, "Unexpected partition number");
      Assert.assertNotNull(replicaState.getParticipantId(), "Participant id should not be null");
      boolean readyToServe = replicaState.isReadyToServe();
      ExecutionStatus pushStatus = replicaState.getVenicePushStatus();
      Assert.assertEquals(
          replicaState.isReadyToServe(),
          pushStatus.equals(ExecutionStatus.COMPLETED),
          "readyToServe == " + readyToServe + " but the push status is: " + pushStatus);
    }
  }

  @Test
  public void testGetNumberOfPartitions() throws Exception {
    Assert.assertEquals(3, offlinePushOnlyRepository.getNumberOfPartitions(resourceName));
    Assert.assertEquals(3, offlinePushOnlyRepository.getNumberOfPartitions(resourceName));
    // Participant become offline.
    manager0.disconnect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    // Partition will not change
    Assert.assertEquals(3, offlinePushOnlyRepository.getNumberOfPartitions(resourceName));
    Assert.assertEquals(3, offlinePushOnlyRepository.getNumberOfPartitions(resourceName));
  }

  @Test
  public void testGetNumberOfPartitionsWhenResourceDropped() throws Exception {
    Assert.assertTrue(admin.getResourcesInCluster(clusterName).contains(resourceName));
    offlinePushOnlyRepository.getNumberOfPartitions(resourceName);
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    admin.dropResource(clusterName, resourceName);
    accessor0.deleteReplicaStatus(resourceName, partitionId0);
    accessor0.deleteReplicaStatus(resourceName, partitionId1);
    accessor1.deleteReplicaStatus(resourceName, partitionId0);
    accessor1.deleteReplicaStatus(resourceName, partitionId1);
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertFalse(admin.getResourcesInCluster(clusterName).contains(resourceName));
    try {
      // Should not find the resource.
      offlinePushOnlyRepository.getNumberOfPartitions(resourceName);
      Assert.fail("Exception should be thrown because resource does not exist now.");
    } catch (VeniceException e) {
      // Expected
    }
  }

  @Test
  public void testGetPartitions() throws Exception {
    PartitionAssignment customizedPartitionAssignment = offlinePushOnlyRepository.getPartitionAssignments(resourceName);
    Assert.assertEquals(2, customizedPartitionAssignment.getAssignedNumberOfPartitions());
    Assert.assertEquals(
        1,
        customizedPartitionAssignment.getPartition(partitionId0).getInstancesInState(ExecutionStatus.COMPLETED).size());
    Assert.assertEquals(0, customizedPartitionAssignment.getPartition(partitionId0).getWorkingInstances().size());

    Instance instance =
        customizedPartitionAssignment.getPartition(partitionId0).getInstancesInState(ExecutionStatus.COMPLETED).get(0);
    Assert.assertEquals(Utils.getHostName(), instance.getHost());
    Assert.assertEquals(httpPort0, instance.getPort());

    // Get assignment from external view
    PartitionAssignment partitionAssignment = offlinePushOnlyRepository.getPartitionAssignments(resourceName);
    List<Instance> liveInstances = partitionAssignment.getPartition(partitionId0).getWorkingInstances();
    // customized view does not have working instances
    Assert.assertEquals(0, liveInstances.size());

    // Participant become offline.
    manager0.disconnect();
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    customizedPartitionAssignment = offlinePushOnlyRepository.getPartitionAssignments(resourceName);
    Assert.assertEquals(2, customizedPartitionAssignment.getAssignedNumberOfPartitions());
    Assert.assertEquals(
        0,
        customizedPartitionAssignment.getPartition(partitionId0).getInstancesInState(ExecutionStatus.COMPLETED).size());
  }

  @Test
  public void testListeners() throws Exception {
    final boolean[] isNoticed = { false, false, false, false };
    RoutingDataRepository.RoutingDataChangedListener listener = new RoutingDataRepository.RoutingDataChangedListener() {
      @Override
      public void onExternalViewChange(PartitionAssignment partitionAssignment) {
        isNoticed[0] = true;
      }

      @Override
      public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
        isNoticed[1] = true;
      }

      @Override
      public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
        isNoticed[2] = true;
      }

      @Override
      public void onRoutingDataDeleted(String kafkaTopic) {
        isNoticed[3] = true;
      }
    };

    offlinePushOnlyRepository.unSubscribeRoutingDataChange(resourceName, listener);
    // Wait notification.
    Thread.sleep(WAIT_TIME);
    Assert.assertFalse(isNoticed[0], "Should not get notification after un-registering.");
    Assert.assertFalse(isNoticed[1], "Should not get notification after un-registering.");
    Assert.assertFalse(isNoticed[2], "Should not get notification after un-registering.");
    Assert.assertFalse(isNoticed[3], "Should not get notification after un-registering.");

    offlinePushOnlyRepository.subscribeRoutingDataChange(resourceName, listener);

    accessor0.deleteReplicaStatus(resourceName, partitionId0);
    accessor0.deleteReplicaStatus(resourceName, partitionId1);
    accessor1.deleteReplicaStatus(resourceName, partitionId0);
    accessor1.deleteReplicaStatus(resourceName, partitionId1);
    // Wait notification.
    Thread.sleep(WAIT_TIME);

    Assert.assertFalse(isNoticed[0], "Should not get notification after resource is deleted.");
    Assert.assertFalse(isNoticed[1], "Should not get notification after resource is deleted.");
    Assert.assertFalse(isNoticed[2], "Should not get notification after resource is deleted.");
    Assert.assertTrue(isNoticed[3], "There is a resource deleted.");
  }
}
