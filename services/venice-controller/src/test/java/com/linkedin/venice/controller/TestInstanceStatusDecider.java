package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestInstanceStatusDecider {
  private HelixVeniceClusterResources resources;
  private String clusterName;
  private SafeHelixDataAccessor accessor;
  private HelixCustomizedViewOfflinePushRepository routingDataRepository;
  private HelixReadWriteStoreRepository readWriteStoreRepository;
  private String storeName = "TestInstanceStatusDecider";
  private int version = 1;
  private String resourceName = Version.composeKafkaTopic(storeName, version);
  private PushMonitorDelegator mockMonitor;

  @BeforeMethod
  public void setUp() {
    clusterName = Utils.getUniqueString("TestInstanceStatusDecider");
    resources = mock(HelixVeniceClusterResources.class);
    routingDataRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    readWriteStoreRepository = mock(HelixReadWriteStoreRepository.class);
    mockMonitor = mock(PushMonitorDelegator.class);

    doAnswer(invocation -> {
      PartitionAssignment partitionAssignment = invocation.getArgument(0);
      int partitionId = invocation.getArgument(1);
      return partitionAssignment.getPartition(partitionId).getReadyToServeInstances();
    }).when(routingDataRepository).getReadyToServeInstances(any(PartitionAssignment.class), anyInt());

    SafeHelixManager manager = mock(SafeHelixManager.class);
    accessor = mock(SafeHelixDataAccessor.class);
    doReturn(routingDataRepository).when(resources).getCustomizedViewRepository();
    doReturn(readWriteStoreRepository).when(resources).getStoreMetadataRepository();
    doReturn(mockMonitor).when(resources).getPushMonitor();
    doReturn(manager).when(resources).getHelixManager();
    doReturn(accessor).when(manager).getHelixDataAccessor();
  }

  @Test
  public void testIsRemovableNonLiveInstance() {
    String liveInstanceId = "localhost_1";
    String nonLiveInstanceId = "test_1";
    List<Instance> instances = new ArrayList<>();
    instances.add(Instance.fromNodeId(liveInstanceId));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    helixStateToInstancesMap.put(HelixState.LEADER, instances);
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMap.put(ExecutionStatus.STARTED, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, helixStateToInstancesMap, executionStatusToInstancesMap));

    doReturn(null).when(accessor).getProperty(any(PropertyKey.class));
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, nonLiveInstanceId).isRemovable(),
        "A non-alive instance should be removable from cluster");
  }

  /**
   * Test is instance removable for a ready to serve version. If removing would cause any data loss, we can not remove
   * that instance.
   */
  @Test
  public void testIsRemovableLossData() {
    int partitionCount = 2;
    String instance1Name = "localhost_1";
    String instance2Name = "localhost_2";
    String instance3Name = "localhost_3";
    Instance instance1 = Instance.fromNodeId(instance1Name);
    Instance instance2 = Instance.fromNodeId(instance2Name);

    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);

    // Partition 0 have 2 online replicas
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instance1));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instance2));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0.put(ExecutionStatus.COMPLETED, Arrays.asList(instance1, instance2));
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    // Partition 1 only have 1 bootstrap replica but no online replica.
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP1 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP1.put(HelixState.LEADER, Arrays.asList(instance2));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP1 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP1.put(ExecutionStatus.STARTED, Arrays.asList(instance2));
    partitionAssignment
        .addPartition(new Partition(1, helixStateToInstancesMapForP1, executionStatusToInstancesMapForP1));

    // Test the completed push.
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 2);
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance1Name, Collections.emptyList()).isRemovable(),
        instance1Name + " could be removed because it's not the last online copy.");
    Assert.assertFalse(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance2Name, Collections.emptyList()).isRemovable(),
        instance2Name + " could NOT be removed because it the last online copy for partition 1.");

    // Locked node check when locked instance doesn't share any replica with the instance being removed
    Assert.assertTrue(
        InstanceStatusDecider
            .isRemovable(resources, clusterName, instance1Name, Collections.singletonList(instance3Name))
            .isRemovable(),
        instance1Name + " could be removed because it's not the last online copy.");
    Assert.assertFalse(
        InstanceStatusDecider
            .isRemovable(resources, clusterName, instance2Name, Collections.singletonList(instance3Name))
            .isRemovable(),
        instance2Name + " could NOT be removed because it the last online copy for partition 1.");

    // Locked node check when locked instance shares replicas with the instance being removed
    NodeRemovableResult nodeRemovableWillLoseDataResult = InstanceStatusDecider
        .isRemovable(resources, clusterName, instance1Name, Collections.singletonList(instance2Name));
    Assert.assertFalse(
        nodeRemovableWillLoseDataResult.isRemovable(),
        instance1Name + " could NOT be removed because it will be the last online copy after " + instance2Name
            + " is removed.");
    Assert.assertEquals(
        nodeRemovableWillLoseDataResult.getBlockingReason(),
        NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA.toString());
    Assert.assertEquals(
        nodeRemovableWillLoseDataResult.getFormattedMessage(),
        "WILL_LOSE_DATA(TestInstanceStatusDecider_v1: Partition 0 will have no online replicas after removing the node.)");
  }

  /**
   * Test if instance is removable during the push. If removing would fail the running job, we can not remove that
   * instance.
   */
  @Test
  public void testIsRemovableInFlightJob() {
    String instanceId = "localhost_1";
    List<Instance> instances = new ArrayList<>();
    instances.add(Instance.fromNodeId(instanceId));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    helixStateToInstancesMap.put(HelixState.LEADER, instances);
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMap.put(ExecutionStatus.STARTED, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, helixStateToInstancesMap, executionStatusToInstancesMap));
    // Test for the running push
    prepareStoreAndVersion(storeName, version, VersionStatus.STARTED, false, 1);

    NodeRemovableResult result = InstanceStatusDecider.isRemovable(resources, clusterName, instanceId);
    Assert
        .assertTrue(result.isRemovable(), "Instance should be removable because ongoing push shouldn't be a blocker.");
    Assert.assertEquals(result.getFormattedMessage(), "Instance is removable");
  }

  /**
   * Test if instance is removable for the ready to serve version. If removing would cause re-balance, we can remove that
   * instance.
   */
  @Test
  public void testIsRemovableTriggerRebalance() {
    int partitionCount = 2;
    String instance1Name = "localhost_1";
    Instance instance1 = Instance.fromNodeId(instance1Name);

    String instance2Name = "localhost_2";
    Instance instance2 = Instance.fromNodeId(instance2Name);

    String instance3Name = "localhost_3";
    Instance instance3 = Instance.fromNodeId(instance3Name);

    String instance4Name = "localhost_4";
    Instance instance4 = Instance.fromNodeId(instance4Name);

    String instance5Name = "localhost_5";

    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instance1));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instance2, instance3));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0.put(ExecutionStatus.COMPLETED, Arrays.asList(instance1, instance2, instance3));
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP1 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP1.put(HelixState.LEADER, Arrays.asList(instance1, instance4));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP1 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP1.put(ExecutionStatus.COMPLETED, Arrays.asList(instance1, instance4));
    partitionAssignment
        .addPartition(new Partition(1, helixStateToInstancesMapForP1, executionStatusToInstancesMapForP1));

    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance3Name, Collections.emptyList()).isRemovable(),
        "Instance should be removable because after removing one instance, there are 2 active replicas in partition 0, it will not trigger re-balance.");
    Assert.assertFalse(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance1Name, Collections.emptyList()).isRemovable(),
        "Instance should NOT be removable because after removing one instance, there are 1 active replicas in partition 1, it will trigger re-balance.");

    // Locked node check when locked instance doesn't share any replica with the instance being removed
    Assert.assertTrue(
        InstanceStatusDecider
            .isRemovable(resources, clusterName, instance3Name, Collections.singletonList(instance5Name))
            .isRemovable(),
        "Instance should be removable because after removing one instance, there are 2 active replicas in partition 0, it will not trigger re-balance.");
    Assert.assertFalse(
        InstanceStatusDecider
            .isRemovable(resources, clusterName, instance1Name, Collections.singletonList(instance5Name))
            .isRemovable(),
        "Instance should NOT be removable because after removing one instance, there are 1 active replicas in partition 1, it will trigger re-balance.");

    // Locked node check when locked instance shares replicas with the instance being removed
    NodeRemovableResult nodeRemovableWillTriggerRebalanceResult = InstanceStatusDecider
        .isRemovable(resources, clusterName, instance2Name, Collections.singletonList(instance3Name));
    Assert.assertFalse(
        nodeRemovableWillTriggerRebalanceResult.isRemovable(),
        instance2Name + " could NOT be removed because it will trigger rebalance after " + instance3Name
            + " is removed.");
    Assert.assertEquals(
        nodeRemovableWillTriggerRebalanceResult.getBlockingReason(),
        NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.toString());
    Assert.assertEquals(
        nodeRemovableWillTriggerRebalanceResult.getFormattedMessage(),
        "WILL_TRIGGER_LOAD_REBALANCE(TestInstanceStatusDecider_v1: Partition 0 will only have 1 active replicas which is smaller than required minimum active replicas: 2)");
  }

  private PartitionAssignment prepareAssignments(String resourceName) {
    return prepareMultiPartitionAssignments(resourceName, 1);
  }

  private PartitionAssignment prepareMultiPartitionAssignments(String resourceName, int partitionCount) {
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, partitionCount);
    resourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
    doReturn(resourceAssignment).when(routingDataRepository).getResourceAssignment();
    return partitionAssignment;
  }

  private void prepareStoreAndVersion(
      String storeName,
      int version,
      VersionStatus status,
      boolean isCurrentVersion,
      int replicationFactor) {
    Store store = TestUtils.createTestStore(storeName, "t", 0);
    Version version1 = new VersionImpl(storeName, version);
    version1.setReplicationFactor(replicationFactor);
    store.addVersion(version1);

    // we need to add the version to the store first before
    // updating its RF since #addVersion would auto-fill some
    // fields in the version (which would be an override in this
    // case)
    version1.setReplicationFactor(replicationFactor);
    if (isCurrentVersion) {
      store.setCurrentVersion(version);
    }

    doReturn(store).when(readWriteStoreRepository).getStore(storeName);

    // Create valid ideal state
    IdealState idealState = mock(IdealState.class);
    when(idealState.isEnabled()).thenReturn(true);
    when(idealState.isValid()).thenReturn(true);
    when(idealState.getMinActiveReplicas()).thenReturn(replicationFactor - 1);

    doAnswer(invocation -> {
      PropertyKey key = invocation.getArgument(0);
      if (key.getPath().contains("LIVEINSTANCES")) {
        return mock(LiveInstance.class);
      } else if (key.getPath().contains("IDEALSTATES")) {
        return idealState;
      }
      return null;
    }).when(accessor).getProperty(any(PropertyKey.class));

  }
}
