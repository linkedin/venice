package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushMonitorUtilsTest {
  @Test
  public void testCompleteStatusCanBeReportedWithOfflineInstancesBelowFailFastThreshold() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushMonitorUtils.setDVCDeadInstanceTime("store_v1", System.currentTimeMillis());
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    /**
      * Instance a is offline and its push status is not completed.
      * Instance b,c,d are online and their push status is completed.
      * In this case, the overall DaVinci push status can be COMPLETED as long as 1 is below the fail fast threshold.
      */
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("a"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("b"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("c"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("d"));
    // Bootstrapping nodes should be ignored
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("e"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("f"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("g"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader).getInstanceStatus(eq("store"), eq("h"));

    Map<CharSequence, Integer> map = new HashMap<>();
    /**
     * 2 is STARTED state, 10 is COMPLETED state.
     * Reference: {@link ExecutionStatus}
     */
    map.put("a", 2);
    map.put("b", 10);
    map.put("c", 10);
    map.put("d", 10);
    map.put("e", 2);
    map.put("f", 2);
    map.put("g", 2);
    map.put("h", 2);

    // Test partition level key first
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test version level key
    doReturn(map).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(Collections.emptyMap()).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test migration case: node "b" and "c" were reporting partition status at STARTED state, but later it completed
    // and reported
    // version level key status as COMPLETED.
    Map<CharSequence, Integer> retiredStatusMap = new HashMap<>();
    retiredStatusMap.put("b", 2);
    retiredStatusMap.put("c", 2);
    doReturn(retiredStatusMap).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    validatePushStatus(reader, "store_v1", Optional.empty(), 2, 0.25, ExecutionStatus.COMPLETED);

    // Test partition level key for incremental push
    String incrementalPushVersion = "incrementalPushVersion";
    Map<CharSequence, Integer> incrementalPushStatusMap = new HashMap<>();
    /**
     * 7 is START_OF_INCREMENTAL_PUSH_RECEIVED state, 8 is END_OF_INCREMENTAL_PUSH_RECEIVED state.
     * Reference: {@link ExecutionStatus}
     */
    incrementalPushStatusMap.put("a", 7);
    incrementalPushStatusMap.put("b", 8);
    incrementalPushStatusMap.put("c", 8);
    incrementalPushStatusMap.put("d", 8);
    incrementalPushStatusMap.put("e", 7);
    incrementalPushStatusMap.put("f", 7);
    incrementalPushStatusMap.put("g", 7);
    incrementalPushStatusMap.put("h", 7);

    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.of(incrementalPushVersion));
    doReturn(incrementalPushStatusMap).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be
    // END_OF_INCREMENTAL_PUSH_RECEIVED.
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    // Test version level key for incremental push
    doReturn(incrementalPushStatusMap).when(reader).getVersionStatus("store", 1, Optional.of(incrementalPushVersion));
    doReturn(Collections.emptyMap()).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be
    // END_OF_INCREMENTAL_PUSH_RECEIVED.
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    // Test migration case: node "b" and "c" were reporting partition status at START_OF_INCREMENTAL_PUSH_RECEIVED
    // state, but later it completed and reported version level key status as END_OF_INCREMENTAL_PUSH_RECEIVED.
    Map<CharSequence, Integer> retiredIncrementalPushStatusMap = new HashMap<>();
    retiredIncrementalPushStatusMap.put("b", 7);
    retiredIncrementalPushStatusMap.put("c", 8);
    doReturn(retiredIncrementalPushStatusMap).when(reader)
        .getPartitionStatus("store", 1, 0, Optional.of(incrementalPushVersion));
    validatePushStatus(
        reader,
        "store_v1",
        Optional.of(incrementalPushVersion),
        2,
        0.25,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDaVinciPushStatusScan(boolean useDaVinciSpecificExecutionStatusForError) {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("a"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("b"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("c"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("d"));

    Map<CharSequence, Integer> map = new HashMap<>();
    map.put("a", 3);
    map.put("b", 3);
    map.put("c", 3);
    map.put("d", 10);
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(null).when(reader).getVersionStatus("store", 2, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 2, 0, Optional.empty());

    Set<String> offlineInstances = new HashSet<>();
    offlineInstances.add("b");
    offlineInstances.add("c");
    /**
     * Testing count-based threshold.
     */
    // Valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is max(2, 0.25*4) = 2.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v1",
        2,
        0.25,
        ExecutionStatus.STARTED,
        null,
        useDaVinciSpecificExecutionStatusForError);

    // Expected to fail.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v1",
        1,
        0.25,
        useDaVinciSpecificExecutionStatusForError
            ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
            : ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances,
        useDaVinciSpecificExecutionStatusForError);

    /**
     * Testing ratio-based threshold.
     */
    // Valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is max(1, 0.5*4) = 2.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v2",
        1,
        0.5,
        ExecutionStatus.STARTED,
        null,
        useDaVinciSpecificExecutionStatusForError);
    // Expected to fail.
    validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
        reader,
        "store_v2",
        1,
        0.25,
        useDaVinciSpecificExecutionStatusForError
            ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
            : ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances,
        useDaVinciSpecificExecutionStatusForError);
  }

  private void validateOfflineReplicaInPushStatusWhenBreachingFailFastThreshold(
      PushStatusStoreReader reader,
      String topicName,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus,
      String expectedErrorDetails,
      boolean useDaVinciSpecificExecutionStatusForError) {
    /**
     * Even if offline instances number exceed the max offline threshold count it will remain STARTED for the first check,
     * as we need to wait until daVinciErrorInstanceWaitTime has passed since it first occurs.
     */
    ExecutionStatusWithDetails executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        useDaVinciSpecificExecutionStatusForError);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);
    // Sleep 1ms and try again.
    Utils.sleep(1);
    executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        useDaVinciSpecificExecutionStatusForError);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
    if (expectedStatus.isError()) {
      Assert.assertEquals(executionStatusWithDetails.getDetails(), expectedErrorDetails);
    }
  }

  private void validatePushStatus(
      PushStatusStoreReader reader,
      String topicName,
      Optional<String> incrementalPushVersion,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus) {
    ExecutionStatusWithDetails executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        incrementalPushVersion,
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        true);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
  }

  @Test
  public void testIncompletePartitionDetailsWithInstances() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance3"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance4"));
    doReturn(PushStatusStoreReader.InstanceStatus.DEAD).when(reader).getInstanceStatus(eq("store"), eq("deadInstance"));
    doReturn(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING).when(reader)
        .getInstanceStatus(eq("store"), eq("bootstrappingInstance"));

    // Test case 1: 3 incomplete partitions (≤ 10) - should show partition IDs with instances
    Map<CharSequence, Integer> partition0Status = new HashMap<>();
    partition0Status.put("instance1", 2); // STARTED
    partition0Status.put("instance2", 2); // STARTED
    partition0Status.put("deadInstance", 2); // Should be ignored
    partition0Status.put("bootstrappingInstance", 2); // Should be ignored

    Map<CharSequence, Integer> partition1Status = new HashMap<>();
    partition1Status.put("instance3", 2); // STARTED
    partition1Status.put("instance4", 10); // COMPLETED

    Map<CharSequence, Integer> partition2Status = new HashMap<>();
    partition2Status.put("instance1", 2); // STARTED
    partition2Status.put("instance2", 3); // END_OF_PUSH_RECEIVED

    // Mock partition status calls
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(partition0Status).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(partition1Status).when(reader).getPartitionStatus("store", 1, 1, Optional.empty());
    doReturn(partition2Status).when(reader).getPartitionStatus("store", 1, 2, Optional.empty());

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        "store_v1",
        3, // 3 partitions
        Optional.empty(),
        2,
        0.25,
        true);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    String details = result.getDetails();
    // Test partition 0 - instances can be in any order due to HashSet
    Assert.assertTrue(
        details.contains("Partition 0 (instances:") && details.contains("instance1") && details.contains("instance2"));
    // Test partition 1 - single instance
    Assert.assertTrue(details.contains("Partition 1 (instances: [instance3])"));
    // Test partition 2 - instances can be in any order due to HashSet
    Assert.assertTrue(
        details.contains("Partition 2 (instances:") && details.contains("instance1") && details.contains("instance2"));
    Assert.assertFalse(details.contains("deadInstance"));
    Assert.assertFalse(details.contains("bootstrappingInstance"));
  }

  @Test
  public void testIncompletePartitionDetailsWithMoreThan10Partitions() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));

    // Create status for 12 partitions (more than 10)
    Map<CharSequence, Integer> partitionStatus = new HashMap<>();
    partitionStatus.put("instance1", 2); // STARTED
    partitionStatus.put("instance2", 2); // STARTED

    // Mock partition status calls for 12 partitions
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    for (int i = 0; i < 12; i++) {
      doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, i, Optional.empty());
    }

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        "store_v1",
        12, // 12 partitions
        Optional.empty(),
        2,
        0.25,
        true);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    String details = result.getDetails();
    // Should show only partition IDs, not instance details
    Assert.assertTrue(details.contains("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"));
    Assert.assertFalse(details.contains("(instances:"));
  }

  @Test
  public void testIncompletePartitionDetailsWithExactly10Partitions() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));

    // Create status for 10 partitions (exactly 10)
    Map<CharSequence, Integer> partitionStatus = new HashMap<>();
    partitionStatus.put("instance1", 2); // STARTED
    partitionStatus.put("instance2", 2); // STARTED

    // Mock partition status calls for 10 partitions
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    for (int i = 0; i < 10; i++) {
      doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, i, Optional.empty());
    }

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        "store_v1",
        10, // 10 partitions
        Optional.empty(),
        2,
        0.25,
        true);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    String details = result.getDetails();
    // Should show partition IDs with instances since it's exactly 10
    Assert.assertTrue(
        details.contains("Partition 0 (instances:") && details.contains("instance1") && details.contains("instance2"));
    Assert.assertTrue(
        details.contains("Partition 9 (instances:") && details.contains("instance1") && details.contains("instance2"));
    Assert.assertFalse(details.contains("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"));
  }

  @Test
  public void testIncompletePartitionDetailsWithIgnoredInstances() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader)
        .getInstanceStatus(eq("store"), eq("ignoredInstance"));

    // Create status with instances to ignore
    Map<CharSequence, Integer> partitionStatus = new HashMap<>();
    partitionStatus.put("instance1", 2); // STARTED
    partitionStatus.put("instance2", 2); // STARTED
    partitionStatus.put("ignoredInstance", 2); // Should be ignored

    // Mock partition status calls
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    // Create set of instances to ignore
    Set<CharSequence> instancesToIgnore = new HashSet<>();
    instancesToIgnore.add("ignoredInstance");

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPartitionLevelPushStatusAndDetails(
        reader,
        "store_v1",
        1, // 1 partition
        Optional.empty(),
        2,
        0.25,
        true,
        instancesToIgnore);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    String details = result.getDetails();
    Assert.assertTrue(
        details.contains("Partition 0 (instances:") && details.contains("instance1") && details.contains("instance2"));
    Assert.assertFalse(details.contains("ignoredInstance"));
  }

  @Test
  public void testIncompletePartitionDetailsWithCompletedInstances() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader)
        .getInstanceStatus(eq("store"), eq("completedInstance"));

    // Create status with some completed instances
    Map<CharSequence, Integer> partitionStatus = new HashMap<>();
    partitionStatus.put("instance1", 2); // STARTED
    partitionStatus.put("instance2", 2); // STARTED
    partitionStatus.put("completedInstance", 10); // COMPLETED

    // Mock partition status calls
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.empty());
    doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        "store_v1",
        1, // 1 partition
        Optional.empty(),
        2,
        0.25,
        true);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    String details = result.getDetails();
    Assert.assertTrue(
        details.contains("Partition 0 (instances:") && details.contains("instance1") && details.contains("instance2"));
    Assert.assertFalse(details.contains("completedInstance"));
  }

  @Test
  public void testIncompletePartitionDetailsWithIncrementalPush() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);

    // Mock instance statuses
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance1"));
    doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(eq("store"), eq("instance2"));

    // Create status for incremental push
    Map<CharSequence, Integer> partitionStatus = new HashMap<>();
    partitionStatus.put("instance1", 7); // START_OF_INCREMENTAL_PUSH_RECEIVED
    partitionStatus.put("instance2", 7); // START_OF_INCREMENTAL_PUSH_RECEIVED

    // Mock partition status calls
    doReturn(null).when(reader).getVersionStatus("store", 1, Optional.of("incrementalPushVersion"));
    doReturn(partitionStatus).when(reader).getPartitionStatus("store", 1, 0, Optional.of("incrementalPushVersion"));

    ExecutionStatusWithDetails result = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        "store_v1",
        1, // 1 partition
        Optional.of("incrementalPushVersion"),
        2,
        0.25,
        true);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
    String details = result.getDetails();
    Assert.assertTrue(
        details.contains("Partition 0 (instances:") && details.contains("instance1") && details.contains("instance2"));
  }
}
