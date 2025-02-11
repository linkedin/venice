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
}
