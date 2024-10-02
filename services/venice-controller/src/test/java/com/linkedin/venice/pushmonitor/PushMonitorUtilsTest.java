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
    map.put("a", 2);
    map.put("b", 10);
    map.put("c", 10);
    map.put("d", 10);
    map.put("e", 2);
    map.put("f", 2);
    map.put("g", 2);
    map.put("h", 2);

    // Test partition level key first
    doReturn(null).when(reader).getVersionStatus("store", 1);
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", 2, 0.25, ExecutionStatus.COMPLETED);

    // Test version level key
    doReturn(map).when(reader).getVersionStatus("store", 1);
    doReturn(Collections.emptyMap()).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    // 1 offline instances is below the fail fast threshold, the overall DaVinci status can be COMPLETED.
    validatePushStatus(reader, "store_v1", 2, 0.25, ExecutionStatus.COMPLETED);
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
    doReturn(null).when(reader).getVersionStatus("store", 1);
    doReturn(null).when(reader).getVersionStatus("store", 2);
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
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus) {
    ExecutionStatusWithDetails executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio,
        true);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
  }
}
