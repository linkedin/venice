package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushMonitorUtilsTest {
  @Test
  public void testCompleteStatusCannotBeReportedWithOfflineInstances() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    /**
     * Instance a is offline and its push status is not completed.
     * Instance b,c,d are online and their push status is completed.
     * In this case, the overall DaVinci push status shouldn't be reported as COMPLETED, because we want to
     * wait for all instances to be completed.
     */
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("a"));
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("b"));
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("c"));
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("d"));

    Map<CharSequence, Integer> map = new HashMap<>();
    map.put("a", 2);
    map.put("b", 10);
    map.put("c", 10);
    map.put("d", 10);

    // Test partition level key first
    doReturn(null).when(reader).getVersionStatus("store", 1);
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());

    // With any number of offline instances that haven't reported COMPLETED, the overall DaVinci status cannot be
    // COMPLETED.
    validateOfflineReplicaInPushStatus(reader, "store_v1", 2, 0.25, ExecutionStatus.STARTED, null);

    // Test version level key
    doReturn(map).when(reader).getVersionStatus("store", 1);
    doReturn(null).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    // With any number of offline instances that haven't reported COMPLETED, the overall DaVinci status cannot be
    // COMPLETED.
    validateOfflineReplicaInPushStatus(reader, "store_v1", 2, 0.25, ExecutionStatus.STARTED, null);
  }

  @Test
  public void testDaVinciPushStatusScan() {
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("a"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("b"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("c"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("d"));

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
    validateOfflineReplicaInPushStatus(reader, "store_v1", 2, 0.25, ExecutionStatus.STARTED, null);
    // Expected to fail.
    validateOfflineReplicaInPushStatus(
        reader,
        "store_v1",
        1,
        0.25,
        ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances);

    /**
     * Testing ratio-based threshold.
     */
    // Valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is max(1, 0.5*4) = 2.
    validateOfflineReplicaInPushStatus(reader, "store_v2", 1, 0.5, ExecutionStatus.STARTED, null);
    // Expected to fail.
    validateOfflineReplicaInPushStatus(
        reader,
        "store_v2",
        1,
        0.25,
        ExecutionStatus.ERROR,
        "Too many dead instances: 2, total instances: 4, example offline instances: " + offlineInstances);
  }

  private void validateOfflineReplicaInPushStatus(
      PushStatusStoreReader reader,
      String topicName,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      ExecutionStatus expectedStatus,
      String expectedErrorDetails) {
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
        maxOfflineInstanceRatio);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);
    // Sleep 1ms and try again.
    Utils.sleep(1);
    executionStatusWithDetails = PushMonitorUtils.getDaVinciPushStatusAndDetails(
        reader,
        topicName,
        1,
        Optional.empty(),
        maxOfflineInstanceCount,
        maxOfflineInstanceRatio);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), expectedStatus);
    if (expectedStatus.equals(ExecutionStatus.ERROR)) {
      Assert.assertEquals(executionStatusWithDetails.getDetails(), expectedErrorDetails);
    }
  }
}
