package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushMonitorUtilsTest {
  @Test
  public void testDaVinciPushStatusScan() {
    String topicName = "store_v1";
    PushMonitorUtils.setDaVinciErrorInstanceWaitTime(0);
    PushStatusStoreReader reader = mock(PushStatusStoreReader.class);
    Map<CharSequence, Integer> map = new HashMap<>();
    map.put("a", 3);
    map.put("b", 3);
    map.put("c", 3);
    map.put("d", 10);
    doReturn(map).when(reader).getPartitionStatus("store", 1, 0, Optional.empty());
    doReturn(map).when(reader).getPartitionStatus("store", 2, 0, Optional.empty());
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("a"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("b"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("c"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("d"));

    // Testing count-based threshold.
    /**
     * It is still valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is 2.
     */
    ExecutionStatusWithDetails executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 2, 1.0);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);

    /**
     * The offline instances number exceed the max offline threshold count, but it will remain STARTED, as we need to wait
     * until daVinciErrorInstanceWaitTime has passed since it first occurs.
     */
    Utils.sleep(1);
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 1, 1.0);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);

    /**
     * This time it should fail, as we override the wait time to 0, and after 1ms, the 2nd query should meet the failure
     * condition check.
     */
    Utils.sleep(1);
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 1, 1.0);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(executionStatusWithDetails.getDetails(), " Too many dead instances: 2, total instances: 4");

    // Testing ratio-based threshold.
    topicName = "store_v2";
    /**
     * It is still valid, because we have 4 replicas, 1 completed, 2 offline, 1 online, threshold number is 4 * 0.5 = 2.
     */
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 100, 0.5);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);

    /**
     * The offline instances number exceed the max offline threshold count, but it will remain STARTED, as we need to wait
     * until daVinciErrorInstanceWaitTime has passed since it first occurs.
     */
    Utils.sleep(1);
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 100, 0.25);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);

    /**
     * This time it should fail, as we override the wait time to 0, and after 1ms, the 2nd query should meet the failure
     * condition check.
     */
    Utils.sleep(1);
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 100, 0.25);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(executionStatusWithDetails.getDetails(), " Too many dead instances: 2, total instances: 4");
  }
}
