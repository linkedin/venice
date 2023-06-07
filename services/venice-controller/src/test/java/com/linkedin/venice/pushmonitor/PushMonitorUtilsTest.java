package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
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
    doReturn(true).when(reader).isInstanceAlive(eq("store"), eq("a"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("b"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("c"));
    doReturn(false).when(reader).isInstanceAlive(eq("store"), eq("d"));

    ExecutionStatusWithDetails executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 2);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.STARTED);
    executionStatusWithDetails =
        PushMonitorUtils.getDaVinciPushStatusAndDetails(reader, topicName, 1, Optional.empty(), 2);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(executionStatusWithDetails.getDetails(), " Too many dead instances: 3, total instances: 4");
  }
}
