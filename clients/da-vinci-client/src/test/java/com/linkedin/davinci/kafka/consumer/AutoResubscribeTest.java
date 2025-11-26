package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class AutoResubscribeTest {
  @Test
  public void testHandleAutoResubscribe() throws InterruptedException {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doCallRealMethod().when(storeIngestionTask).maybeProcessResubscribeRequest();
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(serverConfig).when(storeIngestionTask).getServerConfig();
    PriorityBlockingQueue<Integer> resubscribeQueue = new PriorityBlockingQueue<>();
    doReturn(resubscribeQueue).when(storeIngestionTask).getResubscribeRequestQueue();
    Map<Integer, Long> resubscribeRequestTimestamp = new VeniceConcurrentHashMap<>();
    doReturn(resubscribeRequestTimestamp).when(storeIngestionTask).getPartitionToPreviousResubscribeTimeMap();
    Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();
    doReturn(partitionConsumptionStateMap).when(storeIngestionTask).getPartitionConsumptionStateMap();
    doReturn(300).when(serverConfig).getLagBasedReplicaAutoResubscribeIntervalInSeconds();

    doReturn(2).when(serverConfig).getLagBasedReplicaAutoResubscribeMaxReplicaCount();

    /**
     * The test setup goes as follows:
     * P0: Not stale
     * P1: Stale
     * P2: Does not have PCS
     * P3: Stale
     * P4: Stale but ERROR
     * P5: Stale
     *
     * It is expected to resubscribe P1/P3.
     */

    PartitionConsumptionState pcs1 = mock(PartitionConsumptionState.class);
    PartitionConsumptionState pcs3 = mock(PartitionConsumptionState.class);
    PartitionConsumptionState pcs4 = mock(PartitionConsumptionState.class);
    PartitionConsumptionState pcs5 = mock(PartitionConsumptionState.class);
    partitionConsumptionStateMap.put(1, pcs1);
    partitionConsumptionStateMap.put(3, pcs3);
    partitionConsumptionStateMap.put(4, pcs4);
    partitionConsumptionStateMap.put(5, pcs4);
    doReturn(true).when(pcs4).isErrorReported();
    resubscribeRequestTimestamp.put(0, System.currentTimeMillis());
    resubscribeRequestTimestamp.put(1, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(500));
    resubscribeRequestTimestamp.put(2, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(500));
    resubscribeRequestTimestamp.put(4, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(500));
    resubscribeRequestTimestamp.put(5, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(500));
    resubscribeQueue.put(0);
    resubscribeQueue.put(1);
    resubscribeQueue.put(2);
    resubscribeQueue.put(3);
    resubscribeQueue.put(4);
    resubscribeQueue.put(5);

    storeIngestionTask.maybeProcessResubscribeRequest();
    verify(storeIngestionTask, times(1)).resubscribe(eq(pcs1));
    verify(storeIngestionTask, times(1)).resubscribe(eq(pcs3));
    verify(storeIngestionTask, never()).resubscribe(eq(pcs4));
    verify(storeIngestionTask, never()).resubscribe(eq(pcs5));

  }
}
