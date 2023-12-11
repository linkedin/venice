package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.StuckConsumerRepairStats;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.testng.annotations.Test;


public class AggKafkaConsumerServiceTest {
  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnable() {
    Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap = new HashMap<>();
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckConsumerRepairThresholdMs = 100;
    long nonExistingTopicIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    // Everything is good
    KafkaConsumerService goodConsumerService = mock(KafkaConsumerService.class);
    when(goodConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool()).thenReturn(10l);
    kafkaServerToConsumerServiceMap.put("good", goodConsumerService);
    StoreIngestionTask goodTask = mock(StoreIngestionTask.class);
    when(goodTask.isProducingVersionTopicHealthy()).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("good_task", goodTask);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairThresholdMs,
        nonExistingTopicIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    repairRunnable.run();
    verify(goodConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(stuckConsumerRepairStats, never()).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats, never()).recordIngestionTaskRepair();
    verify(stuckConsumerRepairStats, never()).recordRepairFailure();
    verify(killIngestionTaskRunnable, never()).accept(any());
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForTransientNonExistingTopic() {
    Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap = new HashMap<>();
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckConsumerRepairThresholdMs = 100;
    long nonExistingTopicIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairThresholdMs,
        nonExistingTopicIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    // One stuck consumer
    KafkaConsumerService badConsumerService = mock(KafkaConsumerService.class);
    when(badConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool()).thenReturn(1000l);
    kafkaServerToConsumerServiceMap.put("bad", badConsumerService);

    StoreIngestionTask transientBadTask = mock(StoreIngestionTask.class);
    when(transientBadTask.isProducingVersionTopicHealthy()).thenReturn(false).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("transient_bad_task", transientBadTask);
    repairRunnable.run();
    verify(badConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(stuckConsumerRepairStats).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats, never()).recordIngestionTaskRepair();
    verify(stuckConsumerRepairStats).recordRepairFailure();
    verify(killIngestionTaskRunnable, never()).accept(any());
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForNonExistingTopic() {
    Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap = new HashMap<>();
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckConsumerRepairThresholdMs = 100;
    long nonExistingTopicIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairThresholdMs,
        nonExistingTopicIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    // One stuck consumer
    KafkaConsumerService badConsumerService = mock(KafkaConsumerService.class);
    when(badConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool()).thenReturn(1000l);
    kafkaServerToConsumerServiceMap.put("bad", badConsumerService);
    StoreIngestionTask badTask = mock(StoreIngestionTask.class);
    when(badTask.isProducingVersionTopicHealthy()).thenReturn(false);
    versionTopicStoreIngestionTaskMapping.put("bad_task", badTask);
    repairRunnable.run();
    verify(badConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(badTask, times(6)).isProducingVersionTopicHealthy();
    verify(badTask).closeVeniceWriters(false);
    verify(killIngestionTaskRunnable).accept("bad_task");
    verify(stuckConsumerRepairStats).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats).recordIngestionTaskRepair();

    // One stuck consumer without any problematic ingestion task
    versionTopicStoreIngestionTaskMapping.remove("bad_task");
    repairRunnable.run();
    verify(stuckConsumerRepairStats).recordRepairFailure();
  }
}
