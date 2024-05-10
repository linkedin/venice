package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.StuckConsumerRepairStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext.PubSubPropertiesSupplier;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AggKafkaConsumerServiceTest {
  private PubSubConsumerAdapterFactory consumerFactory;
  private PubSubPropertiesSupplier pubSubPropertiesSupplier;
  private VeniceServerConfig serverConfig;
  private EventThrottler bandwidthThrottler;
  private EventThrottler recordsThrottler;
  private KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private MetricsRepository metricsRepository;
  private TopicExistenceChecker topicExistenceChecker;
  private PubSubMessageDeserializer pubSubDeserializer;
  private Consumer<String> killIngestionTaskRunnable;
  private ReadOnlyStoreRepository metadataRepository;
  private PubSubTopicRepository topicRepository;
  private AggKafkaConsumerService aggKafkaConsumerService;
  private String pubSubUrl = "pubsub.venice.db";
  private PubSubTopic topic;
  private PubSubTopicPartition topicPartition;

  @BeforeMethod
  public void setUp() {
    topicRepository = new PubSubTopicRepository();
    topic = topicRepository.getTopic(Utils.getUniqueString("topic") + "_v1");
    topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    consumerFactory = mock(PubSubConsumerAdapterFactory.class);
    pubSubPropertiesSupplier = mock(PubSubPropertiesSupplier.class);
    bandwidthThrottler = mock(EventThrottler.class);
    recordsThrottler = mock(EventThrottler.class);
    kafkaClusterBasedRecordThrottler = mock(KafkaClusterBasedRecordThrottler.class);
    metricsRepository = mock(MetricsRepository.class);
    topicExistenceChecker = mock(TopicExistenceChecker.class);
    pubSubDeserializer = mock(PubSubMessageDeserializer.class);
    killIngestionTaskRunnable = mock(Consumer.class);
    metadataRepository = mock(ReadOnlyStoreRepository.class);
    serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getKafkaClusterUrlToIdMap()).thenReturn(Object2IntMaps.EMPTY_MAP);

    aggKafkaConsumerService = new AggKafkaConsumerService(
        consumerFactory,
        pubSubPropertiesSupplier,
        serverConfig,
        bandwidthThrottler,
        recordsThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        topicExistenceChecker,
        pubSubDeserializer,
        killIngestionTaskRunnable,
        t -> false,
        metadataRepository);
  }

  // test subscribeConsumerFor
  @Test
  public void testSubscribeConsumerFor() {
    AggKafkaConsumerService aggKafkaConsumerServiceSpy = spy(aggKafkaConsumerService);
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    TopicManager topicManager = mock(TopicManager.class);

    doReturn(mock(AbstractKafkaConsumerService.class)).when(aggKafkaConsumerServiceSpy).getKafkaConsumerService(any());
    when(storeIngestionTask.getVersionTopic()).thenReturn(topic);
    when(storeIngestionTask.getTopicManager(pubSubUrl)).thenReturn(topicManager);

    aggKafkaConsumerServiceSpy.subscribeConsumerFor(pubSubUrl, storeIngestionTask, topicPartition, -1);

    verify(topicManager).prefetchAndCacheLatestOffset(topicPartition);
  }

  @Test
  public void testGetOffsetLagBasedOnMetrics() {
    AggKafkaConsumerService aggKafkaConsumerServiceSpy = spy(aggKafkaConsumerService);

    doReturn(null).when(aggKafkaConsumerServiceSpy).getKafkaConsumerService(pubSubUrl);
    assertEquals(aggKafkaConsumerServiceSpy.getOffsetLagBasedOnMetrics(pubSubUrl, topic, topicPartition), -1);

    AbstractKafkaConsumerService consumerService = mock(AbstractKafkaConsumerService.class);
    when(consumerService.getOffsetLagBasedOnMetrics(topic, topicPartition)).thenReturn(123L);
    doReturn(consumerService).when(aggKafkaConsumerServiceSpy).getKafkaConsumerService(any());
    assertEquals(aggKafkaConsumerServiceSpy.getOffsetLagBasedOnMetrics(pubSubUrl, topic, topicPartition), 123L);
  }

  @Test
  public void testGetLatestOffsetBasedOnMetrics() {
    AggKafkaConsumerService aggKafkaConsumerServiceSpy = spy(aggKafkaConsumerService);
    doReturn(null).when(aggKafkaConsumerServiceSpy).getKafkaConsumerService(pubSubUrl);
    assertEquals(aggKafkaConsumerServiceSpy.getLatestOffsetBasedOnMetrics(pubSubUrl, topic, topicPartition), -1);

    AbstractKafkaConsumerService consumerService = mock(AbstractKafkaConsumerService.class);
    when(consumerService.getLatestOffsetBasedOnMetrics(topic, topicPartition)).thenReturn(1234L);
    doReturn(consumerService).when(aggKafkaConsumerServiceSpy).getKafkaConsumerService(any());
    assertEquals(aggKafkaConsumerServiceSpy.getLatestOffsetBasedOnMetrics(pubSubUrl, topic, topicPartition), 1234L);
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnable() {
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    // Everything is good
    StoreIngestionTask goodTask = mock(StoreIngestionTask.class);
    when(goodTask.isProducingVersionTopicHealthy()).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("good_task", goodTask);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        versionTopicStoreIngestionTaskMapping,
        stuckIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    repairRunnable.run();
    verify(stuckConsumerRepairStats, never()).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats, never()).recordIngestionTaskRepair();
    verify(killIngestionTaskRunnable, never()).accept(any());
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForTransientNonExistingTopic() {
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        versionTopicStoreIngestionTaskMapping,
        stuckIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    // One stuck consumer
    StoreIngestionTask transientBadTask = mock(StoreIngestionTask.class);
    when(transientBadTask.isProducingVersionTopicHealthy()).thenReturn(false).thenReturn(true);
    when(transientBadTask.isIngestionStuckPostOnline()).thenReturn(false);
    versionTopicStoreIngestionTaskMapping.put("transient_bad_task", transientBadTask);
    repairRunnable.run();
    verify(stuckConsumerRepairStats).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats, never()).recordIngestionTaskRepair();
    verify(killIngestionTaskRunnable, never()).accept(any());
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForNonExistingTopic() {
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    long stuckIngestionTaskKillThresholdMs = 1000;
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        versionTopicStoreIngestionTaskMapping,
        stuckIngestionTaskKillThresholdMs,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    // One stuck consumer
    StoreIngestionTask badTask = mock(StoreIngestionTask.class);
    when(badTask.isProducingVersionTopicHealthy()).thenReturn(false);
    when(badTask.isIngestionStuckPostOnline()).thenReturn(false);
    versionTopicStoreIngestionTaskMapping.put("bad_task", badTask);
    repairRunnable.run();
    verify(badTask, times(6)).isProducingVersionTopicHealthy();
    verify(badTask).closeVeniceWriters(false);
    verify(killIngestionTaskRunnable).accept("bad_task");
    verify(stuckConsumerRepairStats).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats).recordIngestionTaskRepair();
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForReplicaHavingIssuesPostOnline() {
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        versionTopicStoreIngestionTaskMapping,
        1000,
        200,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    // One stuck consumer
    StoreIngestionTask badTask = mock(StoreIngestionTask.class);
    when(badTask.isProducingVersionTopicHealthy()).thenReturn(true);
    when(badTask.isIngestionStuckPostOnline()).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("bad_task", badTask);
    repairRunnable.run();
    verify(badTask, times(1)).isProducingVersionTopicHealthy();
    verify(badTask, times(6)).isIngestionStuckPostOnline();
    verify(badTask).closeVeniceWriters(false);
    verify(killIngestionTaskRunnable).accept("bad_task");
    verify(stuckConsumerRepairStats).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats).recordIngestionTaskRepair();
  }
}
