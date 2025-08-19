package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.StuckConsumerRepairStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext.PubSubPropertiesSupplier;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AggKafkaConsumerServiceTest {
  private PubSubConsumerAdapterFactory consumerFactory;
  private PubSubPropertiesSupplier pubSubPropertiesSupplier;
  private VeniceServerConfig serverConfig;
  private IngestionThrottler ingestionThrottler;
  private KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private MetricsRepository metricsRepository;
  private StaleTopicChecker staleTopicChecker;
  private PubSubMessageDeserializer pubSubDeserializer;
  private Consumer<String> killIngestionTaskRunnable;
  private ReadOnlyStoreRepository metadataRepository;
  private PubSubTopicRepository topicRepository;
  private AggKafkaConsumerService aggKafkaConsumerService;
  private String PUBSUB_URL = "pubsub.venice.db";
  private String PUBSUB_URL_SEP = "pubsub.venice.db_sep";
  private PubSubTopic topic;
  private PubSubTopicPartition topicPartition;

  @BeforeMethod
  public void setUp() {
    topicRepository = new PubSubTopicRepository();
    topic = topicRepository.getTopic(Utils.getUniqueString("topic") + "_v1");
    topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    consumerFactory = mock(PubSubConsumerAdapterFactory.class);
    pubSubPropertiesSupplier = mock(PubSubPropertiesSupplier.class);
    ingestionThrottler = mock(IngestionThrottler.class);
    kafkaClusterBasedRecordThrottler = mock(KafkaClusterBasedRecordThrottler.class);
    metricsRepository = mock(MetricsRepository.class);
    staleTopicChecker = mock(StaleTopicChecker.class);
    pubSubDeserializer = mock(PubSubMessageDeserializer.class);
    killIngestionTaskRunnable = mock(Consumer.class);
    metadataRepository = mock(ReadOnlyStoreRepository.class);
    serverConfig = mock(VeniceServerConfig.class);
    Object2IntMap<String> tmpKafkaClusterUrlToIdMap = new Object2IntOpenHashMap<>();
    tmpKafkaClusterUrlToIdMap.put(PUBSUB_URL, 0);
    tmpKafkaClusterUrlToIdMap.put(PUBSUB_URL_SEP, 1);
    when(serverConfig.getKafkaClusterUrlToIdMap()).thenReturn(Object2IntMaps.unmodifiable(tmpKafkaClusterUrlToIdMap));
    // note that this isn't the exact same as the one in VeniceClusterConfig, but it should be sufficient for most cases
    when(serverConfig.getKafkaClusterUrlResolver()).thenReturn(Utils::resolveKafkaUrlForSepTopic);
    when(serverConfig.getConsumerPoolStrategyType()).thenReturn(DEFAULT);
    when(serverConfig.getConsumerPoolSizePerKafkaCluster()).thenReturn(5);
    when(serverConfig.isUnregisterMetricForDeletedStoreEnabled()).thenReturn(Boolean.FALSE);
    when(serverConfig.getSharedConsumerAssignmentStrategy())
        .thenReturn(PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY);
    when(serverConfig.getPubSubPositionTypeRegistry())
        .thenReturn(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);
    Sensor dummySensor = mock(Sensor.class);
    when(metricsRepository.sensor(anyString(), any())).thenReturn(dummySensor);
    PubSubConsumerAdapter adapter = mock(PubSubConsumerAdapter.class);
    when(consumerFactory.create(any(PubSubConsumerAdapterContext.class))).thenReturn(adapter);
    aggKafkaConsumerService = new AggKafkaConsumerService(
        consumerFactory,
        pubSubPropertiesSupplier,
        serverConfig,
        ingestionThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        staleTopicChecker,
        pubSubDeserializer,
        killIngestionTaskRunnable,
        t -> false,
        metadataRepository);
  }

  // test subscribeConsumerFor
  @Test
  public void testSubscribeConsumerFor() {
    doReturn(new VeniceProperties(new Properties())).when(pubSubPropertiesSupplier).get(PUBSUB_URL);

    AggKafkaConsumerService aggKafkaConsumerServiceSpy = spy(aggKafkaConsumerService);
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    TopicManager topicManager = mock(TopicManager.class);

    Properties props = new Properties();
    props.put(KAFKA_BOOTSTRAP_SERVERS, PUBSUB_URL);
    aggKafkaConsumerService.createKafkaConsumerService(props);
    when(storeIngestionTask.getVersionTopic()).thenReturn(topic);
    when(storeIngestionTask.getTopicManager(PUBSUB_URL)).thenReturn(topicManager);
    PartitionReplicaIngestionContext partitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
        topic,
        topicPartition,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    StorePartitionDataReceiver dataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerServiceSpy
        .subscribeConsumerFor(PUBSUB_URL, storeIngestionTask, partitionReplicaIngestionContext, -1);

    // regular pubsub url uses the default cluster id
    Assert.assertEquals(dataReceiver.getKafkaClusterId(), 0);
    verify(topicManager).prefetchAndCacheLatestOffset(topicPartition);

    dataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerServiceSpy
        .subscribeConsumerFor(PUBSUB_URL_SEP, storeIngestionTask, partitionReplicaIngestionContext, -1);
    // pubsub url for sep topic uses a different cluster id
    Assert.assertEquals(dataReceiver.getKafkaClusterId(), 1);

    // Sep topic should share the same kafka consumer service
    AbstractKafkaConsumerService kafkaConsumerService = aggKafkaConsumerService.getKafkaConsumerService(PUBSUB_URL);
    AbstractKafkaConsumerService kafkaConsumerServiceForSep =
        aggKafkaConsumerService.getKafkaConsumerService(PUBSUB_URL_SEP);
    Assert.assertSame(kafkaConsumerService, kafkaConsumerServiceForSep);
  }

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnable() {
    Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap = new HashMap<>();
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    // Everything is good
    KafkaConsumerService goodConsumerService = mock(KafkaConsumerService.class);
    when(goodConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool()).thenReturn(10l);
    kafkaServerToConsumerServiceMap.put("good", goodConsumerService);
    StoreIngestionTask goodTask = mock(StoreIngestionTask.class);
    when(goodTask.isProducingVersionTopicHealthy()).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("good_task", goodTask);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = getDetectAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
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
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = getDetectAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
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
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);

    Runnable repairRunnable = getDetectAndRepairRunnable(
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
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

  @Test
  public void testGetStuckConsumerDetectionAndRepairRunnableForStaleConsumerPoll() {
    Time time = mock(Time.class);
    Logger logger = mock(Logger.class);
    Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap = new HashMap<>();
    Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new HashMap<>();
    StuckConsumerRepairStats stuckConsumerRepairStats = mock(StuckConsumerRepairStats.class);

    // Everything is good from stuck consumer side of things
    KafkaConsumerService goodConsumerService = mock(KafkaConsumerService.class);
    when(goodConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool()).thenReturn(10l);
    kafkaServerToConsumerServiceMap.put("good", goodConsumerService);
    StoreIngestionTask goodTask = mock(StoreIngestionTask.class);
    when(goodTask.isProducingVersionTopicHealthy()).thenReturn(true);
    versionTopicStoreIngestionTaskMapping.put("good_task", goodTask);

    Map<PubSubTopicPartition, Long> staleTopicPartitions = new HashMap<>();
    PubSubTopicPartition mockStaleTopicPartition = mock(PubSubTopicPartition.class);
    String staleTopicName = "noPollTopicName";
    when(mockStaleTopicPartition.getTopicName()).thenReturn(staleTopicName);
    when(mockStaleTopicPartition.getPartitionNumber()).thenReturn(0);
    staleTopicPartitions.put(mockStaleTopicPartition, 0L);
    long expectedStaleTimeMs = 20000L;
    when(goodConsumerService.getStaleTopicPartitions(expectedStaleTimeMs - 10000L)).thenReturn(staleTopicPartitions);

    Consumer<String> killIngestionTaskRunnable = mock(Consumer.class);
    Runnable repairRunnable = getDetectAndRepairRunnable(
        logger,
        time,
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    when(time.getMilliseconds()).thenReturn(expectedStaleTimeMs);
    repairRunnable.run();
    verify(goodConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(stuckConsumerRepairStats, never()).recordStuckConsumerFound();
    verify(stuckConsumerRepairStats, never()).recordIngestionTaskRepair();
    verify(stuckConsumerRepairStats, never()).recordRepairFailure();
    verify(killIngestionTaskRunnable, never()).accept(any());
    ArgumentCaptor<String> logStringCaptor = ArgumentCaptor.forClass(String.class);
    verify(logger).warn(logStringCaptor.capture());
    String logMessage = logStringCaptor.getValue();
    Assert.assertTrue(logMessage.contains("Consumer poll tracker found stale topic partitions"));
    Assert.assertTrue(logMessage.contains(staleTopicName));
    Assert.assertTrue(logMessage.contains("stale for: " + expectedStaleTimeMs));
  }

  private Runnable getDetectAndRepairRunnable(
      Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap,
      Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping,
      StuckConsumerRepairStats stuckConsumerRepairStats,
      Consumer<String> killIngestionTaskRunnable) {
    return getDetectAndRepairRunnable(
        LogManager.getLogger(AggKafkaConsumerService.class),
        SystemTime.INSTANCE,
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
  }

  private Runnable getDetectAndRepairRunnable(
      Logger logger,
      Time time,
      Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap,
      Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping,
      StuckConsumerRepairStats stuckConsumerRepairStats,
      Consumer<String> killIngestionTaskRunnable) {
    long stuckConsumerRepairThresholdMs = 100;
    long nonExistingTopicIngestionTaskKillThresholdMs = 1000;
    long consumerPollTrackerStaleThresholdMs = 10000;

    Runnable repairRunnable = AggKafkaConsumerService.getStuckConsumerDetectionAndRepairRunnable(
        logger,
        time,
        kafkaServerToConsumerServiceMap,
        versionTopicStoreIngestionTaskMapping,
        stuckConsumerRepairThresholdMs,
        nonExistingTopicIngestionTaskKillThresholdMs,
        200,
        consumerPollTrackerStaleThresholdMs,
        stuckConsumerRepairStats,
        killIngestionTaskRunnable);
    return repairRunnable;
  }
}
