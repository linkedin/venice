package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.davinci.utils.IndexedMap;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.utils.RandomAccessDaemonThreadFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class KafkaConsumerServiceTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final PubSubMessageDeserializer pubSubDeserializer = PubSubMessageDeserializer.createOptimizedDeserializer();
  private VeniceServerConfig mockVeniceServerConfig;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY).when(mockVeniceServerConfig)
        .getPubSubPositionTypeRegistry();
    doReturn(20).when(mockVeniceServerConfig).getServerIngestionInfoLogLineLimit();
  }

  @Test
  public void testGetTopicPartitionIngestionInformation() throws Exception {
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(PubSubConsumerAdapterContext.class))).thenReturn(consumer1);

    Properties properties = new Properties();
    String testKafkaUrl = "test_kafka_url";
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");
    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    IngestionThrottler mockIngestionThrottler = mock(IngestionThrottler.class);
    KafkaConsumerService consumerService = getKafkaConsumerServiceWithSingleConsumer(
        factory,
        properties,
        mockMetricsRepository,
        ConsumerPoolType.AA_WC_LEADER_POOL,
        mockIngestionThrottler);
    String storeName3 = Utils.getUniqueString("test_consumer_service");
    PubSubTopic topicForStoreName3 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName3, 1));
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    when(task.getVersionTopic()).thenReturn(topicForStoreName3);
    when(task.isHybridMode()).thenReturn(true);
    PubSubTopic versionTopic = task.getVersionTopic();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(versionTopic, 0);
    PartitionReplicaIngestionContext partitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartition,
        VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);

    ConsumedDataReceiver consumedDataReceiver = mock(ConsumedDataReceiver.class);
    when(consumedDataReceiver.destinationIdentifier()).thenReturn(versionTopic);
    consumerService.startConsumptionIntoDataReceiver(
        partitionReplicaIngestionContext,
        ApacheKafkaOffsetPosition.of(0),
        consumedDataReceiver,
        false);

    SharedKafkaConsumer assignedConsumer = consumerService.assignConsumerFor(versionTopic, topicPartition);
    Set<PubSubTopicPartition> consumerAssignedPartitions = new HashSet<>();
    consumerAssignedPartitions.add(topicPartition);
    assignedConsumer.setCurrentAssignment(consumerAssignedPartitions);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumer2MessageMap = new HashMap<>();
    List<DefaultPubSubMessage> pubSubMessages = new ArrayList<>();
    DefaultPubSubMessage pubSubMessage = mock(DefaultPubSubMessage.class);
    when(pubSubMessage.getPayloadSize()).thenReturn(10);
    pubSubMessages.add(pubSubMessage);
    consumer2MessageMap.put(topicPartition, pubSubMessages);
    when(consumer1.poll(anyLong())).thenReturn(consumer2MessageMap);
    consumerService.start();

    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, true, true, () -> {
      verify(consumer1, atLeastOnce()).poll(anyLong());
      Map<PubSubTopicPartition, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap =
          consumerService.getIngestionInfoFor(versionTopic, topicPartition, false);
      Assert.assertEquals(topicPartitionIngestionInfoMap.size(), 1);
      Assert.assertTrue(topicPartitionIngestionInfoMap.containsKey(topicPartition));
      TopicPartitionIngestionInfo info = topicPartitionIngestionInfoMap.get(topicPartition);
      Assert.assertTrue(info.getConsumerIdStr().contains("0"));
      Assert.assertTrue(info.getConsumerIdStr().contains(testKafkaUrl));
      Assert.assertTrue(info.getMsgRate() > 0);
      Assert.assertTrue(info.getByteRate() > 0);
      Assert.assertEquals(info.getVersionTopicName(), topicForStoreName3.getName());
      Assert.assertTrue(
          info.getElapsedTimeSinceLastConsumerPollInMs() >= 0,
          "elapsedTimeSinceLastConsumerPollInMs should be non-negative");
      Assert.assertTrue(
          info.getElapsedTimeSinceLastRecordForPartitionInMs() >= 0,
          "elapsedTimeSinceLastRecordForPartitionInMs should be non-negative");
      String infoString = info.toString();
      Assert.assertTrue(
          infoString.contains("elapsedTimeSinceLastConsumerPollInMs:"),
          "toString should contain elapsedTimeSinceLastConsumerPollInMs field");
      Assert.assertTrue(
          infoString.contains("elapsedTimeSinceLastRecordForPartitionInMs:"),
          "toString should contain elapsedTimeSinceLastRecordForPartitionInMs field");
      verify(mockIngestionThrottler, atLeastOnce()).maybeThrottleBandwidth(anyInt());
      verify(mockIngestionThrottler, atLeastOnce())
          .maybeThrottleRecordRate(eq(ConsumerPoolType.AA_WC_LEADER_POOL), anyInt());
    });
    consumerService.stop();
  }

  private KafkaConsumerService getKafkaConsumerServiceWithSingleConsumer(
      PubSubConsumerAdapterFactory factory,
      Properties properties,
      MetricsRepository mockMetricsRepository,
      ConsumerPoolType poolType,
      IngestionThrottler mockIngestionThrottler) {
    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    doReturn(factory).when(mockPubSubClientsFactory).getConsumerAdapterFactory();

    PubSubContext mockPubSubContext = mock(PubSubContext.class);
    doReturn(pubSubDeserializer).when(mockPubSubContext).getPubSubMessageDeserializer();
    doReturn(mockPubSubClientsFactory).when(mockPubSubContext).getPubSubClientsFactory();

    KafkaConsumerService consumerService = new KafkaConsumerService(
        poolType,
        properties,
        1000L,
        1,
        mockIngestionThrottler,
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(StaleTopicChecker.class),
        false,
        SystemTime.INSTANCE,
        null,
        false,
        mock(ReadOnlyStoreRepository.class),
        false,
        mockVeniceServerConfig,
        mockPubSubContext,
        null) {
      @Override
      protected SharedKafkaConsumer pickConsumerForPartition(
          PubSubTopic versionTopic,
          PubSubTopicPartition topicPartition) {
        return consumerToConsumptionTask.getByIndex(0).getKey();
      }
    };
    return consumerService;
  }

  @Test
  public void testPartitionWiseGetConsumer() {
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    ApacheKafkaConsumerAdapter consumer2 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer2.hasAnySubscription()).thenReturn(true);

    String storeName1 = Utils.getUniqueString("test_consumer_service1");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreName1 = Version.composeKafkaTopic(storeName1, 1);
    PubSubTopic pubSubTopicForStoreName1 = pubSubTopicRepository.getTopic(topicForStoreName1);
    when(task1.getVersionTopic()).thenReturn(pubSubTopicForStoreName1);
    when(task1.isHybridMode()).thenReturn(true);

    String storeName2 = Utils.getUniqueString("test_consumer_service2");
    String topicForStoreName2 = Version.composeKafkaTopic(storeName2, 1);
    PubSubTopic pubSubTopicForStoreName2 = pubSubTopicRepository.getTopic(topicForStoreName2);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);

    when(task2.getVersionTopic()).thenReturn(pubSubTopicForStoreName2);
    when(task2.isHybridMode()).thenReturn(true);

    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(PubSubConsumerAdapterContext.class))).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");

    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    doReturn(factory).when(mockPubSubClientsFactory).getConsumerAdapterFactory();

    PubSubContext mockPubSubContext = mock(PubSubContext.class);
    doReturn(pubSubDeserializer).when(mockPubSubContext).getPubSubMessageDeserializer();
    doReturn(mockPubSubClientsFactory).when(mockPubSubContext).getPubSubClientsFactory();

    PartitionWiseKafkaConsumerService consumerService = new PartitionWiseKafkaConsumerService(
        ConsumerPoolType.REGULAR_POOL,
        properties,
        1000L,
        2,
        mock(IngestionThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(StaleTopicChecker.class),
        false,
        SystemTime.INSTANCE,
        null,
        false,
        mock(ReadOnlyStoreRepository.class),
        false,
        mockVeniceServerConfig,
        mockPubSubContext,
        null);
    consumerService.start();

    PubSubConsumerAdapter consumerForT1P0 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName1, new PubSubTopicPartitionImpl(pubSubTopicForStoreName1, 0));
    PubSubConsumerAdapter consumerForT1P1 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName1, new PubSubTopicPartitionImpl(pubSubTopicForStoreName1, 1));
    PubSubConsumerAdapter consumerForT1P2 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName1, new PubSubTopicPartitionImpl(pubSubTopicForStoreName1, 2));
    PubSubConsumerAdapter consumerForT1P3 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName1, new PubSubTopicPartitionImpl(pubSubTopicForStoreName1, 3));
    PubSubConsumerAdapter consumerForT2P0 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName2, new PubSubTopicPartitionImpl(pubSubTopicForStoreName2, 4));
    PubSubConsumerAdapter consumerForT2P1 = consumerService
        .assignConsumerFor(pubSubTopicForStoreName2, new PubSubTopicPartitionImpl(pubSubTopicForStoreName2, 5));
    Assert.assertNotEquals(consumerForT1P0, consumerForT1P1);
    Assert.assertNotEquals(consumerForT2P0, consumerForT2P1);
    Assert.assertEquals(consumerForT1P0, consumerForT1P2);
    Assert.assertEquals(consumerForT1P3, consumerForT2P1);
  }

  @Test
  public void testStoreAwarePartitionWiseGetConsumer() {
    String storeName1 = Utils.getUniqueString("test_consumer_service1");
    String topicForStoreName1 = Version.composeKafkaTopic(storeName1, 1);
    PubSubTopic pubSubTopicForStoreName1 = pubSubTopicRepository.getTopic(topicForStoreName1);

    String storeName2 = Utils.getUniqueString("test_consumer_service2");
    String topicForStoreName2 = Version.composeKafkaTopic(storeName2, 1);
    PubSubTopic pubSubTopicForStoreName2 = pubSubTopicRepository.getTopic(topicForStoreName2);

    String storeName3 = Utils.getUniqueString("test_consumer_service3");
    String topicForStoreName3 = Version.composeKafkaTopic(storeName3, 1);
    PubSubTopic pubSubTopicForStoreName3 = pubSubTopicRepository.getTopic(topicForStoreName3);

    String storeName4 = Utils.getUniqueString("test_consumer_service4");
    String topicForStoreName4 = Version.composeKafkaTopic(storeName4, 1);
    PubSubTopic pubSubTopicForStoreName4 = pubSubTopicRepository.getTopic(topicForStoreName4);

    SharedKafkaConsumer consumer1 = mock(SharedKafkaConsumer.class);
    SharedKafkaConsumer consumer2 = mock(SharedKafkaConsumer.class);
    ConsumptionTask consumptionTask = mock(ConsumptionTask.class);

    StoreAwarePartitionWiseKafkaConsumerService consumerService =
        mock(StoreAwarePartitionWiseKafkaConsumerService.class);

    // Prepare for the mock.
    IndexedMap<SharedKafkaConsumer, ConsumptionTask> consumptionTaskIndexedMap = new IndexedHashMap<>(2);
    consumptionTaskIndexedMap.put(consumer1, consumptionTask);
    consumptionTaskIndexedMap.put(consumer2, consumptionTask);
    when(consumerService.getConsumerToConsumptionTask()).thenReturn(consumptionTaskIndexedMap);

    Map<SharedKafkaConsumer, Integer> consumerToBasicLoadMap = new VeniceConcurrentHashMap<>();
    when(consumerService.getConsumerToBaseLoadCount()).thenReturn(consumerToBasicLoadMap);
    Map<SharedKafkaConsumer, Map<String, Integer>> consumerToStoreLoadMap = new VeniceConcurrentHashMap<>();
    when(consumerService.getConsumerToStoreLoadCount()).thenReturn(consumerToStoreLoadMap);

    Map<PubSubTopicPartition, Set<PubSubConsumerAdapter>> rtTopicPartitionToConsumerMap =
        new VeniceConcurrentHashMap<>();
    when(consumerService.getRtTopicPartitionToConsumerMap()).thenReturn(rtTopicPartitionToConsumerMap);
    when(consumerService.getLOGGER())
        .thenReturn(LogManager.getLogger(StoreAwarePartitionWiseKafkaConsumerService.class));
    doCallRealMethod().when(consumerService).pickConsumerForPartition(any(), any());
    doCallRealMethod().when(consumerService).getConsumerStoreLoad(any(), any());
    doCallRealMethod().when(consumerService).increaseConsumerStoreLoad(any(), any());
    doCallRealMethod().when(consumerService).decreaseConsumerStoreLoad(any(), any());

    consumerToBasicLoadMap.put(consumer1, 1);
    Map<String, Integer> innerMap1 = new VeniceConcurrentHashMap<>();
    innerMap1.put(storeName1, 1);
    consumerToStoreLoadMap.put(consumer1, innerMap1);
    consumerToBasicLoadMap.put(consumer2, 2);
    Map<String, Integer> innerMap2 = new VeniceConcurrentHashMap<>();
    innerMap2.put(storeName2, 2);
    consumerToStoreLoadMap.put(consumer2, innerMap2);

    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName1), 10001);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName2), 1);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName3), 1);

    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer2, storeName2), 20002);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer2, storeName1), 2);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer2, storeName3), 2);

    Assert.assertEquals(
        consumerService.pickConsumerForPartition(
            pubSubTopicForStoreName1,
            new PubSubTopicPartitionImpl(pubSubTopicForStoreName1, 0)),
        consumer2);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer2).intValue(), 3);
    Assert.assertEquals(consumerToStoreLoadMap.get(consumer2).get(storeName1).intValue(), 1);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer2, storeName1), 10003);
    Assert.assertEquals(
        consumerService.pickConsumerForPartition(
            pubSubTopicForStoreName2,
            new PubSubTopicPartitionImpl(pubSubTopicForStoreName2, 0)),
        consumer1);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer1).intValue(), 2);
    Assert.assertEquals(consumerToStoreLoadMap.get(consumer1).get(storeName2).intValue(), 1);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName2), 10002);
    Assert.assertEquals(
        consumerService.pickConsumerForPartition(
            pubSubTopicForStoreName3,
            new PubSubTopicPartitionImpl(pubSubTopicForStoreName3, 0)),
        consumer1);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer1).intValue(), 3);
    Assert.assertEquals(consumerToStoreLoadMap.get(consumer1).get(storeName3).intValue(), 1);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName3), 10003);

    // Validate decrease consumer entry
    Assert.assertThrows(() -> consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName4));

    consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName1);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer1).intValue(), 2);
    Assert.assertNull(consumerToStoreLoadMap.get(consumer1).get(storeName1));
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName1), 2);
    Assert.assertThrows(() -> consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName1));

    consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName2);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer1).intValue(), 1);
    Assert.assertNull(consumerToStoreLoadMap.get(consumer1).get(storeName2));
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName2), 1);
    Assert.assertThrows(() -> consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName2));

    consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName3);
    Assert.assertNull(consumerToBasicLoadMap.get(consumer1));
    Assert.assertNull(consumerToStoreLoadMap.get(consumer1));
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName3), 0);
    Assert.assertThrows(() -> consumerService.decreaseConsumerStoreLoad(consumer1, pubSubTopicForStoreName3));

    // Make sure invalid versionTopic won't throw NPE.
    consumerService.decreaseConsumerStoreLoad(consumer1, null);

    // Validate increase consumer entry
    consumerService.increaseConsumerStoreLoad(consumer1, storeName1);
    Assert.assertEquals(consumerToBasicLoadMap.get(consumer1).intValue(), 1);
    Assert.assertEquals(consumerToStoreLoadMap.get(consumer1).get(storeName1).intValue(), 1);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName1), 10001);
    Assert.assertEquals(consumerService.getConsumerStoreLoad(consumer1, storeName2), 1);
  }

  @Test
  public void testGetMaxElapsedTimeMSSinceLastPollInConsumerPool() {
    // Mock the necessary components
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    ApacheKafkaConsumerAdapter consumer2 = mock(ApacheKafkaConsumerAdapter.class);
    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(PubSubConsumerAdapterContext.class))).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");
    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    doReturn(factory).when(mockPubSubClientsFactory).getConsumerAdapterFactory();

    PubSubContext mockPubSubContext = mock(PubSubContext.class);
    doReturn(pubSubDeserializer).when(mockPubSubContext).getPubSubMessageDeserializer();
    doReturn(mockPubSubClientsFactory).when(mockPubSubContext).getPubSubClientsFactory();

    KafkaConsumerService consumerService = new KafkaConsumerService(
        ConsumerPoolType.REGULAR_POOL,
        properties,
        1000L,
        2,
        mock(IngestionThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(StaleTopicChecker.class),
        false,
        SystemTime.INSTANCE,
        null,
        false,
        mock(ReadOnlyStoreRepository.class),
        false,
        mockVeniceServerConfig,
        mockPubSubContext,
        null) {
      @Override
      protected SharedKafkaConsumer pickConsumerForPartition(
          PubSubTopic versionTopic,
          PubSubTopicPartition topicPartition) {
        return null;
      }
    };

    // Create mock ConsumptionTasks
    ConsumptionTask task1 = mock(ConsumptionTask.class);
    ConsumptionTask task2 = mock(ConsumptionTask.class);
    when(task1.getLastSuccessfulPollTimestamp()).thenReturn(System.currentTimeMillis() - 40000); // 40 seconds ago
    when(task2.getLastSuccessfulPollTimestamp()).thenReturn(System.currentTimeMillis() - 60000); // 60 seconds ago
    when(task1.getTaskId()).thenReturn(0); // task id = 0
    when(task2.getTaskId()).thenReturn(1); // task id = 1

    Thread t = mock(Thread.class);
    when(t.getStackTrace()).thenReturn(new StackTraceElement[0]);

    RandomAccessDaemonThreadFactory consumerThreadFactory = mock(RandomAccessDaemonThreadFactory.class);
    when(consumerThreadFactory.getThread(0)).thenReturn(mock(Thread.class));
    when(consumerThreadFactory.getThread(1)).thenReturn(t); // thread id = 1 has longer elapsed time.

    // Set the thread factory
    consumerService.setThreadFactory(consumerThreadFactory);

    // Add tasks to the consumerToConsumptionTask map
    consumerService.consumerToConsumptionTask.put(mock(SharedKafkaConsumer.class), task1);
    consumerService.consumerToConsumptionTask.put(mock(SharedKafkaConsumer.class), task2);

    // Call the method and assert the result
    long maxElapsedTime = consumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool();

    // Verify that the maxElapsedTime is >= 60 seconds.
    Assert.assertTrue(maxElapsedTime >= 60000, "The max elapsed time should be greater than 60000 ms");
    // Verify that the getStackTrace method was called once for t.
    verify(t, times(1)).getStackTrace();
  }

  @Test
  public void testInactiveTopicPartitionChecker() {
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    doCallRealMethod().when(consumerService).shouldEnableInactiveTopicPartitionChecker(any(), any());
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    for (int i = 0; i < 2; i++) {
      boolean configEnabled = (i == 0);
      doReturn(configEnabled).when(serverConfig).isInactiveTopicPartitionCheckerEnabled();
      for (ConsumerPoolType poolType: ConsumerPoolType.values()) {
        boolean expected = configEnabled && (poolType.equals(ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL)
            || poolType.equals(ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL));
        Assert
            .assertEquals(expected, consumerService.shouldEnableInactiveTopicPartitionChecker(serverConfig, poolType));
      }
    }
  }
}
