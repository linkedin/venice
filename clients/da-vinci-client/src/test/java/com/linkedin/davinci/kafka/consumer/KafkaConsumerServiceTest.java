package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaConsumerServiceTest {
  private final KafkaPubSubMessageDeserializer pubSubDeserializer = new KafkaPubSubMessageDeserializer(
      new OptimizedKafkaValueSerializer(),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new));

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testTopicWiseGetConsumer() throws Exception {
    SharedKafkaConsumer consumer1 = mock(SharedKafkaConsumer.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    SharedKafkaConsumer consumer2 = mock(SharedKafkaConsumer.class);
    when(consumer2.hasAnySubscription()).thenReturn(true);

    String storeName1 = Utils.getUniqueString("test_consumer_service1");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreName1 = Version.composeKafkaTopic(storeName1, 1);
    when(task1.getVersionTopic()).thenReturn(topicForStoreName1);
    when(task1.isHybridMode()).thenReturn(true);

    String storeName2 = Utils.getUniqueString("test_consumer_service2");
    String topicForStoreName2 = Version.composeKafkaTopic(storeName2, 1);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreName2);
    when(task2.isHybridMode()).thenReturn(true);

    KafkaClientFactory factory = mock(KafkaClientFactory.class);
    when(factory.getConsumer(any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_url");

    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    KafkaConsumerService consumerService = new TopicWiseKafkaConsumerService(
        factory,
        properties,
        1000l,
        2,
        mock(EventThrottler.class),
        mock(EventThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(TopicExistenceChecker.class),
        false,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        null);
    consumerService.start();

    PubSubConsumer assignedConsumerForTask1 = consumerService.assignConsumerFor(
        topicForStoreName1,
        pubSubTopicRepository.getPubSubTopicPartition(task1.getVersionTopic(), 0));
    PubSubConsumer assignedConsumerForTask2 = consumerService.assignConsumerFor(
        topicForStoreName2,
        pubSubTopicRepository.getPubSubTopicPartition(task2.getVersionTopic(), 0));
    Assert.assertNotEquals(
        assignedConsumerForTask1,
        assignedConsumerForTask2,
        "We should avoid to share consumer when there is consumer not assigned topic.");

    // Get partitions assigned to those two consumers
    Set<TopicPartition> consumer1AssignedPartitions = getTopicPartitionsSet(topicForStoreName1, 5);
    when(consumer1.getAssignment()).thenReturn(consumer1AssignedPartitions);

    Set<TopicPartition> consumer2AssignedPartitions = getTopicPartitionsSet(topicForStoreName2, 3);

    when(consumer2.getAssignment()).thenReturn(consumer2AssignedPartitions);
    SharedPubSubConsumerImpl sharedAssignedConsumerForTask1 = (SharedPubSubConsumerImpl) assignedConsumerForTask1;
    sharedAssignedConsumerForTask1.setCurrentAssignment(convertToPubSubTopicPartitions(consumer1AssignedPartitions));
    SharedPubSubConsumerImpl sharedAssignedConsumerForTask2 = (SharedPubSubConsumerImpl) assignedConsumerForTask2;
    sharedAssignedConsumerForTask2.setCurrentAssignment(convertToPubSubTopicPartitions(consumer2AssignedPartitions));

    String storeName3 = Utils.getUniqueString("test_consumer_service3");
    String topicForStoreName3 = Version.composeKafkaTopic(storeName3, 1);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreName3);
    when(task3.isHybridMode()).thenReturn(true);
    PubSubConsumer assignedConsumerForTask3 = consumerService.assignConsumerFor(
        topicForStoreName3,
        pubSubTopicRepository.getPubSubTopicPartition(task3.getVersionTopic(), 0));
    Assert.assertEquals(
        assignedConsumerForTask3,
        assignedConsumerForTask2,
        "The assigned consumer should come with least partitions, when no zero loaded consumer available.");
    consumerService.stop();
  }

  private Set<TopicPartition> getTopicPartitionsSet(String topic, int partitionNum) {
    Set<TopicPartition> topicPartitionsSet = new HashSet<>();
    for (int i = 0; i < partitionNum; i++) {
      topicPartitionsSet.add(new TopicPartition(topic, i));
    }
    return topicPartitionsSet;
  }

  private Set<PubSubTopicPartition> convertToPubSubTopicPartitions(Set<TopicPartition> topicPartitionsSet) {
    Set<PubSubTopicPartition> pubSubTopicPartitionsSet = new HashSet<>();
    for (TopicPartition topicPartition: topicPartitionsSet) {
      pubSubTopicPartitionsSet
          .add(pubSubTopicRepository.getPubSubTopicPartition(topicPartition.topic(), topicPartition.partition()));
    }
    return pubSubTopicPartitionsSet;
  }

  @Test
  public void testTopicWiseGetConsumerForHybridMode() throws Exception {
    KafkaConsumerWrapper consumer1 = mock(KafkaConsumerWrapper.class);
    when(consumer1.hasAnySubscription()).thenReturn(false);

    KafkaConsumerWrapper consumer2 = mock(KafkaConsumerWrapper.class);
    when(consumer1.hasAnySubscription()).thenReturn(false);

    KafkaClientFactory factory = mock(KafkaClientFactory.class);
    when(factory.getConsumer(any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_url");

    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    KafkaConsumerService consumerService = new TopicWiseKafkaConsumerService(
        factory,
        properties,
        1000l,
        2,
        mock(EventThrottler.class),
        mock(EventThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(TopicExistenceChecker.class),
        false,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        null);
    consumerService.start();

    String storeName = Utils.getUniqueString("test_consumer_service");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    when(task1.getVersionTopic()).thenReturn(topicForStoreVersion1);
    when(task1.isHybridMode()).thenReturn(true);

    SharedPubSubConsumerImpl assignedConsumerForV1 = consumerService.assignConsumerFor(
        topicForStoreVersion1,
        pubSubTopicRepository.getPubSubTopicPartition(task1.getVersionTopic(), 0));
    Assert.assertEquals(
        consumerService.assignConsumerFor(
            topicForStoreVersion1,
            pubSubTopicRepository.getPubSubTopicPartition(task1.getVersionTopic(), 0)),
        assignedConsumerForV1,
        "The 'getConsumer' function should be idempotent");

    String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreVersion2);
    when(task2.isHybridMode()).thenReturn(true);

    SharedPubSubConsumerImpl assignedConsumerForV2 = consumerService.assignConsumerFor(
        topicForStoreVersion2,
        pubSubTopicRepository.getPubSubTopicPartition(task2.getVersionTopic(), 0));
    Assert.assertNotEquals(
        assignedConsumerForV2,
        assignedConsumerForV1,
        "The 'getConsumer' function should return a different consumer from v1");

    String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreVersion3);
    when(task3.isHybridMode()).thenReturn(true);

    try {
      consumerService.assignConsumerFor(
          topicForStoreVersion3,
          pubSubTopicRepository.getPubSubTopicPartition(task3.getVersionTopic(), 0));
      Assert.fail("An exception should be thrown since all 2 consumers should be occupied by other versions");
    } catch (VeniceException e) {
      // expected
    }

    consumerService.stop();
  }

  @Test
  public void testPartitionWiseGetConsumer() throws Exception {
    SharedKafkaConsumer consumer1 = mock(SharedKafkaConsumer.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    SharedKafkaConsumer consumer2 = mock(SharedKafkaConsumer.class);
    when(consumer2.hasAnySubscription()).thenReturn(true);

    String storeName1 = Utils.getUniqueString("test_consumer_service1");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreName1 = Version.composeKafkaTopic(storeName1, 1);
    when(task1.getVersionTopic()).thenReturn(topicForStoreName1);
    when(task1.isHybridMode()).thenReturn(true);

    String storeName2 = Utils.getUniqueString("test_consumer_service2");
    String topicForStoreName2 = Version.composeKafkaTopic(storeName2, 1);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreName2);
    when(task2.isHybridMode()).thenReturn(true);

    KafkaClientFactory factory = mock(KafkaClientFactory.class);
    when(factory.getConsumer(any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_url");

    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    PartitionWiseKafkaConsumerService consumerService = new PartitionWiseKafkaConsumerService(
        factory,
        properties,
        1000l,
        2,
        mock(EventThrottler.class),
        mock(EventThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(TopicExistenceChecker.class),
        false,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        null);
    consumerService.start();

    PubSubConsumer consumerForT1P0 = consumerService
        .assignConsumerFor(topicForStoreName1, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName1, 0));
    PubSubConsumer consumerForT1P1 = consumerService
        .assignConsumerFor(topicForStoreName1, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName1, 1));
    PubSubConsumer consumerForT1P2 = consumerService
        .assignConsumerFor(topicForStoreName1, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName1, 2));
    PubSubConsumer consumerForT1P3 = consumerService
        .assignConsumerFor(topicForStoreName1, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName1, 3));
    PubSubConsumer consumerForT2P0 = consumerService
        .assignConsumerFor(topicForStoreName2, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName2, 0));
    PubSubConsumer consumerForT2P1 = consumerService
        .assignConsumerFor(topicForStoreName2, pubSubTopicRepository.getPubSubTopicPartition(topicForStoreName2, 1));
    Assert.assertNotEquals(consumerForT1P0, consumerForT1P1);
    Assert.assertNotEquals(consumerForT2P0, consumerForT2P1);
    Assert.assertEquals(consumerForT1P0, consumerForT1P2);
    Assert.assertEquals(consumerForT1P3, consumerForT2P1);
  }
}
