package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
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
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaConsumerServiceTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final PubSubMessageDeserializer pubSubDeserializer = new PubSubMessageDeserializer(
      new OptimizedKafkaValueSerializer(),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new));

  @Test
  public void testTopicWiseGetConsumer() throws Exception {
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    ApacheKafkaConsumerAdapter consumer2 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer2.hasAnySubscription()).thenReturn(true);

    String storeName1 = Utils.getUniqueString("test_consumer_service1");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    PubSubTopic topicForStoreName1 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName1, 1));
    when(task1.getVersionTopic()).thenReturn(topicForStoreName1);
    when(task1.isHybridMode()).thenReturn(true);

    String storeName2 = Utils.getUniqueString("test_consumer_service2");
    PubSubTopic topicForStoreName2 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName2, 1));
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreName2);
    when(task2.isHybridMode()).thenReturn(true);

    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(), anyBoolean(), any(), any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");

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
        null,
        false);
    consumerService.start();

    PubSubTopic versionTopicForTask1 = task1.getVersionTopic();
    PubSubConsumerAdapter assignedConsumerForTask1 =
        consumerService.assignConsumerFor(versionTopicForTask1, new PubSubTopicPartitionImpl(versionTopicForTask1, 0));
    PubSubTopic versionTopicForTask2 = task2.getVersionTopic();
    PubSubConsumerAdapter assignedConsumerForTask2 =
        consumerService.assignConsumerFor(versionTopicForTask2, new PubSubTopicPartitionImpl(versionTopicForTask2, 0));
    Assert.assertNotEquals(
        assignedConsumerForTask1,
        assignedConsumerForTask2,
        "We should avoid to share consumer when there is consumer not assigned topic.");

    // Get partitions assigned to those two consumers
    Set<PubSubTopicPartition> consumer1AssignedPartitions = getPubSubTopicPartitionsSet(topicForStoreName1, 5);
    when(consumer1.getAssignment()).thenReturn(consumer1AssignedPartitions);

    Set<PubSubTopicPartition> consumer2AssignedPartitions = getPubSubTopicPartitionsSet(topicForStoreName2, 3);

    when(consumer2.getAssignment()).thenReturn(consumer2AssignedPartitions);
    SharedKafkaConsumer sharedAssignedConsumerForTask1 = (SharedKafkaConsumer) assignedConsumerForTask1;
    sharedAssignedConsumerForTask1.setCurrentAssignment(consumer1AssignedPartitions);
    SharedKafkaConsumer sharedAssignedConsumerForTask2 = (SharedKafkaConsumer) assignedConsumerForTask2;
    sharedAssignedConsumerForTask2.setCurrentAssignment(consumer2AssignedPartitions);

    String storeName3 = Utils.getUniqueString("test_consumer_service3");
    PubSubTopic topicForStoreName3 = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName3, 1));
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreName3);
    when(task3.isHybridMode()).thenReturn(true);
    PubSubTopic versionTopicForTask3 = task3.getVersionTopic();
    PubSubConsumerAdapter assignedConsumerForTask3 =
        consumerService.assignConsumerFor(versionTopicForTask3, new PubSubTopicPartitionImpl(versionTopicForTask3, 0));
    Assert.assertEquals(
        assignedConsumerForTask3,
        assignedConsumerForTask2,
        "The assigned consumer should come with least partitions, when no zero loaded consumer available.");
    consumerService.stop();
  }

  private Set<PubSubTopicPartition> getPubSubTopicPartitionsSet(PubSubTopic pubSubTopic, int partitionNum) {
    Set<PubSubTopicPartition> topicPartitionsSet = new HashSet<>();
    for (int i = 0; i < partitionNum; i++) {
      topicPartitionsSet.add(new PubSubTopicPartitionImpl(pubSubTopic, i));
    }
    return topicPartitionsSet;
  }

  @Test
  public void testTopicWiseGetConsumerForHybridMode() throws Exception {
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer1.hasAnySubscription()).thenReturn(false);

    ApacheKafkaConsumerAdapter consumer2 = mock(ApacheKafkaConsumerAdapter.class);
    when(consumer1.hasAnySubscription()).thenReturn(false);

    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(), anyBoolean(), any(), any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");

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
        null,
        false);
    consumerService.start();

    String storeName = Utils.getUniqueString("test_consumer_service");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    PubSubTopic pubSubTopicForStoreVersion1 = pubSubTopicRepository.getTopic(topicForStoreVersion1);
    when(task1.getVersionTopic()).thenReturn(pubSubTopicForStoreVersion1);
    when(task1.isHybridMode()).thenReturn(true);

    SharedKafkaConsumer assignedConsumerForV1 = consumerService
        .assignConsumerFor(pubSubTopicForStoreVersion1, new PubSubTopicPartitionImpl(task1.getVersionTopic(), 0));
    Assert.assertEquals(
        consumerService
            .assignConsumerFor(pubSubTopicForStoreVersion1, new PubSubTopicPartitionImpl(task1.getVersionTopic(), 0)),
        assignedConsumerForV1,
        "The 'getConsumer' function should be idempotent");

    String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    PubSubTopic pubSubTopicForStoreVersion2 = pubSubTopicRepository.getTopic(topicForStoreVersion2);
    when(task2.getVersionTopic()).thenReturn(pubSubTopicForStoreVersion2);
    when(task2.isHybridMode()).thenReturn(true);

    SharedKafkaConsumer assignedConsumerForV2 = consumerService
        .assignConsumerFor(pubSubTopicForStoreVersion2, new PubSubTopicPartitionImpl(pubSubTopicForStoreVersion2, 0));
    Assert.assertNotEquals(
        assignedConsumerForV2,
        assignedConsumerForV1,
        "The 'getConsumer' function should return a different consumer from v1");

    String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
    PubSubTopic pubSubTopicForStoreVersion3 = pubSubTopicRepository.getTopic(topicForStoreVersion3);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(pubSubTopicForStoreVersion3);
    when(task3.isHybridMode()).thenReturn(true);

    try {
      consumerService
          .assignConsumerFor(pubSubTopicForStoreVersion3, new PubSubTopicPartitionImpl(pubSubTopicForStoreVersion3, 0));
      Assert.fail("An exception should be thrown since all 2 consumers should be occupied by other versions");
    } catch (VeniceException e) {
      // expected
    }

    consumerService.stop();
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
    when(factory.create(any(), anyBoolean(), any(), any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "test_kafka_url");

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
        null,
        false);
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
}
