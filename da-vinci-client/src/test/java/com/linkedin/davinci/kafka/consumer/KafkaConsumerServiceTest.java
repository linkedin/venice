package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class KafkaConsumerServiceTest {

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

    KafkaConsumerService consumerService = new TopicWiseKafkaConsumerService(factory, properties, 1000l,
        2, mock(EventThrottler.class), mock(EventThrottler.class), mock(KafkaClusterBasedRecordThrottler.class),
        mock(KafkaConsumerServiceStats.class), TimeUnit.MINUTES.toMillis(1), mock(TopicExistenceChecker.class), true);

    KafkaConsumerWrapper assignedConsumerForTask1 = consumerService.getConsumer(task1);
    KafkaConsumerWrapper assignedConsumerForTask2 = consumerService.getConsumer(task2);
    Assert.assertNotEquals(assignedConsumerForTask1, assignedConsumerForTask2,
        "We should avoid to share consumer when there is consumer not assigned topic.");

    // Get partitions assigned to those two consumers
    Set<TopicPartition> consumer1AssignedPartitions = getTopicPartitionsSet(topicForStoreName1, 5);
    when(consumer1.getAssignment()).thenReturn(consumer1AssignedPartitions);

    Set<TopicPartition> consumer2AssignedPartitions = getTopicPartitionsSet(topicForStoreName2, 3);

    when(consumer2.getAssignment()).thenReturn(consumer2AssignedPartitions);
    SharedKafkaConsumer sharedAssignedConsumerForTask1 = (SharedKafkaConsumer) assignedConsumerForTask1;
    sharedAssignedConsumerForTask1.setCurrentAssignment(consumer1AssignedPartitions);
    SharedKafkaConsumer sharedAssignedConsumerForTask2 = (SharedKafkaConsumer) assignedConsumerForTask2;
    sharedAssignedConsumerForTask2.setCurrentAssignment(consumer2AssignedPartitions);

    String storeName3 = Utils.getUniqueString("test_consumer_service3");
    String topicForStoreName3 = Version.composeKafkaTopic(storeName3, 1);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreName3);
    when(task3.isHybridMode()).thenReturn(true);
    KafkaConsumerWrapper assignedConsumerForTask3 = consumerService.getConsumer(task3);
    Assert.assertEquals(assignedConsumerForTask3, assignedConsumerForTask2,
        "The assigned consumer should come with least partitions, when no zero loaded consumer available.");
  }

  private Set<TopicPartition> getTopicPartitionsSet(String topic, int partitionNum) {
    Set<TopicPartition> topicPartitionsSet = new HashSet<>();
    for (int i = 0; i < partitionNum; i++) {
      topicPartitionsSet.add(new TopicPartition(topic, i));
    }
    return topicPartitionsSet;
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

    KafkaConsumerService consumerService = new TopicWiseKafkaConsumerService(factory, properties, 1000l,
        2, mock(EventThrottler.class), mock(EventThrottler.class), mock(KafkaClusterBasedRecordThrottler.class),
        mock(KafkaConsumerServiceStats.class), TimeUnit.MINUTES.toMillis(1), mock(TopicExistenceChecker.class), true);

    String storeName = Utils.getUniqueString("test_consumer_service");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    when(task1.getVersionTopic()).thenReturn(topicForStoreVersion1);
    when(task1.isHybridMode()).thenReturn(true);

    KafkaConsumerWrapper assignedConsumerForV1 = consumerService.getConsumer(task1);
    Assert.assertEquals(consumerService.getConsumer(task1), assignedConsumerForV1, "The 'getConsumer' function should be idempotent");

    String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreVersion2);
    when(task2.isHybridMode()).thenReturn(true);

    KafkaConsumerWrapper assignedConsumerForV2 = consumerService.getConsumer(task2);
    Assert.assertNotEquals(assignedConsumerForV2, assignedConsumerForV1, "The 'getConsumer' function should return a different consumer from v1");


    String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreVersion3);
    when(task3.isHybridMode()).thenReturn(true);

    try {
      consumerService.getConsumer(task3);
      Assert.fail("An exception should be thrown since all 2 consumers should be occupied by other versions");
    } catch (VeniceException e) {
      // expected
    }

    consumerService.stop();
  }

  @Test
  public void testPartitionWiseGetConsumer() throws Exception {
    PartitionWiseSharedKafkaConsumer consumer1 = mock(PartitionWiseSharedKafkaConsumer.class);
    when(consumer1.hasAnySubscription()).thenReturn(true);

    PartitionWiseSharedKafkaConsumer consumer2 = mock(PartitionWiseSharedKafkaConsumer.class);
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

    PartitionWiseKafkaConsumerService consumerService = new PartitionWiseKafkaConsumerService(factory, properties, 1000l,
        2, mock(EventThrottler.class), mock(EventThrottler.class), mock(KafkaClusterBasedRecordThrottler.class),
        mock(KafkaConsumerServiceStats.class), TimeUnit.MINUTES.toMillis(1), mock(TopicExistenceChecker.class), true);

    PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer assignedConsumerForTask1 =
        (PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer) consumerService.getConsumer(task1);
    PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer assignedConsumerForTask2 =
        (PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer) consumerService.getConsumer(task2);
    int partitionNumForTask1 = 4;
    int partitionNumForTask2 = 2;
    for (int partitionId = 0; partitionId < partitionNumForTask1; partitionId++) {
      assignedConsumerForTask1.subscribe(topicForStoreName1, partitionId, 0);
    }
    for (int partitionId = 0; partitionId < partitionNumForTask2; partitionId++) {
      assignedConsumerForTask2.subscribe(topicForStoreName2, partitionId, 0);
    }
    Assert.assertEquals(assignedConsumerForTask1.getAssignment().size(), partitionNumForTask1);
    Assert.assertEquals(assignedConsumerForTask2.getAssignment().size(), partitionNumForTask2);
    Assert.assertNotEquals(
        assignedConsumerForTask1.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName1, 0)),
        assignedConsumerForTask1.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName1, 1))
    );
    Assert.assertEquals(
        assignedConsumerForTask1.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName1, 0)),
        assignedConsumerForTask1.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName1, 2))
    );
    Assert.assertEquals(
        assignedConsumerForTask1.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName1, 3)),
        assignedConsumerForTask2.getSharedKafkaConsumerMap().get(new TopicPartition(topicForStoreName2, 1))
    );
    // Balance  check.
    Assert.assertEquals(consumer1.getAssignmentSize(), consumer2.getAssignmentSize());
    Assert.assertNotEquals(assignedConsumerForTask1, assignedConsumerForTask2,
        "We should avoid to share consumer when there is consumer not assigned topic.");

    // Unsubscribe check.
    for (int partitionId = 0; partitionId < partitionNumForTask1; partitionId++) {
      assignedConsumerForTask1.unSubscribe(topicForStoreName1, partitionId);
      TopicPartition topicPartition = new TopicPartition(topicForStoreName1, partitionId);
      Assert.assertNull(assignedConsumerForTask1.getSharedKafkaConsumerMap().get(topicPartition));
    }
    Assert.assertFalse(assignedConsumerForTask1.hasAnySubscription());
  }

  @Test
  public void testPartitionWiseGetConsumerForRTTopic() throws Exception {
    KafkaConsumerWrapper consumer1 = mock(KafkaConsumerWrapper.class);
    when(consumer1.hasAnySubscription()).thenReturn(false);

    KafkaConsumerWrapper consumer2 = mock(KafkaConsumerWrapper.class);
    when(consumer2.hasAnySubscription()).thenReturn(false);

    KafkaClientFactory factory = mock(KafkaClientFactory.class);
    when(factory.getConsumer(any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_url");

    PartitionWiseKafkaConsumerService consumerService = new PartitionWiseKafkaConsumerService(factory, properties, 1000l,
        2, mock(EventThrottler.class), mock(EventThrottler.class), mock(KafkaClusterBasedRecordThrottler.class),
        mock(KafkaConsumerServiceStats.class), TimeUnit.MINUTES.toMillis(1), mock(TopicExistenceChecker.class), true);

    String storeName = Utils.getUniqueString("test_consumer_service");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    String realTimeTopic = Version.composeRealTimeTopic(storeName);
    when(task1.getVersionTopic()).thenReturn(topicForStoreVersion1);
    when(task1.isHybridMode()).thenReturn(true);

    PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer assignedConsumerForV1 =
        (PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer) consumerService.getConsumer(task1);


    Assert.assertEquals(consumerService.getConsumer(task1), assignedConsumerForV1, "The 'getConsumer' function should be idempotent");

    String topicForStoreVersion2 = Version.composeKafkaTopic(storeName, 2);
    StoreIngestionTask task2 = mock(StoreIngestionTask.class);
    when(task2.getVersionTopic()).thenReturn(topicForStoreVersion2);
    when(task2.isHybridMode()).thenReturn(true);

    PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer assignedConsumerForV2 =
        (PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer) consumerService.getConsumer(task2);
    Assert.assertNotEquals(assignedConsumerForV2, assignedConsumerForV1, "The 'getConsumer' function should return a different consumer from v1");
    assignedConsumerForV1.subscribe(realTimeTopic, 0, 0);
    assignedConsumerForV1.subscribe(realTimeTopic, 1, 0);

    assignedConsumerForV2.subscribe(realTimeTopic, 0, 0);
    assignedConsumerForV2.subscribe(realTimeTopic, 1, 0);

    Assert.assertEquals(
        assignedConsumerForV1.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 0)),
        assignedConsumerForV2.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 1))
    );
    Assert.assertNotEquals(
        assignedConsumerForV1.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 0)),
        assignedConsumerForV2.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 0))
    );

    String topicForStoreVersion3 = Version.composeKafkaTopic(storeName, 3);
    StoreIngestionTask task3 = mock(StoreIngestionTask.class);
    when(task3.getVersionTopic()).thenReturn(topicForStoreVersion3);
    when(task3.isHybridMode()).thenReturn(true);

    PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer assignedConsumerForV3 =
        (PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer) consumerService.getConsumer(task3);
    // We only have 2 consumers and the RT has two 2 partitions, 3rd time RT subscribing partition 0 should not work, we could not find consumer.
    Assert.assertThrows(VeniceException.class, () -> assignedConsumerForV3.subscribe(realTimeTopic, 0, 0));

    assignedConsumerForV1.unSubscribe(realTimeTopic, 0);
    assignedConsumerForV3.subscribe(realTimeTopic, 0, 0);
    Assert.assertEquals(
        assignedConsumerForV3.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 0)),
        assignedConsumerForV2.getSharedKafkaConsumerMap().get(new TopicPartition(realTimeTopic, 1))
    );
    consumerService.stop();
  }
}
