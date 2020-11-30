package com.linkedin.davinci.kafka.consumer;


import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.TestUtils;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class KafkaConsumerServiceTest {

  @Test
  public void testGetConsumerForHybridMode() throws Exception {
    KafkaConsumerWrapper consumer1 = mock(KafkaConsumerWrapper.class);
    when(consumer1.hasSubscription()).thenReturn(false);

    KafkaConsumerWrapper consumer2 = mock(KafkaConsumerWrapper.class);
    when(consumer1.hasSubscription()).thenReturn(false);

    KafkaClientFactory factory = mock(KafkaClientFactory.class);
    when(factory.getConsumer(any())).thenReturn(consumer1, consumer2);

    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "test_kafka_url");

    KafkaConsumerService consumerService = new KafkaConsumerService(factory, properties, 1000l,
        2, mock(EventThrottler.class), mock(EventThrottler.class), mock(KafkaConsumerServiceStats.class));

    String storeName = TestUtils.getUniqueString("test_consumer_service");
    StoreIngestionTask task1 = mock(StoreIngestionTask.class);
    String topicForStoreVersion1 = Version.composeKafkaTopic(storeName, 1);
    when(task1.getVersionTopic()).thenReturn(topicForStoreVersion1);
    when(task1.isHybridMode()).thenReturn(true);

    KafkaConsumerWrapper assignedConsumerForV1 = consumerService.getConsumer(task1);
    assignedConsumerForV1 = consumerService.getConsumer(task1);
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
}
