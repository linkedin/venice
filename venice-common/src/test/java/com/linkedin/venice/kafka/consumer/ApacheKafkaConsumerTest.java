package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerTest {
  ApacheKafkaConsumer consumer;
  String existingTopic1 = "existingTopic1_v1";
  @BeforeMethod
  public void setConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ServiceFactory.getKafkaBroker().getAddress());
    consumer = new ApacheKafkaConsumer(properties);
  }

  @Test
  public void testBatchUnsubscribe() {
    consumer.subscribe(existingTopic1, 1, 100L);
    consumer.subscribe(existingTopic1, 2, 100L);
    consumer.subscribe(existingTopic1, 3, 100L);

    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition(existingTopic1, 1));
    topicPartitions.add(new TopicPartition(existingTopic1, 2));

    consumer.batchUnsubscribe(topicPartitions);
    Assert.assertEquals(consumer.getAssignment().size(), 1);
    topicPartitions.clear();
    topicPartitions.add(new TopicPartition(existingTopic1, 3));
    consumer.batchUnsubscribe(Collections.singleton(new TopicPartition(existingTopic1, 3)));
    Assert.assertEquals(consumer.getAssignment().size(), 0);

  }
}
