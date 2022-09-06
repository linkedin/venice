package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerTest {
  ApacheKafkaConsumer consumer;
  KafkaBrokerWrapper kafkaBroker;
  private ZkServerWrapper zkServer;

  @BeforeMethod
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    kafkaBroker = ServiceFactory.getKafkaBroker(zkServer);
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getAddress());
    consumer = new ApacheKafkaConsumer(properties);
  }

  @AfterMethod
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(kafkaBroker);
    Utils.closeQuietlyWithErrorLogged(zkServer);
    Utils.closeQuietlyWithErrorLogged(consumer);
  }

  @Test
  public void testBatchUnsubscribe() {
    String existingTopic1 = "existingTopic1_v1";
    consumer.subscribe(existingTopic1, 1, 100L);
    consumer.subscribe(existingTopic1, 2, 100L);
    consumer.subscribe(existingTopic1, 3, 100L);

    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition(existingTopic1, 1));
    topicPartitions.add(new TopicPartition(existingTopic1, 2));

    consumer.batchUnsubscribe(topicPartitions);
    assertConsumerHasSpecificNumberOfAssignmedPartitions(consumer, 1);
    topicPartitions.clear();
    topicPartitions.add(new TopicPartition(existingTopic1, 3));
    consumer.batchUnsubscribe(Collections.singleton(new TopicPartition(existingTopic1, 3)));
    assertConsumerHasNoAssignment(consumer);
  }

  @Test
  public void testPauseAndResume() {
    // Calling pause and resume on an unsubbed partition on a raw Kafka consumer fails,
    // but our wrapper is expected to treat these functions as no-ops in those cases.
    assertConsumerHasNoAssignment(consumer);
    consumer.resume("some_topic_the_consumer_was_never_subscribed_to", 0);
    assertConsumerHasNoAssignment(consumer);
    consumer.pause("some_topic_the_consumer_was_never_subscribed_to", 0);
    assertConsumerHasNoAssignment(consumer);

    String topic = "topic";
    int partition = 1;
    consumer.subscribe(topic, partition, 0);
    assertConsumerHasSpecificNumberOfAssignmedPartitions(consumer, 1);
    consumer.pause(topic, partition);
    assertConsumerHasSpecificNumberOfAssignmedPartitions(consumer, 1);
    consumer.resume(topic, partition);
    assertConsumerHasSpecificNumberOfAssignmedPartitions(consumer, 1);
  }

  private void assertConsumerHasNoAssignment(ApacheKafkaConsumer c) {
    Assert.assertEquals(c.getAssignment().size(), 0, "Consumer should have no assignment!");
  }

  private void assertConsumerHasSpecificNumberOfAssignmedPartitions(ApacheKafkaConsumer c, int expected) {
    Assert
        .assertEquals(c.getAssignment().size(), expected, "Consumer should have exactly " + expected + " assignments!");
  }
}
