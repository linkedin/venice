package com.linkedin.venice.kafka.consumer;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerTest {
  ApacheKafkaConsumerAdapter consumer;
  PubSubBrokerWrapper kafkaBroker;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void setUp() {
    kafkaBroker = ServiceFactory.getPubSubBroker();
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getAddress());
    consumer = new ApacheKafkaConsumerAdapter(properties, mock(KafkaPubSubMessageDeserializer.class));
  }

  @AfterMethod
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(kafkaBroker);
    Utils.closeQuietlyWithErrorLogged(consumer);
  }

  @Test
  public void testBatchUnsubscribe() {
    PubSubTopic existingTopic1 = pubSubTopicRepository.getTopic("existingTopic1_v1");
    PubSubTopicPartition existingTopicPartition1 = new PubSubTopicPartitionImpl(existingTopic1, 1);
    PubSubTopicPartition existingTopicPartition2 = new PubSubTopicPartitionImpl(existingTopic1, 2);
    PubSubTopicPartition existingTopicPartition3 = new PubSubTopicPartitionImpl(existingTopic1, 3);

    consumer.subscribe(existingTopicPartition1, 100L);
    consumer.subscribe(existingTopicPartition2, 100L);
    consumer.subscribe(existingTopicPartition3, 100L);

    Set<PubSubTopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(existingTopicPartition1);
    topicPartitions.add(existingTopicPartition2);

    consumer.batchUnsubscribe(topicPartitions);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
    topicPartitions.clear();
    topicPartitions.add(existingTopicPartition3);
    consumer.batchUnsubscribe(Collections.singleton(existingTopicPartition3));
    assertConsumerHasNoAssignment(consumer);
  }

  @Test
  public void testPauseAndResume() {
    // Calling pause and resume on an unsubbed partition on a raw Kafka consumer fails,
    // but our wrapper is expected to treat these functions as no-ops in those cases.
    assertConsumerHasNoAssignment(consumer);
    PubSubTopic topicWasNeverSubscribedTo =
        pubSubTopicRepository.getTopic("some_topic_the_consumer_was_never_subscribed_to_v1");
    PubSubTopicPartition topicPartitionWasNeverSubscribedTo =
        new PubSubTopicPartitionImpl(topicWasNeverSubscribedTo, 0);
    consumer.resume(topicPartitionWasNeverSubscribedTo);
    assertConsumerHasNoAssignment(consumer);
    consumer.pause(topicPartitionWasNeverSubscribedTo);
    assertConsumerHasNoAssignment(consumer);

    PubSubTopic topic = pubSubTopicRepository.getTopic("topic_v1");
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 1);
    consumer.subscribe(pubSubTopicPartition, 0);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
    consumer.pause(pubSubTopicPartition);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
    consumer.resume(pubSubTopicPartition);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
  }

  private void assertConsumerHasNoAssignment(ApacheKafkaConsumerAdapter c) {
    Assert.assertEquals(c.getAssignment().size(), 0, "Consumer should have no assignment!");
  }

  private void assertConsumerHasSpecificNumberOfAssignedPartitions(ApacheKafkaConsumerAdapter c, int expected) {
    Assert
        .assertEquals(c.getAssignment().size(), expected, "Consumer should have exactly " + expected + " assignments!");
  }
}
