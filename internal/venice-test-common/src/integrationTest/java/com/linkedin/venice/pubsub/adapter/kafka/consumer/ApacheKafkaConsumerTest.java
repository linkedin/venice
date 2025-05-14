package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubAdminAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminConfig;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerTest {
  private ApacheKafkaConsumerAdapter consumer;
  private ApacheKafkaAdminAdapter admin;
  private PubSubBrokerWrapper kafkaBroker;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private static final PubSubTopicConfiguration TOPIC_CONFIGURATION =
      new PubSubTopicConfiguration(Optional.of(70000000L), false, Optional.of(1), 700000L, Optional.empty());

  @BeforeMethod
  public void setUp() {
    kafkaBroker = ServiceFactory.getPubSubBroker();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBroker.getAddress());
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
            .setPubSubPositionTypeRegistry(kafkaBroker.getPubSubPositionTypeRegistry())
            .setConsumerName("testConsumer")
            .build());
    consumer = new ApacheKafkaConsumerAdapter(apacheKafkaConsumerConfig);
    admin = new ApacheKafkaAdminAdapter(
        new ApacheKafkaAdminConfig(
            new PubSubAdminAdapterContext.Builder().setAdminClientName("testAdminClient")
                .setVeniceProperties(new VeniceProperties(properties))
                .setPubSubTopicRepository(pubSubTopicRepository)
                .setPubSubPositionTypeRegistry(kafkaBroker.getPubSubPositionTypeRegistry())
                .build()));
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

    // create topic with 4 partitions
    admin.createTopic(existingTopic1, 4, (short) 1, TOPIC_CONFIGURATION);
    assertTrue(admin.containsTopic(existingTopic1));
    assertTrue(admin.containsTopicWithPartitionCheck(existingTopicPartition1));
    assertTrue(admin.containsTopicWithPartitionCheck(existingTopicPartition2));
    assertTrue(admin.containsTopicWithPartitionCheck(existingTopicPartition3));

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
    admin.createTopic(topicWasNeverSubscribedTo, 1, (short) 1, TOPIC_CONFIGURATION);
    assertTrue(admin.containsTopic(topicWasNeverSubscribedTo));

    PubSubTopicPartition topicPartitionWasNeverSubscribedTo =
        new PubSubTopicPartitionImpl(topicWasNeverSubscribedTo, 0);
    consumer.resume(topicPartitionWasNeverSubscribedTo);
    assertConsumerHasNoAssignment(consumer);
    consumer.pause(topicPartitionWasNeverSubscribedTo);
    assertConsumerHasNoAssignment(consumer);

    PubSubTopic topic = pubSubTopicRepository.getTopic("topic_v1");
    admin.createTopic(topic, 2, (short) 1, TOPIC_CONFIGURATION);
    assertTrue(admin.containsTopic(topic));

    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 1);
    consumer.subscribe(pubSubTopicPartition, 0);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
    consumer.pause(pubSubTopicPartition);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
    consumer.resume(pubSubTopicPartition);
    assertConsumerHasSpecificNumberOfAssignedPartitions(consumer, 1);
  }

  private void assertConsumerHasNoAssignment(ApacheKafkaConsumerAdapter c) {
    assertEquals(c.getAssignment().size(), 0, "Consumer should have no assignment!");
  }

  private void assertConsumerHasSpecificNumberOfAssignedPartitions(ApacheKafkaConsumerAdapter c, int expected) {
    assertEquals(c.getAssignment().size(), expected, "Consumer should have exactly " + expected + " assignments!");
  }
}
