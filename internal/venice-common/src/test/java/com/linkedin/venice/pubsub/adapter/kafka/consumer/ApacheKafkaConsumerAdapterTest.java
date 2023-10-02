package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerAdapterTest {
  private Consumer<byte[], byte[]> internalKafkaConsumer;
  private ApacheKafkaConsumerAdapter kafkaConsumerAdapter;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    internalKafkaConsumer = mock(Consumer.class);
    pubSubMessageDeserializer = mock(PubSubMessageDeserializer.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    topicPartitionsOffsetsTracker = mock(TopicPartitionsOffsetsTracker.class);
    kafkaConsumerAdapter = new ApacheKafkaConsumerAdapter(
        internalKafkaConsumer,
        new VeniceProperties(new Properties()),
        pubSubMessageDeserializer,
        topicPartitionsOffsetsTracker);
  }

  @Test
  public void testBatchUnsubscribe() {
    Map<PubSubTopicPartition, TopicPartition> topicPartitionBeingUnsubscribedMap = new HashMap<>();
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 0), new TopicPartition("t0_v1", 0));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 1), new TopicPartition("t0_v1", 1));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t1_v1"), 1), new TopicPartition("t1_v1", 1));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t2_v1"), 2), new TopicPartition("t2_v1", 2));
    Set<PubSubTopicPartition> pubSubTopicPartitionsToUnsubscribe = topicPartitionBeingUnsubscribedMap.keySet();

    Map<PubSubTopicPartition, TopicPartition> unaffectedTopicPartitionsMap = new HashMap<>();
    unaffectedTopicPartitionsMap.put(
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 101),
        new TopicPartition("t0_v1", 101));
    unaffectedTopicPartitionsMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t4_v1"), 4), new TopicPartition("t4_v1", 4));

    Set<TopicPartition> currentAssignedTopicPartitions = new HashSet<>();
    currentAssignedTopicPartitions.addAll(topicPartitionBeingUnsubscribedMap.values());
    currentAssignedTopicPartitions.addAll(unaffectedTopicPartitionsMap.values());

    when(internalKafkaConsumer.assignment()).thenReturn(currentAssignedTopicPartitions);

    Set<TopicPartition> capturedReassignments = new HashSet<>();
    doAnswer(invocation -> {
      capturedReassignments.addAll(invocation.getArgument(0));
      return null;
    }).when(internalKafkaConsumer).assign(any());

    kafkaConsumerAdapter.batchUnsubscribe(pubSubTopicPartitionsToUnsubscribe);

    // verify that the reassignment contains all the unaffected topic partitions
    assertEquals(
        capturedReassignments,
        new HashSet<>(unaffectedTopicPartitionsMap.values()),
        "all unaffected topic partitions should be reassigned");
    verify(internalKafkaConsumer).assign(eq(capturedReassignments));
    // verify that the offsets tracker is updated for all the topic partitions being unsubscribed
    for (TopicPartition topicPartition: topicPartitionBeingUnsubscribedMap.values()) {
      verify(topicPartitionsOffsetsTracker).removeTrackedOffsets(topicPartition);
    }
  }
}
