package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class SharedKafkaConsumerTest {

  @Test
  public void testSubscriptionCleanupDuringPoll() throws InterruptedException {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    Time mockTime = new MockTime();
    final long nonExistingTopicCleanupDelayMS = 1000;

    SharedKafkaConsumer sharedConsumer = new SharedKafkaConsumer(consumer, consumerService, 1, nonExistingTopicCleanupDelayMS, mockTime);
    String existingTopic1 = "existingTopic1";
    String existingTopic2 = "existingTopic2";
    String existingTopicWithoutIngestionTask1 = "existingTopicWithoutIngestionTask1";
    String nonExistingTopic1 = "nonExistingTopic1";
    Map<String, List<PartitionInfo>> topicListReturnedByConsumer = new HashMap<>();
    topicListReturnedByConsumer.put(existingTopic1, Collections.emptyList());
    topicListReturnedByConsumer.put(existingTopic2, Collections.emptyList());
    topicListReturnedByConsumer.put(existingTopicWithoutIngestionTask1, Collections.emptyList());
    doReturn(topicListReturnedByConsumer).when(consumer).listTopics();
    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    assignmentReturnedConsumer.add(new TopicPartition(existingTopic1, 1));
    assignmentReturnedConsumer.add(new TopicPartition(existingTopic2, 1));
    assignmentReturnedConsumer.add(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    assignmentReturnedConsumer.add(new TopicPartition(nonExistingTopic1, 1));
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    Set<TopicPartition> assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask = new HashSet<>(assignmentReturnedConsumer);
    assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask.remove(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    when(consumer.getAssignment()).thenReturn(
        assignmentReturnedConsumer, // after subscribing existingTopic1
        assignmentReturnedConsumer, // after subscribing existingTopic2
        assignmentReturnedConsumer, // after subscribing existingTopicWithoutIngestionTask1
        assignmentReturnedConsumer, // after subscribing nonExistingTopic1
        assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask); // after unsubscription to existingTopicWithoutIngestionTask1
    sharedConsumer.subscribe(existingTopic1, 1, -1);
    sharedConsumer.subscribe(existingTopic2, 1, -1);
    sharedConsumer.subscribe(existingTopicWithoutIngestionTask1, 1, -1);
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    sharedConsumer.attach(existingTopic1, mock(StoreIngestionTask.class));
    sharedConsumer.attach(existingTopic2, mock(StoreIngestionTask.class));
    StoreIngestionTask ingestionTaskForNonExistingTopic1 = mock(StoreIngestionTask.class);
    sharedConsumer.attach(nonExistingTopic1, ingestionTaskForNonExistingTopic1);

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsReturnedByConsumer = new HashMap<>();
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic1, 1), Arrays.asList(new ConsumerRecord<>(existingTopic1, 1, 0, null, null)));
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic2, 1), Arrays.asList(new ConsumerRecord<>(existingTopic2, 1, 0, null, null)));
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopicWithoutIngestionTask1, 1), Arrays.asList(new ConsumerRecord<>(existingTopicWithoutIngestionTask1, 1, 0, null, null)));
    doReturn(new ConsumerRecords(consumerRecordsReturnedByConsumer)).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    Set<TopicPartition> newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    // Shared Consumer should NOT cleanup the subscriptions to the non-existing topics since the delay hasn't exhausted yet
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
    // Shared consumer should cleanup the subscriptions without corresponding ingestion task
    newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    verify(consumer).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats).recordDetectedNoRunningIngestionTopicNum(1);
    // Shared Consumer should cleanup the subscriptions to the non-existing topics since the delay is exhausted
    mockTime.sleep(nonExistingTopicCleanupDelayMS);
    sharedConsumer.poll(1000);
    newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    newAssignment.remove(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    verify(consumer).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1).setLastConsumerException(any());
  }

  @Test
  public void testIngestionTaskBehaviorWhenConsumerListTopicsReturnStaledInfo() throws InterruptedException {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    Time mockTime = new MockTime();
    final long nonExistingTopicCleanupDelayMS = 1000;

    SharedKafkaConsumer sharedConsumer = new SharedKafkaConsumer(consumer, consumerService, 1, nonExistingTopicCleanupDelayMS, mockTime);
    String existingTopic1 = "existingTopic1";
    String nonExistingTopic1 = "nonExistingTopic1";
    Map<String, List<PartitionInfo>> staledTopicListReturnedByConsumer = new HashMap<>();
    staledTopicListReturnedByConsumer.put(existingTopic1, Collections.emptyList());
    Map<String, List<PartitionInfo>> topicListReturnedByConsumer = new HashMap<>(staledTopicListReturnedByConsumer);
    topicListReturnedByConsumer.put(nonExistingTopic1, Collections.emptyList());
    doReturn(topicListReturnedByConsumer).when(consumer).listTopics();
    when(consumer.listTopics()).thenReturn(
        staledTopicListReturnedByConsumer,
        staledTopicListReturnedByConsumer,
        topicListReturnedByConsumer
    );
    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    assignmentReturnedConsumer.add(new TopicPartition(existingTopic1, 1));
    assignmentReturnedConsumer.add(new TopicPartition(nonExistingTopic1, 1));
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer);
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    sharedConsumer.attach(existingTopic1, mock(StoreIngestionTask.class));
    StoreIngestionTask ingestionTaskForNonExistingTopic1 = mock(StoreIngestionTask.class);
    sharedConsumer.attach(nonExistingTopic1, ingestionTaskForNonExistingTopic1);

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsReturnedByConsumer = new HashMap<>();
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic1, 1), Arrays.asList(new ConsumerRecord<>(existingTopic1, 1, 0, null, null)));
    doReturn(new ConsumerRecords(consumerRecordsReturnedByConsumer)).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    Set<TopicPartition> newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    // No cleanup is expected
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
    // No cleanup is expected
    mockTime.sleep(nonExistingTopicCleanupDelayMS / 2);
    sharedConsumer.poll(1000);
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
    // No cleanup is expected within delay
    mockTime.sleep(nonExistingTopicCleanupDelayMS / 2 - 1);
    sharedConsumer.poll(1000);
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
  }
}
