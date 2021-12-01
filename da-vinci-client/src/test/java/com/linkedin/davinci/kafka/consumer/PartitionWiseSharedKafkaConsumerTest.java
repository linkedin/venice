package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class PartitionWiseSharedKafkaConsumerTest {
  @Test
  public void testSubscriptionCleanupDuringPoll() throws InterruptedException {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    Time mockTime = new MockTime();
    final long nonExistingTopicCleanupDelayMS = 1000;
    String existingTopic1 = "existingTopic1_v1";
    String existingTopicWithoutIngestionTask1 = "existingTopicWithoutIngestionTask1";
    String nonExistingTopic1 = "nonExistingTopic1_v1";

    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    MetadataRepoBasedTopicExistingCheckerImpl topicExistenceChecker = new MetadataRepoBasedTopicExistingCheckerImpl(repository);
    Store store = mock(Store.class);
    doReturn(Optional.of(new VersionImpl("existingTopic", 1))).when(store).getVersion(1);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic1");
    doReturn(store).when(repository).getStoreOrThrow("existingTopic2");
    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    PartitionWiseSharedKafkaConsumer sharedConsumer = new PartitionWiseSharedKafkaConsumer(
        consumer, consumerService, 1, nonExistingTopicCleanupDelayMS, mockTime, topicExistenceChecker);

    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    TopicPartition existingTopicPartition1 = new TopicPartition(existingTopic1, 1);
    TopicPartition existingTopicPartitionWithoutIngestionTask1 = new TopicPartition(existingTopicWithoutIngestionTask1, 1);
    TopicPartition nonExistingTopicPartition1 = new TopicPartition(nonExistingTopic1, 1);
    TopicPartition nonExistingTopicPartition0 = new TopicPartition(nonExistingTopic1, 0);
    assignmentReturnedConsumer.add(existingTopicPartition1);
    assignmentReturnedConsumer.add(existingTopicPartitionWithoutIngestionTask1);
    assignmentReturnedConsumer.add(nonExistingTopicPartition1);
    assignmentReturnedConsumer.add(nonExistingTopicPartition0);
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    Set<TopicPartition> assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask = new HashSet<>(assignmentReturnedConsumer);
    assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask.remove(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    when(consumer.getAssignment()).thenReturn(
        assignmentReturnedConsumer, // after subscribing existingTopicPartition1
        assignmentReturnedConsumer, // after subscribing existingTopicPartitionWithoutIngestionTask1
        assignmentReturnedConsumer, // after subscribing nonExistingTopicPartition0
        assignmentReturnedConsumer, // after subscribing nonExistingTopicPartition1
        assignmentReturnedConsumerWithoutExistingTopicWithoutIngestionTask); // after unsubscription to existingTopicWithoutIngestionTask1
    sharedConsumer.subscribe(existingTopic1, 1, -1);
    sharedConsumer.subscribe(existingTopicWithoutIngestionTask1, 1, -1);
    sharedConsumer.subscribe(nonExistingTopic1, 0, -1);
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    StoreIngestionTask ingestionTaskForNonExistingTopicPartition1 = mock(StoreIngestionTask.class);
    StoreIngestionTask ingestionTaskForNonExistingTopicPartition0 = mock(StoreIngestionTask.class);
    // Build the mapping from topic partition to ingestion task.
    sharedConsumer.addIngestionTaskForTopicPartition(existingTopicPartition1, mock(StoreIngestionTask.class));
    sharedConsumer.addIngestionTaskForTopicPartition(nonExistingTopicPartition1, ingestionTaskForNonExistingTopicPartition1);
    sharedConsumer.addIngestionTaskForTopicPartition(nonExistingTopicPartition0, ingestionTaskForNonExistingTopicPartition0);
    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>>
        consumerRecordsReturnedByConsumer = new HashMap<>();
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic1, 1),
        Collections.singletonList(new ConsumerRecord<>(existingTopic1, 1, 0, null, null)));
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopicWithoutIngestionTask1, 1),
        Collections.singletonList(new ConsumerRecord<>(existingTopicWithoutIngestionTask1, 1, 0, null, null)));

    doReturn(new ConsumerRecords(consumerRecordsReturnedByConsumer)).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    Set<TopicPartition> newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    // Shared Consumer should NOT cleanup the subscriptions to the non-existing topics since the delay hasn't exhausted yet
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopicPartition1, never()).setLastConsumerException(any());
    // Shared consumer should cleanup the subscriptions without corresponding ingestion task
    newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(existingTopicPartitionWithoutIngestionTask1);
    verify(consumer).assign(new HashSet<>(newAssignment));
    verify(consumerServiceStats).recordDetectedNoRunningIngestionTopicPartitionNum(1);
    // Shared Consumer should cleanup the subscriptions to the non-existing topics since the delay is exhausted
    mockTime.sleep(nonExistingTopicCleanupDelayMS);
    sharedConsumer.poll(1000);
    newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(nonExistingTopicPartition1);
    newAssignment.remove(existingTopicPartitionWithoutIngestionTask1);
    newAssignment.remove(nonExistingTopicPartition0);
    verify(consumer).assign(new HashSet<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopicPartition1).setLastConsumerException(any());
    verify(ingestionTaskForNonExistingTopicPartition0).setLastConsumerException(any());
  }

  @Test
  public void testSubscriptionEmptyPoll() throws InterruptedException {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    Time mockTime = new MockTime();
    final long nonExistingTopicCleanupDelayMS = 1000;
    String nonExistingTopic1 = "nonExistingTopic1_v3";

    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    MetadataRepoBasedTopicExistingCheckerImpl topicExistenceChecker = new MetadataRepoBasedTopicExistingCheckerImpl(repository);
    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    PartitionWiseSharedKafkaConsumer sharedConsumer = new PartitionWiseSharedKafkaConsumer(
        consumer, consumerService, 1, nonExistingTopicCleanupDelayMS, mockTime, topicExistenceChecker);
    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    assignmentReturnedConsumer.add(new TopicPartition(nonExistingTopic1, 1));
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer); // after unsubscription to existingTopicWithoutIngestionTask1
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    StoreIngestionTask ingestionTaskForNonExistingTopic1 = mock(StoreIngestionTask.class);
    sharedConsumer.addIngestionTaskForTopicPartition(new TopicPartition(nonExistingTopic1, 1), ingestionTaskForNonExistingTopic1);

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsReturnedByConsumer = new HashMap<>();
    doReturn(new ConsumerRecords(consumerRecordsReturnedByConsumer)).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    Set<TopicPartition> newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    mockTime.sleep(nonExistingTopicCleanupDelayMS);
    doReturn(Collections.emptySet()).when(consumer).getAssignment();

    sharedConsumer.poll(1000);

    verify(consumer, times(1)).poll(1000);
  }

  @Test
  public void testIngestionTaskBehaviorWhenConsumerListTopicsReturnStaleInfo() throws InterruptedException {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    Time mockTime = new MockTime();
    final long nonExistingTopicCleanupDelayMS = 1000;
    String existingTopic1 = "existingTopic1_v1";
    String nonExistingTopic1 = "nonExistingTopic1_v1";

    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    MetadataRepoBasedTopicExistingCheckerImpl topicExistenceChecker = new MetadataRepoBasedTopicExistingCheckerImpl(repository);
    Store store = mock(Store.class);
    doReturn(Optional.of(new VersionImpl("existingTopic", 1))).when(store).getVersion(1);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic1");
    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    PartitionWiseSharedKafkaConsumer sharedConsumer = new PartitionWiseSharedKafkaConsumer(
        consumer, consumerService, 1, nonExistingTopicCleanupDelayMS, mockTime, topicExistenceChecker);
    Map<String, List<PartitionInfo>> staledTopicListReturnedByConsumer = new HashMap<>();
    staledTopicListReturnedByConsumer.put(existingTopic1, Collections.emptyList());
    Map<String, List<PartitionInfo>> topicListReturnedByConsumer = new HashMap<>(staledTopicListReturnedByConsumer);
    topicListReturnedByConsumer.put(nonExistingTopic1, Collections.emptyList());
    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    assignmentReturnedConsumer.add(new TopicPartition(existingTopic1, 1));
    assignmentReturnedConsumer.add(new TopicPartition(nonExistingTopic1, 1));
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer);
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    StoreIngestionTask ingestionTaskForNonExistingTopic1 = mock(StoreIngestionTask.class);
    sharedConsumer.addIngestionTaskForTopicPartition(new TopicPartition(existingTopic1, 1), mock(StoreIngestionTask.class));
    // Build the mapping from topic partition to ingestion task.
    sharedConsumer.addIngestionTaskForTopicPartition(new TopicPartition(nonExistingTopic1, 1), ingestionTaskForNonExistingTopic1);

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
    doReturn(store).when(repository).getStoreOrThrow("nonExistingTopic1");
    mockTime.sleep(nonExistingTopicCleanupDelayMS / 2 - 1);
    sharedConsumer.poll(1000);
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
  }
}
