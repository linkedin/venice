package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
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
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TopicWiseSharedKafkaConsumerTest
    extends AbstractSharedKafkaConsumerTest<TopicWiseSharedKafkaConsumer, String> {
  @Test
  public void testSubscriptionCleanupDuringPoll() throws InterruptedException {
    String existingTopic1 = "existingTopic1_v1";
    String existingTopic2 = "existingTopic2_v1";
    String existingTopicWithoutIngestionTask1 = "existingTopicWithoutIngestionTask1";
    String nonExistingTopic1 = "nonExistingTopic1_v1";

    Store store = mock(Store.class);
    doReturn(Optional.of(new VersionImpl("existingTopic", 1))).when(store).getVersion(1);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic1");
    doReturn(store).when(repository).getStoreOrThrow("existingTopic2");
    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    SharedKafkaConsumer sharedConsumer = getConsumer();

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
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic1, 1),
        Collections.singletonList(new ConsumerRecord<>(existingTopic1, 1, 0, null, null)));
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopic2, 1),
        Collections.singletonList(new ConsumerRecord<>(existingTopic2, 1, 0, null, null)));
    consumerRecordsReturnedByConsumer.put(new TopicPartition(existingTopicWithoutIngestionTask1, 1),
        Collections.singletonList(new ConsumerRecord<>(existingTopicWithoutIngestionTask1, 1, 0, null, null)));

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
    verify(consumer).assign(new HashSet<>(newAssignment));
    verify(consumerServiceStats).recordDetectedNoRunningIngestionTopicNum(1);
    // Shared Consumer should cleanup the subscriptions to the non-existing topics since the delay is exhausted
    mockTime.sleep(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS);
    sharedConsumer.poll(1000);
    newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    newAssignment.remove(new TopicPartition(existingTopicWithoutIngestionTask1, 1));
    verify(consumer).assign(new HashSet<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1).setLastConsumerException(any());
  }

  @Test
  public void testSubscriptionEmptyPoll() throws InterruptedException {
    String nonExistingTopic1 = "nonExistingTopic1_v3";

    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    SharedKafkaConsumer sharedConsumer = getConsumer();

    Set<TopicPartition> assignmentReturnedConsumer = new HashSet<>();
    assignmentReturnedConsumer.add(new TopicPartition(nonExistingTopic1, 1));
    doReturn(assignmentReturnedConsumer).when(consumer).getAssignment();
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer); // after unsubscription to existingTopicWithoutIngestionTask1
    sharedConsumer.subscribe(nonExistingTopic1, 1, -1);
    StoreIngestionTask ingestionTaskForNonExistingTopic1 = mock(StoreIngestionTask.class);
    sharedConsumer.attach(nonExistingTopic1, ingestionTaskForNonExistingTopic1);

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsReturnedByConsumer = new HashMap<>();
    doReturn(new ConsumerRecords(consumerRecordsReturnedByConsumer)).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    Set<TopicPartition> newAssignment = new HashSet<>(assignmentReturnedConsumer);
    newAssignment.remove(new TopicPartition(nonExistingTopic1, 1));
    mockTime.sleep(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS);
    doReturn(Collections.emptySet()).when(consumer).getAssignment();

    sharedConsumer.poll(1000);

    verify(consumer, times(1)).poll(1000);
  }

  @Test
  public void testIngestionTaskBehaviorWhenConsumerListTopicsReturnStaleInfo() throws InterruptedException {
    String existingTopic1 = "existingTopic1_v1";
    String nonExistingTopic1 = "nonExistingTopic1_v1";

    Store store = mock(Store.class);
    doReturn(Optional.of(new VersionImpl("existingTopic", 1))).when(store).getVersion(1);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic1");
    doThrow(new VeniceNoStoreException(nonExistingTopic1)).when(repository).getStoreOrThrow("nonExistingTopic1");

    SharedKafkaConsumer sharedConsumer = getConsumer();

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
    mockTime.sleep(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS / 2);
    sharedConsumer.poll(1000);
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
    // No cleanup is expected within delay
    doReturn(store).when(repository).getStoreOrThrow("nonExistingTopic1");
    mockTime.sleep(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS / 2 - 1);
    sharedConsumer.poll(1000);
    verify(consumer, never()).assign(new ArrayList<>(newAssignment));
    verify(consumerServiceStats, times(2)).recordDetectedDeletedTopicNum(1);
    verify(ingestionTaskForNonExistingTopic1, never()).setLastConsumerException(any());
  }

  @Override
  protected TopicWiseSharedKafkaConsumer instantiateConsumer(
      KafkaConsumerWrapper delegate,
      KafkaConsumerService service,
      int sanitizeInterval,
      long cleanupDelayMS,
      Time time,
      TopicExistenceChecker topicChecker) {
    return new TopicWiseSharedKafkaConsumer(delegate, service, sanitizeInterval, cleanupDelayMS, time, topicChecker);
  }

  @Override
  protected Set<String> getThingsWithoutCorrespondingIngestionTask(TopicWiseSharedKafkaConsumer consumer) {
    return consumer.getTopicsWithoutCorrespondingIngestionTask();
  }

  @Override
  protected Set<String> instantiateUnitsOfSubscription(Set<TopicPartition> topicPartitions) {
    return topicPartitions.stream()
        .map(topicPartition -> topicPartition.topic())
        .collect(Collectors.toSet());
  }

  @Override
  protected void subscribe(TopicWiseSharedKafkaConsumer consumer, TopicPartition topicPartition,
      StoreIngestionTask storeIngestionTask) {
    /** This is called by {@link TopicWiseKafkaConsumerService#attach(KafkaConsumerWrapper, String, StoreIngestionTask)} */
    consumer.attach(topicPartition.topic(), storeIngestionTask);
  }
}
