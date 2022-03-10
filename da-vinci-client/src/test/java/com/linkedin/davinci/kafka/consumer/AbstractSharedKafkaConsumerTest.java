package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/**
 * A test class for the tests which are written generically, in such a way that they apply
 * to both the topic-wise shared consumer (implemented as {@link SharedKafkaConsumer}) and
 * the {@link PartitionWiseSharedKafkaConsumer}.
 *
 * TODO: Some of the tests in the child classes are pretty similar and could be refactored
 *       though it may not be worth it considering that we will probably want to deprecate
 *       the topic-wise shared consumer at some point anyway.
 *
 * @param <C> The consumer implementation
 * @param <S> The unit of subscription (e.g. topic name or {@link org.apache.kafka.common.TopicPartition})
 */
public abstract class AbstractSharedKafkaConsumerTest<C extends SharedKafkaConsumer, S> {
  protected static final long NON_EXISTING_TOPIC_CLEANUP_DELAY_MS = 1000;

  protected KafkaConsumerWrapper consumer;
  protected KafkaConsumerServiceStats consumerServiceStats;
  protected Time mockTime;
  private KafkaConsumerService consumerService;
  protected ReadOnlyStoreRepository repository;

  @BeforeMethod
  public void setUp() {
    consumer = mock(KafkaConsumerWrapper.class);
    consumerService = mock(KafkaConsumerService.class);
    consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();
    mockTime = new MockTime();
    repository = mock(ReadOnlyStoreRepository.class);
  }

  protected C getConsumer() {
    MetadataRepoBasedTopicExistingCheckerImpl topicExistenceChecker =
        new MetadataRepoBasedTopicExistingCheckerImpl(repository);

    return instantiateConsumer(
        consumer,
        consumerService,
        1,
        NON_EXISTING_TOPIC_CLEANUP_DELAY_MS,
        mockTime,
        topicExistenceChecker);
  }

  protected abstract C instantiateConsumer(
      KafkaConsumerWrapper delegate,
      KafkaConsumerService service,
      int sanitizeInterval,
      long cleanupDelayMS,
      Time time,
      TopicExistenceChecker topicChecker);

  protected abstract Set<S> getThingsWithoutCorrespondingIngestionTask(C consumer);

  protected abstract Set<S> instantiateUnitsOfSubscription(Set<TopicPartition> topicPartitions);

  /**
   * The way in which a {@link StoreIngestionTask} is attached to the consumer is different between partition-wise
   * and topic-wise implementations. This might be tech debt worth cleaning up, as the difference makes the code
   * more complex to reason about...
   */
  protected abstract void subscribe(C consumer, TopicPartition topicPartition, StoreIngestionTask storeIngestionTask);

  @Test
  public void testSanitizeTopicsWithoutCorrespondingIngestionTask() {
    // Given a consumer, some topic-partitions, and some records
    C consumerUnderTest = getConsumer();
    Set<TopicPartition> someTopicPartitions = new HashSet<>();
    TopicPartition firstTopicPartition = new TopicPartition("topic1", 1);
    TopicPartition secondTopicPartition = new TopicPartition("topic1", 2);
    TopicPartition thirdTopicPartition = new TopicPartition("topic2", 1);
    someTopicPartitions.add(firstTopicPartition);
    someTopicPartitions.add(secondTopicPartition);
    someTopicPartitions.add(thirdTopicPartition);
    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> someRecordsByTopicPartitions = new HashMap<>();
    for (TopicPartition topicPartition: someTopicPartitions) {
      List<ConsumerRecord<Object, Object>> consumerRecordList = new ArrayList<>();
      for (int offset = 0; offset < 10; offset++) {
        consumerRecordList.add(new ConsumerRecord<>(
            topicPartition.topic(), topicPartition.partition(), offset, new Object(), new Object()));
      }
      someRecordsByTopicPartitions.put(topicPartition, consumerRecordList);
    }
    int expectedNumberOfCallsToSubscribeSoFar = 0;
    int expectedNumberOfCallsToAssignSoFar = 0;

    // Scenario 1: no subs, empty input
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> emptyRecords = ConsumerRecords.empty();
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(emptyRecords);
    Set<S> thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertTrue(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should not be anything flagged for removal by sanitization when processing an empty input.");
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 2: no subs, some input
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> someRecords =
        new ConsumerRecords(someRecordsByTopicPartitions);
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(someRecords);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization = getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    Set<S> unitsOfSubscriptionWithoutCorrespondingIngestionTask =
        instantiateUnitsOfSubscription(someTopicPartitions);
    assertEquals(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.size(),
        unitsOfSubscriptionWithoutCorrespondingIngestionTask.size(),
        "When there are no subscriptions, all inputs should be flagged for removal by sanitization.");
    for (S unitOfSubscription: thingsWithoutCorrespondingIngestionTaskFromLastSanitization) {
      assertTrue(unitsOfSubscriptionWithoutCorrespondingIngestionTask.contains(unitOfSubscription),
          "The expected unit of subscription was not flagged by sanitization.");
    }
    for (S unitOfSubscription: unitsOfSubscriptionWithoutCorrespondingIngestionTask) {
      assertTrue(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.contains(unitOfSubscription),
          "A unit of subscription was flagged by sanitization but should not have been.");
    }
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 3: some subs, empty input
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doReturn(someTopicPartitions).when(this.consumer).getAssignment();
    for (TopicPartition topicPartition: someTopicPartitions) {
      consumerUnderTest.subscribe(topicPartition.topic(), topicPartition.partition(), 0);
      verify(this.consumer).subscribe(topicPartition.topic(), topicPartition.partition(), 0);
      expectedNumberOfCallsToSubscribeSoFar++;
      subscribe(consumerUnderTest, topicPartition, storeIngestionTask);
    }
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(emptyRecords);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertTrue(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should not be anything flagged for removal by sanitization when processing an empty input.");
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 4: some subs, some input that corresponds exactly to the subs
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(someRecords);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertTrue(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should not be anything flagged for removal by sanitization when processing an input that corresponds exactly to the subs.");
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 5: some subs, some input that corresponding to a subset of the subs
    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> someRecordsByTopicPartitionsCorrespondingToSubsetOfSubs =
        new HashMap<>();
    someRecordsByTopicPartitionsCorrespondingToSubsetOfSubs.put(
        firstTopicPartition, someRecordsByTopicPartitions.get(firstTopicPartition));
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> someRecordsCorrespondingToSubsetOfSubs =
        new ConsumerRecords(someRecordsByTopicPartitionsCorrespondingToSubsetOfSubs);
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(someRecordsCorrespondingToSubsetOfSubs);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertTrue(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should not be anything flagged for removal by sanitization when processing an input that corresponds to a subset of the subs.");
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 6: some subs, some input that corresponding to a superset of the subs
    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> someRecordsByTopicPartitionsCorrespondingToSupersetOfSubs =
        new HashMap<>();
    someRecordsByTopicPartitions.forEach((k, v) -> someRecordsByTopicPartitionsCorrespondingToSupersetOfSubs.put(k, v));
    List<ConsumerRecord<Object, Object>> consumerRecordList = new ArrayList<>();
    TopicPartition topicPartition = new TopicPartition("topic3", 2);
    for (int offset = 0; offset < 10; offset++) {
      consumerRecordList.add(new ConsumerRecord<>(
          topicPartition.topic(), topicPartition.partition(), offset, new Object(), new Object()));
    }
    someRecordsByTopicPartitionsCorrespondingToSupersetOfSubs.put(topicPartition, consumerRecordList);
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> someRecordsCorrespondingToSupersetOfSubs =
        new ConsumerRecords(someRecordsByTopicPartitionsCorrespondingToSupersetOfSubs);
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(someRecordsCorrespondingToSupersetOfSubs);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertFalse(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should be something flagged for removal by sanitization when processing an input that corresponds to a superset of the subs.");
    verify(this.consumer, times(expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

    // Scenario 7: some subs, some input that corresponds exactly to the subs, but a missing ingestion task
    doReturn(someRecordsByTopicPartitionsCorrespondingToSupersetOfSubs.keySet()).when(this.consumer).getAssignment();
    consumerUnderTest.subscribe(topicPartition.topic(), topicPartition.partition(), 0);
    consumerUnderTest.sanitizeTopicsWithoutCorrespondingIngestionTask(someRecordsCorrespondingToSupersetOfSubs);
    thingsWithoutCorrespondingIngestionTaskFromLastSanitization =
        getThingsWithoutCorrespondingIngestionTask(consumerUnderTest);
    assertFalse(thingsWithoutCorrespondingIngestionTaskFromLastSanitization.isEmpty(),
        "There should be something flagged for removal by sanitization when processing an input that corresponds to a superset of the subs.");
    verify(this.consumer, times(++expectedNumberOfCallsToSubscribeSoFar)).subscribe(anyString(), anyInt(), anyLong());
    verify(this.consumer, times(++expectedNumberOfCallsToAssignSoFar)).assign(anyCollection());

  }
}
