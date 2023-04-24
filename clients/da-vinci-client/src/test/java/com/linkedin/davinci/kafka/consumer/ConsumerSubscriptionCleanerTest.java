package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.TestMockTime;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.testng.annotations.Test;


public class ConsumerSubscriptionCleanerTest {
  protected static final long NON_EXISTING_TOPIC_CLEANUP_DELAY_MS = 1000;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testCleanUp() {
    PubSubTopic existingTopic1 = pubSubTopicRepository.getTopic("existingTopic1_v1");
    PubSubTopic existingTopic2 = pubSubTopicRepository.getTopic("existingTopic2_v1");
    PubSubTopic existingTopicWithoutIngestionTask1 =
        pubSubTopicRepository.getTopic("existingTopicWithoutIngestionTask_v1");
    PubSubTopic nonExistingTopic1 = pubSubTopicRepository.getTopic("nonExistingTopic1_v1");

    Set<PubSubTopicPartition> currentAssignment = new HashSet<>();
    PubSubTopicPartition existingTopicPartitionWithoutIngestionTask =
        new PubSubTopicPartitionImpl(existingTopicWithoutIngestionTask1, 1);
    PubSubTopicPartition nonExistentTopicPartition = new PubSubTopicPartitionImpl(nonExistingTopic1, 1);
    currentAssignment.add(new PubSubTopicPartitionImpl(existingTopic1, 1));
    currentAssignment.add(new PubSubTopicPartitionImpl(existingTopic2, 1));
    currentAssignment.add(existingTopicPartitionWithoutIngestionTask);
    currentAssignment.add(nonExistentTopicPartition);

    TestMockTime time = new TestMockTime();

    Consumer<Set<PubSubTopicPartition>> batchUnsubFunction = mock(Consumer.class);
    int batchUnsubFunctionExpectedCallCount = 0;

    ConsumerSubscriptionCleaner consumerSubscriptionCleaner = new ConsumerSubscriptionCleaner(
        NON_EXISTING_TOPIC_CLEANUP_DELAY_MS,
        1,
        topic -> !topic.equals(nonExistingTopic1.getName()),
        () -> currentAssignment,
        value -> {},
        batchUnsubFunction,
        time);

    // Nothing should be unsubbed prior to configured delay
    Set<PubSubTopicPartition> partitionsToUnsub =
        consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(new HashSet<>());
    assertTrue(partitionsToUnsub.isEmpty());
    verify(batchUnsubFunction, times(batchUnsubFunctionExpectedCallCount)).accept(anySet());

    // After delay, unsubbing should happen
    time.addMilliseconds(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS + 1);
    partitionsToUnsub = consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(partitionsToUnsub);
    assertEquals(partitionsToUnsub.size(), 1);
    assertTrue(partitionsToUnsub.contains(nonExistentTopicPartition));
    verify(batchUnsubFunction, times(++batchUnsubFunctionExpectedCallCount)).accept(partitionsToUnsub);

    // Even after delay, if there's nothing to unsub, then nothing should happen
    currentAssignment.remove(nonExistentTopicPartition);
    time.addMilliseconds(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS + 1);
    partitionsToUnsub = consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(partitionsToUnsub);
    assertTrue(partitionsToUnsub.isEmpty());
    verify(batchUnsubFunction, times(batchUnsubFunctionExpectedCallCount)).accept(anySet()); // N.B. Same number of
                                                                                             // times as before

    // Explicitly call topic-partition to unsub
    Set<PubSubTopicPartition> topicPartitionsToUnsubExplicitly = new HashSet<>();
    topicPartitionsToUnsubExplicitly.add(existingTopicPartitionWithoutIngestionTask);
    consumerSubscriptionCleaner.unsubscribe(topicPartitionsToUnsubExplicitly);
    verify(batchUnsubFunction, times(++batchUnsubFunctionExpectedCallCount)).accept(anySet());
  }
}
