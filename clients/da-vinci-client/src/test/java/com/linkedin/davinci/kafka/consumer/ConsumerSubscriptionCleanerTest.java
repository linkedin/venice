package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestMockTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;


public class ConsumerSubscriptionCleanerTest {
  protected static final long NON_EXISTING_TOPIC_CLEANUP_DELAY_MS = 1000;

  @Test
  public void testCleanUp() {
    String existingTopic1 = "existingTopic1_v1";
    String existingTopic2 = "existingTopic2_v1";
    String existingTopicWithoutIngestionTask1 = "existingTopicWithoutIngestionTask1";
    String nonExistingTopic1 = "nonExistingTopic1_v1";

    Set<TopicPartition> currentAssignment = new HashSet<>();
    TopicPartition existingTopicPartitionWithoutIngestionTask =
        new TopicPartition(existingTopicWithoutIngestionTask1, 1);
    TopicPartition nonExistentTopicPartition = new TopicPartition(nonExistingTopic1, 1);
    currentAssignment.add(new TopicPartition(existingTopic1, 1));
    currentAssignment.add(new TopicPartition(existingTopic2, 1));
    currentAssignment.add(existingTopicPartitionWithoutIngestionTask);
    currentAssignment.add(nonExistentTopicPartition);

    TestMockTime time = new TestMockTime();

    Consumer<Set<TopicPartition>> batchUnsubFunction = mock(Consumer.class);
    int batchUnsubFunctionExpectedCallCount = 0;

    ConsumerSubscriptionCleaner consumerSubscriptionCleaner = new ConsumerSubscriptionCleaner(
        NON_EXISTING_TOPIC_CLEANUP_DELAY_MS,
        1,
        topic -> !topic.equals(nonExistingTopic1),
        () -> currentAssignment,
        value -> {},
        batchUnsubFunction,
        time);

    // Nothing should be unsubbed prior to configured delay
    Set<TopicPartition> partitionsToUnsub =
        consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(new HashSet<>());
    assertTrue(partitionsToUnsub.isEmpty());
    verify(batchUnsubFunction, times(batchUnsubFunctionExpectedCallCount)).accept(anySet());

    // After delay, unsubbing should happen
    time.add(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS + 1, TimeUnit.MILLISECONDS);
    partitionsToUnsub = consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(partitionsToUnsub);
    assertEquals(partitionsToUnsub.size(), 1);
    assertTrue(partitionsToUnsub.contains(nonExistentTopicPartition));
    verify(batchUnsubFunction, times(++batchUnsubFunctionExpectedCallCount)).accept(partitionsToUnsub);

    // Even after delay, if there's nothing to unsub, then nothing should happen
    currentAssignment.remove(nonExistentTopicPartition);
    time.add(NON_EXISTING_TOPIC_CLEANUP_DELAY_MS + 1, TimeUnit.MILLISECONDS);
    partitionsToUnsub = consumerSubscriptionCleaner.getTopicPartitionsToUnsubscribe(partitionsToUnsub);
    assertTrue(partitionsToUnsub.isEmpty());
    verify(batchUnsubFunction, times(batchUnsubFunctionExpectedCallCount)).accept(anySet()); // N.B. Same number of
                                                                                             // times as before

    // Explicitly call topic-partition to unsub
    Set<TopicPartition> topicPartitionsToUnsubExplicitly = new HashSet<>();
    topicPartitionsToUnsubExplicitly.add(existingTopicPartitionWithoutIngestionTask);
    consumerSubscriptionCleaner.unsubscribe(topicPartitionsToUnsubExplicitly);
    verify(batchUnsubFunction, times(++batchUnsubFunctionExpectedCallCount)).accept(anySet());
  }
}
