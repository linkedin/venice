package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.SystemTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SharedKafkaConsumerTest {
  protected PubSubConsumerAdapter consumer;
  protected AggKafkaConsumerServiceStats stats;

  protected PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private SharedKafkaConsumer sharedKafkaConsumer;
  private Set<PubSubTopicPartition> topicPartitions;
  private PubSubConsumerAdapter consumerAdapter;

  @BeforeMethod
  public void setUp() {
    consumer = mock(PubSubConsumerAdapter.class);
    stats = mock(AggKafkaConsumerServiceStats.class);
  }

  @Test
  public void testSubscriptionEmptyPoll() {
    PubSubTopic nonExistingTopic1 = pubSubTopicRepository.getTopic("nonExistingTopic1_v3");

    SharedKafkaConsumer sharedConsumer = new SharedKafkaConsumer(consumer, stats, () -> {}, (c, vt, tp) -> {});

    Set<PubSubTopicPartition> assignmentReturnedConsumer = new HashSet<>();
    PubSubTopicPartition nonExistentPubSubTopicPartition = new PubSubTopicPartitionImpl(nonExistingTopic1, 1);
    assignmentReturnedConsumer.add(nonExistentPubSubTopicPartition);
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer);
    sharedConsumer.subscribe(nonExistingTopic1, nonExistentPubSubTopicPartition, PubSubSymbolicPosition.EARLIEST);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pubSubMessagesReturnedByConsumer = new HashMap<>();
    doReturn(pubSubMessagesReturnedByConsumer).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    verify(consumer, times(1)).poll(1000);

    when(consumer.getAssignment()).thenReturn(Collections.emptySet()); // after unsubscription to
    sharedConsumer.unSubscribe(nonExistentPubSubTopicPartition);

    sharedConsumer.poll(1000);
    verify(consumer, times(1)).poll(1000);
  }

  private void setUpSharedConsumer() {
    consumerAdapter = mock(PubSubConsumerAdapter.class);
    stats = mock(AggKafkaConsumerServiceStats.class);
    Runnable assignmentChangeListener = mock(Runnable.class);
    SharedKafkaConsumer.UnsubscriptionListener unsubscriptionListener =
        mock(SharedKafkaConsumer.UnsubscriptionListener.class);

    sharedKafkaConsumer = new SharedKafkaConsumer(
        consumerAdapter,
        stats,
        assignmentChangeListener,
        unsubscriptionListener,
        new SystemTime());
    topicPartitions = new HashSet<>();
    topicPartitions.add(mock(PubSubTopicPartition.class));
  }

  @Test
  public void testWaitAfterUnsubscribe() {
    setUpSharedConsumer();
    Supplier<Set<PubSubTopicPartition>> supplier = () -> topicPartitions;
    long pollTimesBeforeUnsubscribe = sharedKafkaConsumer.getPollTimes();
    sharedKafkaConsumer.unSubscribeAction(supplier, TimeUnit.SECONDS.toMillis(1));

    // This is to test that if the poll time is not incremented when the consumer is unsubscribed the correct log can
    // be found in the logs.
    Assert.assertEquals(pollTimesBeforeUnsubscribe, sharedKafkaConsumer.getPollTimes());
  }
}
