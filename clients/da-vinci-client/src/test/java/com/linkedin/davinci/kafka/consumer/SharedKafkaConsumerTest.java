package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SharedKafkaConsumerTest {
  protected PubSubConsumerAdapter consumer;
  protected KafkaConsumerServiceStats consumerServiceStats;

  protected PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void setUp() {
    consumer = mock(PubSubConsumerAdapter.class);
    consumerServiceStats = mock(KafkaConsumerServiceStats.class);
  }

  @Test
  public void testSubscriptionEmptyPoll() {
    PubSubTopic nonExistingTopic1 = pubSubTopicRepository.getTopic("nonExistingTopic1_v3");

    SharedKafkaConsumer sharedConsumer =
        new SharedKafkaConsumer(consumer, consumerServiceStats, () -> {}, (c, tp) -> {});

    Set<PubSubTopicPartition> assignmentReturnedConsumer = new HashSet<>();
    PubSubTopicPartition nonExistentPubSubTopicPartition = new PubSubTopicPartitionImpl(nonExistingTopic1, 1);
    assignmentReturnedConsumer.add(nonExistentPubSubTopicPartition);
    when(consumer.getAssignment()).thenReturn(assignmentReturnedConsumer);
    sharedConsumer.subscribe(nonExistingTopic1, nonExistentPubSubTopicPartition, -1);

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> pubSubMessagesReturnedByConsumer =
        new HashMap<>();
    doReturn(pubSubMessagesReturnedByConsumer).when(consumer).poll(anyLong());

    sharedConsumer.poll(1000);
    verify(consumer, times(1)).poll(1000);

    when(consumer.getAssignment()).thenReturn(Collections.emptySet()); // after unsubscription to
    sharedConsumer.unSubscribe(nonExistentPubSubTopicPartition);

    sharedConsumer.poll(1000);
    verify(consumer, times(1)).poll(1000);
  }
}
