package com.linkedin.venice.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.linkedin.venice.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.MockInMemoryAdminAdapter;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;


public class TopicMangerUnitTest extends TopicManagerTestBase {
  private InMemoryKafkaBroker inMemoryKafkaBroker;

  @Override
  protected void createTopicManager() {
    inMemoryKafkaBroker = new InMemoryKafkaBroker("local");
    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = mock(ApacheKafkaAdminAdapterFactory.class);
    MockInMemoryAdminAdapter mockInMemoryAdminAdapter = new MockInMemoryAdminAdapter(inMemoryKafkaBroker);
    doReturn(mockInMemoryAdminAdapter).when(pubSubAdminAdapterFactory).create(any(), eq(pubSubTopicRepository));
    MockInMemoryConsumer mockInMemoryConsumer =
        new MockInMemoryConsumer(inMemoryKafkaBroker, new RandomPollStrategy(), mock(PubSubConsumer.class));
    mockInMemoryConsumer.setMockInMemoryAdminAdapter(mockInMemoryAdminAdapter);
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory = mock(PubSubConsumerAdapterFactory.class);
    doReturn(mockInMemoryConsumer).when(pubSubConsumerAdapterFactory).create(any(), anyBoolean(), any(), anyString());

    topicManager = TopicManagerRepository.builder()
        .setPubSubProperties(k -> new VeniceProperties())
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setLocalKafkaBootstrapServers("localhost:1234")
        .setPubSubConsumerAdapterFactory(pubSubConsumerAdapterFactory)
        .setPubSubAdminAdapterFactory(pubSubAdminAdapterFactory)
        .setKafkaOperationTimeoutMs(500L)
        .setTopicDeletionStatusPollIntervalMs(100L)
        .setTopicMinLogCompactionLagMs(MIN_COMPACTION_LAG)
        .build()
        .getTopicManager();
  }

  @Override
  protected PubSubProducerAdapter createPubSubProducerAdapter() {
    return new MockInMemoryProducerAdapter(inMemoryKafkaBroker);
  }
}
