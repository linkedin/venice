package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.List;
import java.util.Set;


public abstract class AbstractKafkaConsumerService extends AbstractVeniceService {
  public abstract SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition);

  public abstract SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition topicPartition);

  public abstract void unsubscribeAll(PubSubTopic versionTopic);

  public abstract void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition);

  public abstract void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub);

  public abstract boolean hasAnySubscriptionFor(PubSubTopic versionTopic);

  public abstract long getMaxElapsedTimeMSSinceLastPollInConsumerPool();

  public abstract void startConsumptionIntoDataReceiver(
      PubSubTopicPartition topicPartition,
      long lastReadOffset,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver);

  public abstract long getOffsetLagFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition);

  public abstract long getLatestOffsetFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition);
}
