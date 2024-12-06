package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.List;
import java.util.Map;
import java.util.Set;


public abstract class AbstractKafkaConsumerService extends AbstractVeniceService {
  public abstract SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition);

  public abstract SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition topicPartition);

  public abstract void unsubscribeAll(PubSubTopic versionTopic);

  public void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    unSubscribe(versionTopic, pubSubTopicPartition, SharedKafkaConsumer.DEFAULT_MAX_WAIT_MS);
  }

  public abstract void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition, long timeoutMs);

  public abstract void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub);

  public abstract boolean hasAnySubscriptionFor(PubSubTopic versionTopic);

  public abstract long getMaxElapsedTimeMSSinceLastPollInConsumerPool();

  public abstract void startConsumptionIntoDataReceiver(
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      long lastReadOffset,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver);

  public abstract long getOffsetLagBasedOnMetrics(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition);

  public abstract long getLatestOffsetBasedOnMetrics(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition);

  public abstract Map<PubSubTopicPartition, TopicPartitionIngestionInfo> getIngestionInfoFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition);

}
