package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * This delegator impl is used to distribute different partition requests into different consumer service.
 * When {@text ConfigKeys#SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED} is off, this class
 * will always return the default consumer service.
 * When the option is on, it will return the dedicated consumer service when the topic partition belongs
 * to a Real-time topic and the corresponding store has active/active or write compute enabled.
 * The reason to use dedicated consumer pool for leader replicas of active/active or write compute stores is
 * that handling the writes before putting into the drainer queue is too expensive comparing to others.
 */
public class KafkaConsumerServiceDelegator extends AbstractKafkaConsumerService {
  private final KafkaConsumerService defaultConsumerService;
  private final KafkaConsumerService consumerServiceForAAWCLeader;
  private final Function<String, Boolean> isAAWCStoreFunc;

  public KafkaConsumerServiceDelegator(
      VeniceServerConfig serverConfig,
      BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor,
      Function<String, Boolean> isAAWCStoreFunc) {

    this.defaultConsumerService =
        consumerServiceConstructor.apply(serverConfig.getConsumerPoolSizePerKafkaCluster(), ""); // Empty stats suffix
    if (serverConfig.isDedicatedConsumerPoolForAAWCLeaderEnabled()) {
      this.consumerServiceForAAWCLeader = consumerServiceConstructor
          .apply(serverConfig.getDedicatedConsumerPoolSizeForAAWCLeader(), "_for_aa_wc_leader");
    } else {
      this.consumerServiceForAAWCLeader = null;
    }
    this.isAAWCStoreFunc = isAAWCStoreFunc;
  }

  private KafkaConsumerService getKafkaConsumerService(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    if (this.consumerServiceForAAWCLeader != null && isAAWCStoreFunc.apply(versionTopic.getName())
        && topicPartition.getPubSubTopic().isRealTime()) {
      /**
       * For AAWC leader replica, this function will return the dedicated consumer pool.
       */
      return consumerServiceForAAWCLeader;
    }
    return defaultConsumerService;
  }

  @Override
  public SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    return getKafkaConsumerService(versionTopic, topicPartition)
        .getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
  }

  @Override
  public SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    return getKafkaConsumerService(versionTopic, topicPartition).assignConsumerFor(versionTopic, topicPartition);
  }

  @Override
  public void unsubscribeAll(PubSubTopic versionTopic) {
    defaultConsumerService.unsubscribeAll(versionTopic);
    if (consumerServiceForAAWCLeader != null) {
      consumerServiceForAAWCLeader.unsubscribeAll(versionTopic);
    }
  }

  @Override
  public void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    getKafkaConsumerService(versionTopic, pubSubTopicPartition).unSubscribe(versionTopic, pubSubTopicPartition);
  }

  @Override
  public void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub) {
    defaultConsumerService.batchUnsubscribe(versionTopic, topicPartitionsToUnSub);
    if (consumerServiceForAAWCLeader != null) {
      consumerServiceForAAWCLeader.batchUnsubscribe(versionTopic, topicPartitionsToUnSub);
    }
  }

  @Override
  public boolean hasAnySubscriptionFor(PubSubTopic versionTopic) {
    return defaultConsumerService.hasAnySubscriptionFor(versionTopic)
        || consumerServiceForAAWCLeader != null && consumerServiceForAAWCLeader.hasAnySubscriptionFor(versionTopic);
  }

  @Override
  public long getMaxElapsedTimeMSSinceLastPollInConsumerPool() {
    return Math.max(
        defaultConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool(),
        consumerServiceForAAWCLeader == null
            ? 0
            : consumerServiceForAAWCLeader.getMaxElapsedTimeMSSinceLastPollInConsumerPool());
  }

  @Override
  public void startConsumptionIntoDataReceiver(
      PubSubTopicPartition topicPartition,
      long lastReadOffset,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver) {
    PubSubTopic versionTopic = consumedDataReceiver.destinationIdentifier();
    getKafkaConsumerService(versionTopic, topicPartition)
        .startConsumptionIntoDataReceiver(topicPartition, lastReadOffset, consumedDataReceiver);
  }

  @Override
  public long getOffsetLagFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    return getKafkaConsumerService(versionTopic, pubSubTopicPartition)
        .getOffsetLagFor(versionTopic, pubSubTopicPartition);
  }

  @Override
  public long getLatestOffsetFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    return getKafkaConsumerService(versionTopic, pubSubTopicPartition)
        .getLatestOffsetFor(versionTopic, pubSubTopicPartition);
  }

  @Override
  public boolean startInner() throws Exception {
    defaultConsumerService.start();
    if (consumerServiceForAAWCLeader != null) {
      consumerServiceForAAWCLeader.start();
    }
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    defaultConsumerService.stop();
    if (consumerServiceForAAWCLeader != null) {
      consumerServiceForAAWCLeader.stop();
    }
  }
}
