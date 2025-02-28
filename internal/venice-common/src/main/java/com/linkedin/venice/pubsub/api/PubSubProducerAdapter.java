package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.UnderDevelopment;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/**
 * The pub-sub producer interface with which venice writer's interact to send messages to pub-sub topic.
 *
 * An implementation of this interface are required to provide the following guarantees:
 *  1. At-least once delivery (ALOD): messages should not be dropped.
 *  2. In order delivery (IOD): messages in the same partition should follow the order in which they were sent.
 */
public interface PubSubProducerAdapter {
  /**
   * The support for the following two getNumberOfPartitions APIs will be removed.
   */
  @Deprecated
  int getNumberOfPartitions(String topic);

  /**
   * Sends a message to a PubSub topic asynchronously and returns a {@link Future} representing the result of the produce operation.
   *
   * @param topic The name of the Kafka topic to which the message will be sent.
   * @param partition The partition to which the message should be sent.
   * @param key The key associated with the message, used for partitioning and message retrieval.
   * @param value The message payload to be sent to the PubSubTopic topic.
   * @param pubSubMessageHeaders Additional headers to be included with the message.
   * @param pubSubProducerCallback An optional callback to handle the result of the produce operation.
   * @return A {@link Future} representing the result of the produce operation.
   * @throws PubSubOpTimeoutException If the produce operation times out.
   * @throws PubSubTopicAuthorizationException If there's an authorization error while producing the message.
   * @throws PubSubTopicDoesNotExistException If the target topic does not exist.
   * @throws PubSubClientRetriableException If a retriable error occurs while producing the message.
   * @throws PubSubClientException If an error occurs while producing the message.
   */
  CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders pubSubMessageHeaders,
      PubSubProducerCallback pubSubProducerCallback);

  @UnderDevelopment
  default CompletableFuture<PubSubProduceResult> sendMessage(
      PubSubTopicPartition pubSubTopicPartition,
      byte[] keyBytes,
      byte[] valueBytes,
      PubSubMessageHeaders pubSubMessageHeaders,
      PubSubProducerCallback pubSubProducerCallback) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  void flush();

  void close(long closeTimeOutMs);

  Object2DoubleMap<String> getMeasurableProducerMetrics();

  String getBrokerAddress();
}
