package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A Kafka specific callback which wraps generic {@link PubSubProducerCallback}
 */
public class ApacheKafkaProducerCallback implements Callback {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerCallback.class);

  private final PubSubProducerCallback pubsubProducerCallback;
  private final CompletableFuture<PubSubProduceResult> produceResultFuture = new CompletableFuture<>();

  public ApacheKafkaProducerCallback(PubSubProducerCallback pubsubProducerCallback) {
    this.pubsubProducerCallback = pubsubProducerCallback;
  }

  /**
   *
   * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
   *                 NULL if an error occurred.
   * @param kafkaException The exception thrown during processing of this record. Null if no error occurred.
   *                  Possible thrown exceptions include:
   *
   *                  Non-Retriable exceptions (fatal, the message will never be sent):
   *
   *                  InvalidTopicException
   *                  OffsetMetadataTooLargeException
   *                  RecordBatchTooLargeException
   *                  RecordTooLargeException
   *                  UnknownServerException
   *
   *                  Retriable exceptions (transient, may be covered by increasing #.retries):
   *
   *                  CorruptRecordException
   *                  InvalidMetadataException
   *                  NotEnoughReplicasAfterAppendException
   *                  NotEnoughReplicasException
   *                  OffsetOutOfRangeException
   *                  TimeoutException
   *                  UnknownTopicOrPartitionException
   */
  @Override
  public void onCompletion(RecordMetadata metadata, Exception kafkaException) {
    PubSubProduceResult produceResult = null;
    Exception pubSubException = getPubSubException(kafkaException);
    if (kafkaException != null) {
      produceResultFuture.completeExceptionally(pubSubException);
    } else {
      produceResult = new ApacheKafkaProduceResult(metadata);
      produceResultFuture.complete(produceResult);
    }

    // This is a special case where the producer is closed forcefully. We can skip the callback processing
    if (kafkaException != null && kafkaException.getMessage() != null
        && kafkaException.getMessage().contains("Producer is closed forcefully")) {
      LOGGER.debug("Producer is closed forcefully. Skipping the callback processing.");
      return;
    }

    if (pubsubProducerCallback != null) {
      pubsubProducerCallback.onCompletion(produceResult, pubSubException);
    }
  }

  CompletableFuture<PubSubProduceResult> getProduceResultFuture() {
    return produceResultFuture;
  }

  private Exception getPubSubException(Exception exception) {
    if (exception == null) {
      return null;
    }

    if (exception instanceof UnknownTopicOrPartitionException) {
      return new PubSubTopicDoesNotExistException("Topic does not exists", exception);
    }

    if (exception instanceof AuthorizationException || exception instanceof AuthenticationException) {
      return new PubSubTopicAuthorizationException("Does not have permission to publish to topic", exception);
    }

    if (exception instanceof org.apache.kafka.common.errors.TimeoutException) {
      return new PubSubOpTimeoutException("Timeout while publishing to topic", exception);
    }

    if (exception instanceof RetriableException) {
      return new PubSubClientRetriableException("Retriable exception while publishing to topic", exception);
    }

    return new PubSubClientException("Exception while publishing to topic", exception);
  }
}
