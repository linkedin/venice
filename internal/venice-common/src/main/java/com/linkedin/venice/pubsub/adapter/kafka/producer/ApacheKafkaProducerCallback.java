package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A Kafka specific callback which wraps generic {@link PubSubProducerCallback}
 */
public class ApacheKafkaProducerCallback implements Callback {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerCallback.class);

  private final PubSubProducerCallback pubsubProducerCallback;
  private final CompletableFuture<PubSubProduceResult> produceResultFuture = new CompletableFuture<>();
  private final ApacheKafkaProducerAdapter producerAdapter;

  public ApacheKafkaProducerCallback(
      PubSubProducerCallback pubsubProducerCallback,
      ApacheKafkaProducerAdapter producerAdapter) {
    this.pubsubProducerCallback = pubsubProducerCallback;
    this.producerAdapter = producerAdapter;
  }

  /**
   *
   * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
   *                 NULL if an error occurred.
   * @param exception The exception thrown during processing of this record. Null if no error occurred.
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
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    PubSubProduceResult produceResult = null;
    if (exception != null) {
      produceResultFuture.completeExceptionally(exception);
    } else {
      produceResult = new ApacheKafkaProduceResult(metadata);
      produceResultFuture.complete(produceResult);
    }

    // This is a special case where the producer is closed forcefully. We can skip the callback processing
    if (exception != null && exception.getMessage() != null && producerAdapter.isForceClose()
        && exception.getMessage().contains("Producer is closed forcefully")) {
      LOGGER.debug("Producer is closed forcefully. Skipping the callback processing.");
      return;
    }

    if (pubsubProducerCallback != null) {
      pubsubProducerCallback.onCompletion(produceResult, exception);
    }
  }

  Future<PubSubProduceResult> getProduceResultFuture() {
    return produceResultFuture;
  }
}
