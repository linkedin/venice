package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A Kafka specific callback which wraps generic {@link PubSubProducerCallback}
 */
public class ApacheKafkaProducerCallback implements Callback {
  private final PubSubProducerCallback pubsubProducerCallback;

  public ApacheKafkaProducerCallback(PubSubProducerCallback pubsubProducerCallback) {
    this.pubsubProducerCallback = pubsubProducerCallback;
  }

  /**
   *
   * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
   *                 with -1 value for all fields except for topicPartition will be returned if an error occurred.
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
    if (pubsubProducerCallback == null) {
      return;
    }
    PubSubProduceResult produceResult = new ApacheKafkaProduceResult(metadata);
    if (exception != null) {
      pubsubProducerCallback.completeExceptionally(exception);
    } else {
      pubsubProducerCallback.complete(produceResult);
    }
    pubsubProducerCallback.onCompletion(new ApacheKafkaProduceResult(metadata), exception);
  }

  Future<PubSubProduceResult> getProduceResultFuture() {
    return pubsubProducerCallback;
  }
}
