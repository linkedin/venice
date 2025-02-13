package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * Converts RecordMetadata to {@link PubSubProduceResult}
 */
public class ApacheKafkaProduceResult extends SimplePubSubProduceResultImpl {
  public ApacheKafkaProduceResult(RecordMetadata recordMetadata) {
    super(
        recordMetadata.topic(),
        recordMetadata.partition(),
        recordMetadata.offset(),
        new ApacheKafkaOffsetPosition(recordMetadata.offset()),
        recordMetadata.serializedKeySize() + recordMetadata.serializedValueSize());
  }
}
