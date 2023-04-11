package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.pools.ObjectPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Converts a Kafka {@link ConsumerRecord} to {@link PubSubMessage}.
 */
public class KafkaPubSubMessageDeserializer extends PubSubMessageDeserializer<Long> {
  private static final Logger LOGGER = LogManager.getLogger(KafkaPubSubMessageDeserializer.class);

  public KafkaPubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool) {
    super(valueSerializer, putEnvelopePool, updateEnvelopePool);
  }

  public PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deserialize(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      PubSubTopicPartition topicPartition) {
    PubSubMessageHeaders pubSubMessageHeaders = new PubSubMessageHeaders();
    for (Header header: consumerRecord.headers()) {
      pubSubMessageHeaders.add(header.key(), header.value());
    }
    long position = consumerRecord.offset();
    return deserialize(
        topicPartition,
        consumerRecord.key(),
        consumerRecord.value(),
        pubSubMessageHeaders,
        position,
        consumerRecord.timestamp());
  }
}
