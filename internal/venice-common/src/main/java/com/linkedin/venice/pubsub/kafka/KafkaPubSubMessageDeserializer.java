package com.linkedin.venice.pubsub.kafka;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.pools.ObjectPool;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaPubSubMessageDeserializer implements
    PubSubMessageDeserializer<KafkaKey, KafkaMessageEnvelope, Long, ConsumerRecord<byte[], byte[]>, PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> {
  private static final Logger LOGGER = LogManager.getLogger(KafkaPubSubMessageDeserializer.class);

  public static final String VENICE_TRANSPORT_PROTOCOL_HEADER = "vtp";

  private final KafkaKeySerializer keySerializer = new KafkaKeySerializer();
  private final KafkaValueSerializer valueSerializer;
  private final ObjectPool<KafkaMessageEnvelope> putEnvelopePool;
  private final ObjectPool<KafkaMessageEnvelope> updateEnvelopePool;

  private final PubSubTopicRepository pubSubTopicRepository;

  public KafkaPubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool,
      PubSubTopicRepository pubSubTopicRepository) {
    this.valueSerializer = valueSerializer;
    this.putEnvelopePool = putEnvelopePool;
    this.updateEnvelopePool = updateEnvelopePool;
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  @Override
  public PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deserialize(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      PubSubTopicPartition topicPartition) {
    // TODO: Put the key in an object pool as well
    KafkaKey key = keySerializer.deserialize(null, consumerRecord.key());
    KafkaMessageEnvelope value = null;
    if (key.isControlMessage()) {
      for (Header header: consumerRecord.headers()) {
        if (header.key().equals(VENICE_TRANSPORT_PROTOCOL_HEADER)) {
          try {
            Schema providedProtocolSchema = AvroCompatibilityHelper.parse(new String(header.value()));
            value = valueSerializer
                .deserialize(consumerRecord.value(), providedProtocolSchema, getEnvelope(key.getKeyHeaderByte()));
          } catch (Exception e) {
            // Improper header... will ignore.
            LOGGER.warn("Received unparsable schema in protocol header: " + VENICE_TRANSPORT_PROTOCOL_HEADER, e);
          }
          break; // We don't look at other headers
        }
      }
    }
    if (value == null) {
      value = valueSerializer.deserialize(consumerRecord.value(), getEnvelope(key.getKeyHeaderByte()));
    }
    // TODO: Put the message container in an object pool as well
    return new ImmutablePubSubMessage<>(
        key,
        value,
        topicPartition,
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        consumerRecord.key().length + consumerRecord.value().length);
  }

  private KafkaMessageEnvelope getEnvelope(byte keyHeaderByte) {
    switch (keyHeaderByte) {
      case MessageType.Constants.PUT_KEY_HEADER_BYTE:
        return putEnvelopePool.get();
      // No need to pool control messages since there are so few of them, and they are varied anyway, limiting reuse.
      case MessageType.Constants.CONTROL_MESSAGE_KEY_HEADER_BYTE:
        return new KafkaMessageEnvelope();
      case MessageType.Constants.UPDATE_KEY_HEADER_BYTE:
        return updateEnvelopePool.get();
      default:
        throw new IllegalStateException("Illegal key header byte: " + keyHeaderByte);
    }
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }
}
