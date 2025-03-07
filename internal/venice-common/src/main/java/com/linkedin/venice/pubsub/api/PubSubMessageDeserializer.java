package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.utils.pools.ObjectPool;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The class for deserializing messages from the pubsub specific message format to {@link PubSubMessage}
 */
public class PubSubMessageDeserializer {
  private static final Logger LOGGER = LogManager.getLogger(PubSubMessageDeserializer.class);
  private final KafkaKeySerializer keySerializer = new KafkaKeySerializer();
  private final KafkaValueSerializer valueSerializer;
  private final ObjectPool<KafkaMessageEnvelope> putEnvelopePool;
  private final ObjectPool<KafkaMessageEnvelope> updateEnvelopePool;

  public PubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool) {
    this.valueSerializer = valueSerializer;
    this.putEnvelopePool = putEnvelopePool;
    this.updateEnvelopePool = updateEnvelopePool;
  }

  /**
   * Deserialize a message from the pubsub specific message format to PubSubMessage.
   *
   * @param topicPartition the topic partition from which the message was read
   * @param keyBytes the key bytes of the message
   * @param valueBytes the value bytes of the message
   * @param headers the headers of the message
   * @param pubSubPosition the position of the message in the topic partition
   * @param timestamp the timestamp of the message
   * @return the deserialized PubSubMessage
   */
  public DefaultPubSubMessage deserialize(
      PubSubTopicPartition topicPartition,
      byte[] keyBytes,
      byte[] valueBytes,
      PubSubMessageHeaders headers,
      PubSubPosition pubSubPosition,
      Long timestamp) {
    // TODO: Put the key in an object pool as well
    KafkaKey key = keySerializer.deserialize(null, keyBytes);
    KafkaMessageEnvelope value = null;
    if (key.isControlMessage()) {
      for (PubSubMessageHeader header: headers) {
        // only process VENICE_TRANSPORT_PROTOCOL_HEADER here. Other headers will be stored in
        // ImmutablePubSubMessage and used down the ingestion path later
        if (header.key().equals(VENICE_TRANSPORT_PROTOCOL_HEADER)) {
          try {
            Schema providedProtocolSchema = AvroCompatibilityHelper.parse(new String(header.value()));
            value =
                valueSerializer.deserialize(valueBytes, providedProtocolSchema, getEnvelope(key.getKeyHeaderByte()));
          } catch (Exception e) {
            // Improper header... will ignore.
            LOGGER.warn(
                "Received unparsable schema or encountered schema registration issue in protocol header: "
                    + VENICE_TRANSPORT_PROTOCOL_HEADER,
                e);
          }
          break; // We don't look at other headers
        }
      }
    }
    if (value == null) {
      value = valueSerializer.deserialize(valueBytes, getEnvelope(key.getKeyHeaderByte()));
    }
    // TODO: Put the message container in an object pool as well
    return new ImmutablePubSubMessage(
        key,
        value,
        topicPartition,
        pubSubPosition,
        timestamp,
        keyBytes.length + valueBytes.length,
        headers);
  }

  private KafkaMessageEnvelope getEnvelope(byte keyHeaderByte) {
    switch (keyHeaderByte) {
      case MessageType.Constants.PUT_KEY_HEADER_BYTE:
        return putEnvelopePool.get();
      // No need to pool control messages since there are so few of them, and they are varied anyway, limiting reuse.
      case MessageType.Constants.CONTROL_MESSAGE_KEY_HEADER_BYTE:
      case MessageType.Constants.GLOBAL_RT_DIV_KEY_HEADER_BYTE:
        return new KafkaMessageEnvelope();
      case MessageType.Constants.UPDATE_KEY_HEADER_BYTE:
        return updateEnvelopePool.get();
      default:
        throw new IllegalStateException("Illegal key header byte: " + keyHeaderByte);
    }
  }

  public void close() {
    if (valueSerializer != null) {
      valueSerializer.close();
    }
  }

  // For testing only.
  public KafkaValueSerializer getValueSerializer() {
    return valueSerializer;
  }

  public static PubSubMessageDeserializer getInstance() {
    return new PubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
  }
}
