package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.utils.pools.ObjectPool;
import java.util.function.Supplier;
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
  private final boolean producerTimestampFallbackEnabled;

  public PubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool) {
    this(valueSerializer, putEnvelopePool, updateEnvelopePool, true);
  }

  public PubSubMessageDeserializer(
      KafkaValueSerializer valueSerializer,
      ObjectPool<KafkaMessageEnvelope> putEnvelopePool,
      ObjectPool<KafkaMessageEnvelope> updateEnvelopePool,
      boolean producerTimestampFallbackEnabled) {
    this.valueSerializer = valueSerializer;
    this.putEnvelopePool = putEnvelopePool;
    this.updateEnvelopePool = updateEnvelopePool;
    this.producerTimestampFallbackEnabled = producerTimestampFallbackEnabled;
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
            Supplier<Schema> providedProtocolSchema = () -> AvroCompatibilityHelper.parse(new String(header.value()));
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
    /*
     * Strip the protocol-schema header before constructing ImmutablePubSubMessage. Its sole
     * purpose is forward-compat schema bootstrap during deserialization (above), and once we
     * have the value envelope it is dead weight in heap.
     *
     * The header value is the entire Avro schema for KafkaMessageEnvelope (~16 KB JSON text).
     * VeniceWriter#getHeaders attaches it only when both (a) producerMetadata.segmentNumber
     * == 0 && messageSequenceNumber == 0 and (b) protocolSchemaHeader != null. Even so, in
     * NR pass-through paths and any other code that copies headers from upstream messages,
     * 'vtp' can ride along on a substantial fraction of records. With back-pressure on the
     * StoreBufferService drainer (e.g. during a gzip-Inflater GCLocker stall) a queue with
     * hundreds of thousands of records can pin its own ~16 KB byte[] copy of identical
     * schema text per record - upwards of 10 GB of redundant retention has been observed
     * in production heap dumps. Removing the header here keeps the value (already
     * deserialized into the KafkaMessageEnvelope above) without the byte[] tail.
     *
     * When the header is present, build a new PubSubMessageHeaders that excludes it rather
     * than mutating the caller's instance. This avoids two failure modes:
     *   1) EmptyPubSubMessageHeaders.SINGLETON throws on remove() by design.
     *   2) Any caller-supplied immutable wrapper would also throw on remove().
     * It also keeps the deserialize() contract free of an undocumented input-mutation
     * side effect.
     *
     * Common case ('vtp' absent) is a single map lookup with no allocation. Allocating
     * a small replacement headers object only on the rare path is well within the budget
     * we're trying to recover.
     */
    if (headers.get(VENICE_TRANSPORT_PROTOCOL_HEADER) != null) {
      PubSubMessageHeaders filtered = new PubSubMessageHeaders();
      for (PubSubMessageHeader h: headers) {
        if (!VENICE_TRANSPORT_PROTOCOL_HEADER.equals(h.key())) {
          filtered.add(h);
        }
      }
      headers = filtered;
    }
    // When enabled, prefer Venice's own producer timestamp when the pub-sub system timestamp is
    // missing or zero. Some pub-sub systems do not provide reliable per-message timestamps.
    // Venice always embeds a producer timestamp in the KafkaMessageEnvelope, so we can fall back
    // to it. Controlled by PUBSUB_PRODUCER_TIMESTAMP_FALLBACK_ENABLED (default: true).
    long effectiveTimestamp = (producerTimestampFallbackEnabled && (timestamp == null || timestamp <= 0))
        ? value.producerMetadata.messageTimestamp
        : (timestamp != null ? timestamp : 0L);
    // TODO: Put the message container in an object pool as well
    return new ImmutablePubSubMessage(
        key,
        value,
        topicPartition,
        pubSubPosition,
        effectiveTimestamp,
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

  /**
   * Do not use the following default deserializer in production code as it does not support schema evolution
   * properly. It is only provided for convenience in test code.
   */
  public static PubSubMessageDeserializer createDefaultDeserializer() {
    return new PubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
  }

  /**
   * Do not use the following default deserializer in production code as it does not support schema evolution
   * properly. It is only provided for convenience in test code.
   */
  public static PubSubMessageDeserializer createOptimizedDeserializer() {
    return new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
  }
}
