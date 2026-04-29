package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.nio.ByteBuffer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PubSubMessageDeserializer}
 */
public class PubSubMessageDeserializerTest {
  private PubSubTopicRepository topicRepository;
  private PubSubMessageDeserializer messageDeserializer;
  private PubSubTopicPartition topicPartition;
  private KafkaKeySerializer keySerializer;
  private KafkaValueSerializer valueSerializer;
  private PubSubPosition position;

  @BeforeMethod
  public void setUp() {
    position = mock(PubSubPosition.class);
    topicRepository = new PubSubTopicRepository();
    messageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    topicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic("test"), 42);
    keySerializer = new KafkaKeySerializer();
    valueSerializer = new OptimizedKafkaValueSerializer();
  }

  @AfterMethod
  public void cleanUp() {
    messageDeserializer.close();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Illegal key header byte.*")
  public void testDeserializerFailsWhenKeyValueFormatIsInvalid() {
    messageDeserializer
        .deserialize(topicPartition, "key".getBytes(), "value".getBytes(), new PubSubMessageHeaders(), position, 12L);
  }

  @Test(expectedExceptions = VeniceMessageException.class, expectedExceptionsMessageRegExp = ".*Received Magic Byte 'v' which is not supported by OptimizedKafkaValueSerializer.*")
  public void testDeserializerFailsWhenValueFormatIsInvalid() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        "value".getBytes(),
        new PubSubMessageHeaders(),
        position,
        12L);
  }

  @Test
  public void testDeserializer() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        12L);
    // verify
    KafkaKey actualKey = message.getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(message.getValue(), value);
    assertEquals(message.getPosition(), position);
  }

  @Test
  public void testDeserializerValueWithSchemaFromPubSubMessageHeaders() {
    KafkaKey key = new KafkaKey(MessageType.CONTROL_MESSAGE, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    PubSubMessageHeader header =
        new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, KafkaMessageEnvelope.SCHEMA$.toString().getBytes());
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders().add(header),
        position,
        12L);

    // verify
    KafkaKey actualKey = message.getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(message.getValue(), value);
    assertEquals(message.getPosition(), position);
    /*
     * The protocol-schema header must not be retained in the resulting message: the schema is
     * ~16 KB of redundant Avro JSON and queueing it per-record blows up heap during back-pressure.
     */
    assertNull(
        message.getPubSubMessageHeaders().get(VENICE_TRANSPORT_PROTOCOL_HEADER),
        "vtp header should be stripped after the protocol schema is consumed");
  }

  @Test
  public void testDeserializerStripsVtpHeaderFromPutMessage() {
    /*
     * Producer attaches the vtp header to the first message of a segment, including PUTs.
     * The consumer never uses vtp on non-control messages, so it must be stripped to avoid
     * pinning ~16 KB of schema text per queued record.
     */
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    PubSubMessageHeader vtp =
        new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, KafkaMessageEnvelope.SCHEMA$.toString().getBytes());
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders().add(vtp),
        position,
        12L);

    assertNull(
        message.getPubSubMessageHeaders().get(VENICE_TRANSPORT_PROTOCOL_HEADER),
        "vtp header should be stripped on non-control messages too");
    assertEquals(message.getValue(), value);
  }

  @Test
  public void testDeserializerWithEmptyPubSubMessageHeadersSingleton() {
    /*
     * EmptyPubSubMessageHeaders.SINGLETON is intentionally immutable: its remove()
     * throws UnsupportedOperationException. The vtp strip must be a no-op when the
     * header isn't present, otherwise it breaks callers passing the singleton.
     */
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        EmptyPubSubMessageHeaders.SINGLETON,
        position,
        12L);

    assertEquals(message.getValue(), value);
    assertEquals(message.getPubSubMessageHeaders(), EmptyPubSubMessageHeaders.SINGLETON);
  }

  @Test
  public void testDeserializerHandlesImmutableNonEmptyHeaders() {
    /*
     * Some callers may wrap headers in an immutable variant whose remove() throws even when
     * the underlying map is non-empty. The strip must work on those without throwing.
     */
    KafkaKey key = new KafkaKey(MessageType.CONTROL_MESSAGE, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    PubSubMessageHeader vtp =
        new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, KafkaMessageEnvelope.SCHEMA$.toString().getBytes());
    PubSubMessageHeader lcs = new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });

    PubSubMessageHeaders immutableNonEmpty = new PubSubMessageHeaders() {
      private final PubSubMessageHeaders delegate = new PubSubMessageHeaders().add(vtp).add(lcs);

      @Override
      public PubSubMessageHeader get(String k) {
        return delegate.get(k);
      }

      @Override
      public java.util.Iterator<PubSubMessageHeader> iterator() {
        return delegate.iterator();
      }

      @Override
      public PubSubMessageHeaders remove(String k) {
        throw new UnsupportedOperationException("immutable");
      }

      @Override
      public PubSubMessageHeaders add(PubSubMessageHeader h) {
        throw new UnsupportedOperationException("immutable");
      }
    };

    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        immutableNonEmpty,
        position,
        12L);

    assertNull(
        message.getPubSubMessageHeaders().get(VENICE_TRANSPORT_PROTOCOL_HEADER),
        "vtp must be stripped without mutating the immutable input");
    assertEquals(
        message.getPubSubMessageHeaders().get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(),
        new byte[] { 1 },
        "non-vtp headers must round-trip via the rebuilt headers");
  }

  @Test
  public void testDeserializerStripsVtpHeaderButPreservesOtherHeaders() {
    /*
     * Stripping vtp must not touch other headers - lcs (leader-complete state), vpm
     * (view partitions map), or any unknown future header keys must survive deserialization
     * intact for downstream consumers.
     */
    KafkaKey key = new KafkaKey(MessageType.CONTROL_MESSAGE, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    PubSubMessageHeader vtp =
        new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, KafkaMessageEnvelope.SCHEMA$.toString().getBytes());
    PubSubMessageHeader lcs = new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { 1 });
    PubSubMessageHeader custom = new PubSubMessageHeader("some-future-header", "payload".getBytes());

    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders().add(vtp).add(lcs).add(custom),
        position,
        12L);

    PubSubMessageHeaders out = message.getPubSubMessageHeaders();
    assertNull(out.get(VENICE_TRANSPORT_PROTOCOL_HEADER), "vtp must be stripped");
    assertEquals(out.get(VENICE_LEADER_COMPLETION_STATE_HEADER).value(), new byte[] { 1 }, "lcs must be preserved");
    assertEquals(out.get("some-future-header").value(), "payload".getBytes(), "unknown headers must be preserved");
  }

  @Test
  public void testDeserializerValueWithInvalidSchemaFromPubSubMessageHeaders() {
    KafkaKey key = new KafkaKey(MessageType.CONTROL_MESSAGE, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    PubSubMessageHeader header = new PubSubMessageHeader(VENICE_TRANSPORT_PROTOCOL_HEADER, "invalid".getBytes());
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders().add(header),
        position,
        12L);

    // verify
    KafkaKey actualKey = message.getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(message.getValue(), value);
    assertEquals(message.getPosition(), position);
  }

  @Test
  public void testDeserializerFallsBackToProducerTimestampWhenBrokerTimestampIsZero() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    long producerTimestamp = 1700000000000L;
    value.producerMetadata.messageTimestamp = producerTimestamp;

    // When broker timestamp is 0, should fall back to producer timestamp
    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        0L);
    assertEquals(message.getPubSubMessageTime(), producerTimestamp);

    // When broker timestamp is valid, should use broker timestamp
    long brokerTimestamp = 1700000001000L;
    message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        brokerTimestamp);
    assertEquals(message.getPubSubMessageTime(), brokerTimestamp);
  }

  @Test
  public void testDeserializerDoesNotFallBackWhenFallbackDisabled() {
    PubSubMessageDeserializer disabledFallbackDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        false); // fallback disabled

    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    value.producerMetadata.messageTimestamp = 1700000000000L;

    // When broker timestamp is 0 and fallback is disabled, 0 should be returned as-is
    DefaultPubSubMessage message = disabledFallbackDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        0L);
    assertEquals(message.getPubSubMessageTime(), 0L);

    // When broker timestamp is null and fallback is disabled, 0 should be returned
    message = disabledFallbackDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        null);
    assertEquals(message.getPubSubMessageTime(), 0L);
    disabledFallbackDeserializer.close();
  }

  @Test
  public void testDeserializerFallsBackToProducerTimestampWhenBrokerTimestampIsNull() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = getDummyValue();
    long producerTimestamp = 1700000000000L;
    value.producerMetadata.messageTimestamp = producerTimestamp;

    DefaultPubSubMessage message = messageDeserializer.deserialize(
        topicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        position,
        null);
    assertEquals(message.getPubSubMessageTime(), producerTimestamp);
  }

  private KafkaMessageEnvelope getDummyValue() {
    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.producerMetadata = new ProducerMetadata();
    value.producerMetadata.messageTimestamp = 0;
    value.producerMetadata.messageSequenceNumber = 0;
    value.producerMetadata.segmentNumber = 0;
    value.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(1024);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    value.payloadUnion = put;
    return value;
  }
}
