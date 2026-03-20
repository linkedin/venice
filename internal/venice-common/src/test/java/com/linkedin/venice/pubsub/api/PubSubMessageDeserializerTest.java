package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

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
