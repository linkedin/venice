package com.linkedin.venice.pubsub.api;

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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link PubSubMessageDeserializer}
 */
public class PubSubMessageDeserializerTest {
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicPartition pubSubTopicPartition;

  @BeforeMethod
  public void setUp() {
    pubSubTopicRepository = new PubSubTopicRepository();
    pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 42);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Illegal key header byte.*")
  public void testDeserializerFailsWhenKeyValueFormatIsInvalid() {
    pubSubMessageDeserializer
        .deserialize(pubSubTopicPartition, "key".getBytes(), "value".getBytes(), new PubSubMessageHeaders(), 11L, 12L);
  }

  @Test(expectedExceptions = VeniceMessageException.class, expectedExceptionsMessageRegExp = ".*Received Magic Byte 'v' which is not supported by OptimizedKafkaValueSerializer.*")
  public void testDeserializerFailsWhenValueFormatIsInvalid() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();
    pubSubMessageDeserializer.deserialize(
        pubSubTopicPartition,
        keySerializer.serialize("test", key),
        "value".getBytes(),
        new PubSubMessageHeaders(),
        11L,
        12L);
  }

  @Test
  public void testDeserializer() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();
    KafkaMessageEnvelope value = getDummyValue();

    KafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message = pubSubMessageDeserializer.deserialize(
        pubSubTopicPartition,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value),
        new PubSubMessageHeaders(),
        11L,
        12L);

    // verify
    KafkaKey actualKey = message.getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(message.getValue(), value);
    assertEquals((long) message.getOffset(), 11);
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
