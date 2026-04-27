package com.linkedin.venice.pubsub;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class ImmutablePubSubMessageTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final PubSubTopicPartition TOPIC_PARTITION =
      new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test_v1"), 0);

  /**
   * Capacity accounting in StoreBufferService.QueueNode delegates to
   * ImmutablePubSubMessage.getHeapSize. If headers are not counted there, queues
   * can grow well past their byte budget when per-record headers carry payload
   * (e.g. the 'vpm' view-partition-map or any future custom key).
   */
  @Test
  public void testGetHeapSizeIncludesHeaders() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaMessageEnvelope value = dummyEnvelope();

    DefaultPubSubMessage withoutHeaders = new ImmutablePubSubMessage(
        key,
        value,
        TOPIC_PARTITION,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0,
        EmptyPubSubMessageHeaders.SINGLETON);

    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[1024]));
    DefaultPubSubMessage withHeaders =
        new ImmutablePubSubMessage(key, value, TOPIC_PARTITION, ApacheKafkaOffsetPosition.of(0), 0L, 0, headers);

    assertTrue(
        withHeaders.getHeapSize() > withoutHeaders.getHeapSize() + 1024,
        "getHeapSize() must include the headers payload — saw " + withHeaders.getHeapSize() + " vs "
            + withoutHeaders.getHeapSize());
  }

  @Test
  public void testGetHeapSizeWithNullHeaderValueDoesNotThrow() {
    /*
     * Kafka headers may carry a null value, and ApacheKafkaConsumerAdapter passes them
     * through to PubSubMessageHeaders.add(key, null). The size accounting must treat
     * such a header as zero bytes rather than NPE'ing.
     */
    PubSubMessageHeaders headersWithNullValue =
        new PubSubMessageHeaders().add(new PubSubMessageHeader("nullable-key", null));
    DefaultPubSubMessage message = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.PUT, "key".getBytes()),
        dummyEnvelope(),
        TOPIC_PARTITION,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0,
        headersWithNullValue);
    assertTrue(message.getHeapSize() > 0, "Heap size must be positive even when a header value is null");
  }

  @Test
  public void testGetHeapSizeWithNullHeadersDoesNotThrow() {
    /*
     * The 6-arg constructor leaves pubSubMessageHeaders as null. getHeapSize must
     * tolerate that path (used heavily by tests and the writer code).
     */
    DefaultPubSubMessage message = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.PUT, "key".getBytes()),
        dummyEnvelope(),
        TOPIC_PARTITION,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0);
    assertNotNull(message);
    assertTrue(message.getHeapSize() > 0, "Heap size must be positive even when headers are null");
  }

  private static KafkaMessageEnvelope dummyEnvelope() {
    KafkaMessageEnvelope env = new KafkaMessageEnvelope();
    env.producerMetadata = new ProducerMetadata();
    env.producerMetadata.messageTimestamp = 0;
    env.producerMetadata.messageSequenceNumber = 0;
    env.producerMetadata.segmentNumber = 0;
    env.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(8);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    env.payloadUnion = put;
    return env;
  }
}
