package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class ControlMessageDumperTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String TOPIC = "test_store_v1";
  private static final int PARTITION = 0;

  @Test
  public void testDisplayPrintsPrcHeaderAsLongWhenLogHeadersEnabled() {
    GUID producerGuid = new GUID();
    long expectedRecordCount = 49925L;
    byte[] prcBytes = ByteBuffer.allocate(Long.BYTES).putLong(expectedRecordCount).array();
    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, prcBytes);

    DefaultPubSubMessage eopWithPrc = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, headers, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eopWithPrc), true);

    assertTrue(output.contains("END_OF_PUSH"), "EOP type should be printed: " + output);
    assertTrue(output.contains("headers:"), "headers: prefix should be printed: " + output);
    assertTrue(
        output.contains("prc = " + expectedRecordCount + " (long)"),
        "prc header should be decoded as long: " + output);
  }

  @Test
  public void testDisplaySuppressesHeadersByDefault() {
    GUID producerGuid = new GUID();
    byte[] prcBytes = ByteBuffer.allocate(Long.BYTES).putLong(123L).array();
    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, prcBytes);

    DefaultPubSubMessage eopWithPrc = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, headers, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eopWithPrc), false);

    assertTrue(output.contains("END_OF_PUSH"), output);
    assertFalse(output.contains("headers:"), "headers should be suppressed by default: " + output);
    assertFalse(output.contains("prc"), "prc value should not be printed without flag: " + output);
  }

  @Test
  public void testDisplayOmitsHeadersBlockWhenNoHeaders() {
    GUID producerGuid = new GUID();
    DefaultPubSubMessage eopNoHeaders = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, null, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eopNoHeaders), true);

    assertTrue(output.contains("END_OF_PUSH"), "EOP type should be printed: " + output);
    assertFalse(output.contains("headers:"), "no headers: block should be printed when no headers: " + output);
  }

  @Test
  public void testDisplayPrintsNonLongHeaderAsByteArray() {
    GUID producerGuid = new GUID();
    byte[] vtpBytes = "schema".getBytes(StandardCharsets.UTF_8);
    PubSubMessageHeaders headers =
        new PubSubMessageHeaders().add(PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER, vtpBytes);

    DefaultPubSubMessage eopWithVtp = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, headers, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eopWithVtp), true);

    assertTrue(output.contains("headers:"), output);
    assertTrue(
        output.contains("vtp = " + Arrays.toString(vtpBytes)),
        "Non-8-byte header should fall back to Arrays.toString(): " + output);
    assertFalse(output.contains("(long)"), "Non-8-byte header should not be decoded as long: " + output);
  }

  @Test
  public void testDisplayHandlesNullHeaderValue() {
    GUID producerGuid = new GUID();
    PubSubMessageHeaders headers = new PubSubMessageHeaders().add("custom", null);

    DefaultPubSubMessage eop = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, headers, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eop), true);

    assertTrue(output.contains("custom = null"), "Null header value should print 'null': " + output);
  }

  private static String runDumperAndCaptureOutput(List<DefaultPubSubMessage> messages, boolean logHeaders) {
    PubSubConsumerAdapter consumer = mockConsumer(messages);
    ControlMessageDumper dumper = new ControlMessageDumper(
        consumer,
        TOPIC,
        PARTITION,
        ApacheKafkaOffsetPosition.of(0),
        messages.size() + 1,
        logHeaders);

    PrintStream originalOut = System.out;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (PrintStream capturing = new PrintStream(buffer)) {
      System.setOut(capturing);
      dumper.fetch().display();
    } finally {
      System.setOut(originalOut);
    }
    return buffer.toString();
  }

  private static PubSubConsumerAdapter mockConsumer(List<DefaultPubSubMessage> messages) {
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(TOPIC), PARTITION);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> firstPoll = new HashMap<>();
    firstPoll.put(partition, messages);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> emptyPoll = Collections.emptyMap();

    when(consumer.poll(anyLong())).thenReturn(firstPoll).thenReturn(emptyPoll);
    return consumer;
  }

  private static DefaultPubSubMessage buildControlMessage(
      GUID producerGuid,
      ControlMessageType type,
      PubSubMessageHeaders headers,
      long offset) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = type.getValue();
    if (type == ControlMessageType.END_OF_PUSH) {
      controlMessage.controlMessageUnion = new EndOfPush();
    }
    return wrap(producerGuid, controlMessage, headers, offset);
  }

  private static DefaultPubSubMessage wrap(
      GUID producerGuid,
      ControlMessage controlMessage,
      PubSubMessageHeaders headers,
      long offset) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, Utils.getUniqueString("key-").getBytes());
    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.CONTROL_MESSAGE.getValue();
    envelope.producerMetadata = new ProducerMetadata();
    envelope.producerMetadata.producerGUID = producerGuid;
    envelope.producerMetadata.segmentNumber = 0;
    envelope.producerMetadata.messageSequenceNumber = 0;
    envelope.producerMetadata.messageTimestamp = 0L;
    envelope.payloadUnion = controlMessage;

    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(TOPIC), PARTITION);
    return new ImmutablePubSubMessage(
        kafkaKey,
        envelope,
        topicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        0L,
        0,
        headers);
  }
}
