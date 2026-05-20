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
import com.linkedin.venice.writer.LeaderCompleteState;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
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
    long expectedRecordCount = 49925L;
    String output = runWithHeader(
        PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER,
        ByteBuffer.allocate(Long.BYTES).putLong(expectedRecordCount).array(),
        true);

    assertTrue(output.contains("END_OF_PUSH"), output);
    assertTrue(output.contains("headers:"), output);
    assertTrue(
        output.contains("prc = " + expectedRecordCount + "\n"),
        "prc should be decoded as bare long without sentinel tag: " + output);
    assertFalse(output.contains("(unavailable)"), "Non-sentinel value should not be tagged unavailable: " + output);
  }

  @Test
  public void testDisplayMarksPrcUnavailableSentinel() {
    String output = runWithHeader(
        PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER,
        ByteBuffer.allocate(Long.BYTES).putLong(PubSubMessageHeaders.PRC_HEADER_UNAVAILABLE_SENTINEL).array(),
        true);

    assertTrue(
        output.contains("prc = " + PubSubMessageHeaders.PRC_HEADER_UNAVAILABLE_SENTINEL + " (unavailable)"),
        "PRC sentinel value should be tagged as unavailable: " + output);
  }

  @Test
  public void testDisplayDecodesExecutionIdAsLong() {
    long executionId = 8675309L;
    String output = runWithHeader(
        PubSubMessageHeaders.EXECUTION_ID_KEY,
        ByteBuffer.allocate(Long.BYTES).putLong(executionId).array(),
        true);

    assertTrue(
        output.contains(PubSubMessageHeaders.EXECUTION_ID_KEY + " = " + executionId + "\n"),
        "EXECUTION_ID should be decoded as bare long: " + output);
  }

  @Test
  public void testDisplayDecodesLeaderCompletionStateAsEnumName() {
    String output = runWithHeader(
        PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER,
        new byte[] { (byte) LeaderCompleteState.LEADER_COMPLETED.getValue() },
        true);

    assertTrue(
        output.contains("lcs = " + LeaderCompleteState.LEADER_COMPLETED.name()),
        "lcs should be decoded to enum name: " + output);
  }

  @Test
  public void testDisplayHandlesUnknownLeaderCompletionStateValue() {
    String output =
        runWithHeader(PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER, new byte[] { (byte) 99 }, true);

    assertTrue(output.contains("lcs = unknown(99)"), "Unknown lcs value should be flagged: " + output);
  }

  @Test
  public void testDisplayDecodesViewPartitionsMapAsJsonString() {
    String json = "{\"view1\":[0],\"view2\":[1,2]}";
    String output = runWithHeader(
        PubSubMessageHeaders.VENICE_VIEW_PARTITIONS_MAP_HEADER,
        json.getBytes(StandardCharsets.UTF_8),
        true);

    assertTrue(output.contains("vpm = " + json), "vpm should be decoded as raw JSON UTF-8 string: " + output);
  }

  @Test
  public void testDisplayDecodesShortVtpAsUtf8String() {
    String shortSchema = "{\"type\":\"record\",\"name\":\"K\"}";
    String output = runWithHeader(
        PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER,
        shortSchema.getBytes(StandardCharsets.UTF_8),
        true);

    assertTrue(output.contains("vtp = " + shortSchema), "Short vtp should be decoded inline: " + output);
    assertFalse(output.contains("avro envelope schema"), "Short vtp should not be truncated-tagged: " + output);
  }

  @Test
  public void testDisplayTruncatesLongVtpAndReportsTotalSize() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < ControlMessageDumper.VTP_DISPLAY_CHAR_LIMIT + 50; i++) {
      builder.append('a');
    }
    String longSchema = builder.toString();
    byte[] vtpBytes = longSchema.getBytes(StandardCharsets.UTF_8);

    String output = runWithHeader(PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER, vtpBytes, true);

    assertTrue(
        output.contains("... (avro envelope schema, " + vtpBytes.length + " bytes total)"),
        "Long vtp should be truncated with size suffix: " + output);
  }

  @Test
  public void testDisplaySuppressesHeadersByDefault() {
    byte[] prcBytes = ByteBuffer.allocate(Long.BYTES).putLong(123L).array();
    String output = runWithHeader(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, prcBytes, false);

    assertTrue(output.contains("END_OF_PUSH"), output);
    assertFalse(output.contains("headers:"), "headers should be suppressed by default: " + output);
    assertFalse(output.contains("prc"), "prc value should not be printed without flag: " + output);
  }

  @Test
  public void testDisplayOmitsHeadersBlockWhenNoHeaders() {
    GUID producerGuid = new GUID();
    DefaultPubSubMessage eopNoHeaders = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, null, 100L);

    String output = runDumperAndCaptureOutput(Collections.singletonList(eopNoHeaders), true);

    assertTrue(output.contains("END_OF_PUSH"), output);
    assertFalse(output.contains("headers:"), "no headers: block should be printed when no headers: " + output);
  }

  @Test
  public void testDisplayFallsBackToLongForUnknown8ByteHeader() {
    byte[] eightBytes = ByteBuffer.allocate(Long.BYTES).putLong(42L).array();
    String output = runWithHeader("custom-long", eightBytes, true);

    assertTrue(
        output.contains("custom-long = 42 (long)"),
        "Unknown 8-byte header should fall back to long decode with (long) tag: " + output);
  }

  @Test
  public void testDisplayFallsBackToByteArrayForUnknownArbitraryHeader() {
    byte[] arbitrary = new byte[] { 1, 2, 3 };
    String output = runWithHeader("custom-bytes", arbitrary, true);

    assertTrue(
        output.contains("custom-bytes = " + Arrays.toString(arbitrary)),
        "Unknown non-8-byte header should fall back to Arrays.toString: " + output);
  }

  @Test
  public void testDisplayHandlesNullHeaderValue() {
    String output = runWithHeader("custom", null, true);

    assertTrue(output.contains("custom = null"), "Null header value should print 'null': " + output);
  }

  private static String runWithHeader(String key, byte[] value, boolean logHeaders) {
    GUID producerGuid = new GUID();
    PubSubMessageHeaders headers = new PubSubMessageHeaders().add(key, value);
    DefaultPubSubMessage eop = buildControlMessage(producerGuid, ControlMessageType.END_OF_PUSH, headers, 100L);
    return runDumperAndCaptureOutput(Collections.singletonList(eop), logHeaders);
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
    try (PrintStream capturing = new PrintStream(buffer, true, StandardCharsets.UTF_8.name())) {
      System.setOut(capturing);
      dumper.fetch().display();
      return buffer.toString(StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("UTF-8 is required by the JDK spec", e);
    } finally {
      System.setOut(originalOut);
    }
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
    KafkaKey kafkaKey =
        new KafkaKey(MessageType.CONTROL_MESSAGE, Utils.getUniqueString("key-").getBytes(StandardCharsets.UTF_8));
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
