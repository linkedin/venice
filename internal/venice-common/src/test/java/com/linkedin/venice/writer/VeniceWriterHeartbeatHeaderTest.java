package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceWriterHeartbeatHeaderTest {
  private VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  private PubSubProducerAdapter mockProducerAdapter;
  private Schema testSchema;
  private PubSubTopicPartition topicPartition;
  private PubSubTopicRepository pubSubTopicRepository;

  @BeforeMethod
  public void setUp() {
    mockProducerAdapter = mock(PubSubProducerAdapter.class);
    testSchema = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema();
    pubSubTopicRepository = new PubSubTopicRepository();
    topicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test_topic_v1"), 0);

    // Mock producer adapter to return completed future
    when(mockProducerAdapter.sendMessage(anyString(), anyInt(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Create VeniceWriter with protocol schema override to enable VTP headers
    VeniceWriterOptions options =
        new VeniceWriterOptions.Builder("test_topic").setKeyPayloadSerializer(new DefaultSerializer())
            .setValuePayloadSerializer(new DefaultSerializer())
            .setWriteComputePayloadSerializer(new DefaultSerializer())
            .setTime(SystemTime.INSTANCE)
            .setPartitioner(null)
            .setPartitionCount(1) // Set partition count to avoid "Invalid number of partitions: 0" error
            .build();

    // Create test properties for VeniceWriter
    Properties testProperties = new Properties();
    testProperties.put(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");

    // Override protocol schema to enable VTP headers.
    veniceWriter = new VeniceWriter<>(options, new VeniceProperties(testProperties), mockProducerAdapter, testSchema);
  }

  @Test
  public void testHeartbeatMessageDoesNotIncludeVtpHeader() {
    // Arrange
    ArgumentCaptor<PubSubMessageHeaders> headersCaptor = ArgumentCaptor.forClass(PubSubMessageHeaders.class);

    // Act - Send heartbeat message
    veniceWriter.sendHeartbeat(
        topicPartition,
        mock(PubSubProducerCallback.class),
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        false,
        LeaderCompleteState.LEADER_NOT_COMPLETED,
        System.currentTimeMillis(),
        true);

    // Assert
    verify(mockProducerAdapter).sendMessage(
        eq("test_topic_v1"),
        eq(0),
        eq(KafkaKey.HEART_BEAT),
        any(KafkaMessageEnvelope.class),
        headersCaptor.capture(),
        any(PubSubProducerCallback.class));

    PubSubMessageHeaders capturedHeaders = headersCaptor.getValue();

    // Verify VTP header is NOT present in heartbeat message
    PubSubMessageHeader vtpHeader = capturedHeaders.get(VENICE_TRANSPORT_PROTOCOL_HEADER);
    assertNull(vtpHeader, "Heartbeat messages should not include VTP headers");
  }

  @Test
  public void testDataMessageIncludesVtpHeaderWhenFirstMessage() {
    // Arrange
    ArgumentCaptor<PubSubMessageHeaders> headersCaptor = ArgumentCaptor.forClass(PubSubMessageHeaders.class);

    // Act - Send first data message (segment=0, sequence=0)
    veniceWriter.put(
        "test_key".getBytes(StandardCharsets.UTF_8),
        "test_value".getBytes(StandardCharsets.UTF_8),
        1,
        (PubSubProducerCallback) null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

    // Assert - sendMessage may be called multiple times (for views, etc.)
    verify(mockProducerAdapter, atLeastOnce()).sendMessage(
        anyString(),
        anyInt(),
        any(KafkaKey.class),
        any(KafkaMessageEnvelope.class),
        headersCaptor.capture(),
        any(PubSubProducerCallback.class));

    // Check if any of the captured headers contains VTP header
    boolean foundVtpHeader = false;
    for (PubSubMessageHeaders capturedHeaders: headersCaptor.getAllValues()) {
      PubSubMessageHeader vtpHeader = capturedHeaders.get(VENICE_TRANSPORT_PROTOCOL_HEADER);
      if (vtpHeader != null) {
        foundVtpHeader = true;
        // Verify header contains the protocol schema
        String schemaString = new String(vtpHeader.value(), StandardCharsets.UTF_8);
        assertTrue(schemaString.contains("KafkaMessageEnvelope"), "VTP header should contain KME schema");
        break;
      }
    }

    assertTrue(foundVtpHeader, "First data message should include VTP header");
  }
}
