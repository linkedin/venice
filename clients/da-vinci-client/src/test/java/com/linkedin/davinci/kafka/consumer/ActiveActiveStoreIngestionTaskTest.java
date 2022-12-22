package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.writer.VeniceWriter.ENABLE_CHUNKING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ActiveActiveStoreIngestionTaskTest {
  @Test
  public void testLeaderCanSendValueChunksIntoDrainer()
      throws ExecutionException, InterruptedException, TimeoutException {
    String testTopic = "test";
    int valueSchemaId = 1;
    int rmdProtocolVersionID = 1;
    int kafkaClusterId = 0;
    int subPartition = 0;
    String kafkaUrl = "kafkaUrl";
    long beforeProcessingRecordTimestamp = 0;
    boolean resultReuseInput = true;

    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mock(HostLevelIngestionStats.class));
    when(ingestionTask.getVersionIngestionStats()).thenReturn(mock(AggVersionedIngestionStats.class));
    when(ingestionTask.getVersionedDIVStats()).thenReturn(mock(AggVersionedDIVStats.class));
    when(ingestionTask.getKafkaVersionTopic()).thenReturn(testTopic);
    when(ingestionTask.createProducerCallback(any(), any(), any(), anyInt(), anyString(), anyLong()))
        .thenCallRealMethod();
    when(ingestionTask.getProduceToTopicFunction(any(), any(), any(), anyInt(), anyBoolean())).thenCallRealMethod();
    when(ingestionTask.getRmdProtocolVersionID()).thenReturn(rmdProtocolVersionID);
    doCallRealMethod().when(ingestionTask)
        .produceToLocalKafka(any(), any(), any(), any(), anyInt(), anyString(), anyInt(), anyLong());
    byte[] key = "foo".getBytes();
    byte[] updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);

    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    AtomicLong offset = new AtomicLong(0);
    ArgumentCaptor<KafkaKey> kafkaKeyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<KafkaMessageEnvelope> kafkaMessageEnvelopeArgumentCaptor =
        ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    when(
        mockedProducer.sendMessage(
            anyString(),
            kafkaKeyArgumentCaptor.capture(),
            kafkaMessageEnvelopeArgumentCaptor.capture(),
            anyInt(),
            any())).thenAnswer((Answer<Future>) invocation -> {
              KafkaKey kafkaKey = invocation.getArgument(1);
              KafkaMessageEnvelope kafkaMessageEnvelope = invocation.getArgument(2);
              Callback callback = invocation.getArgument(4);
              RecordMetadata recordMetadata = mock(RecordMetadata.class);
              offset.addAndGet(1);
              when(recordMetadata.offset()).thenReturn(offset.get());
              when(recordMetadata.serializedKeySize()).thenReturn(kafkaKey.getKeyLength());
              MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope.messageType);
              when(recordMetadata.serializedValueSize()).thenReturn(
                  messageType.equals(MessageType.PUT)
                      ? ((Put) (kafkaMessageEnvelope.payloadUnion)).putValue.remaining()
                      : 0);
              callback.onCompletion(recordMetadata, null);
              return mockedFuture;
            });
    Properties writerProperties = new Properties();
    writerProperties.put(ENABLE_CHUNKING, true);

    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(testTopic).setPartitioner(new DefaultVenicePartitioner())
            .setTime(SystemTime.INSTANCE)
            .build();
    VeniceWriter<byte[], byte[], byte[]> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);
    when(ingestionTask.getVeniceWriter()).thenReturn(Lazy.of(() -> writer));
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    String valueString = stringBuilder.toString();
    byte[] valueBytes = valueString.getBytes();
    byte[] schemaIdPrependedValueBytes = new byte[4 + valueBytes.length];
    ByteUtils.writeInt(schemaIdPrependedValueBytes, 1, 0);
    System.arraycopy(valueBytes, 0, schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedValueBytes = ByteBuffer.wrap(schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedRmdBytes = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.offset()).thenReturn(100L);

    Put updatedPut = new Put();
    updatedPut.putValue = ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, resultReuseInput);
    updatedPut.schemaId = valueSchemaId;
    updatedPut.replicationMetadataVersionId = rmdProtocolVersionID;
    updatedPut.replicationMetadataPayload = updatedRmdBytes;
    LeaderProducedRecordContext leaderProducedRecordContext =
        LeaderProducedRecordContext.newPutRecord(kafkaClusterId, consumerRecord.offset(), updatedKeyBytes, updatedPut);

    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    ingestionTask.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        ingestionTask.getProduceToTopicFunction(
            updatedKeyBytes,
            updatedValueBytes,
            updatedRmdBytes,
            valueSchemaId,
            resultReuseInput),
        subPartition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingRecordTimestamp);

    // Send 1 SOS, 2 Chunks, 1 Manifest.
    verify(mockedProducer, times(4)).sendMessage(eq(testTopic), any(), any(), anyInt(), any());
    ArgumentCaptor<LeaderProducedRecordContext> leaderProducedRecordContextArgumentCaptor =
        ArgumentCaptor.forClass(LeaderProducedRecordContext.class);
    verify(ingestionTask, times(3)).produceToStoreBufferService(
        any(),
        leaderProducedRecordContextArgumentCaptor.capture(),
        anyInt(),
        anyString(),
        anyLong());

    // Make sure the chunks && manifest are exactly the same for both produced to VT and drain to leader storage.
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(0).getValueUnion(),
        kafkaMessageEnvelopeArgumentCaptor.getAllValues().get(1).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(1).getValueUnion(),
        kafkaMessageEnvelopeArgumentCaptor.getAllValues().get(2).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(2).getValueUnion(),
        kafkaMessageEnvelopeArgumentCaptor.getAllValues().get(3).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(0).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(1).getKey());
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(1).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(2).getKey());
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(2).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(3).getKey());
  }
}
