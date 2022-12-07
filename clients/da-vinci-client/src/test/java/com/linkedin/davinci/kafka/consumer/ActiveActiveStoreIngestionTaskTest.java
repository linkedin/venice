package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.writer.VeniceWriter.ENABLE_CHUNKING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;


public class ActiveActiveStoreIngestionTaskTest {
  @Test
  public void testLeaderCanSendValueChunksIntoDrainer()
      throws ExecutionException, InterruptedException, TimeoutException {
    String testTopic = "test";
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mock(HostLevelIngestionStats.class));
    when(ingestionTask.getVersionIngestionStats()).thenReturn(mock(AggVersionedIngestionStats.class));
    when(ingestionTask.getVersionedDIVStats()).thenReturn(mock(AggVersionedDIVStats.class));
    when(ingestionTask.getKafkaVersionTopic()).thenReturn(testTopic);
    LeaderProducerCallback mockLeaderProducerCallback = mock(LeaderProducerCallback.class);
    when(ingestionTask.createLeaderProducerCallback(any(), any(), any(), anyInt(), anyString(), anyLong()))
        .thenReturn(mockLeaderProducerCallback);
    doCallRealMethod().when(ingestionTask)
        .produceToLocalKafka(any(), any(), any(), any(), anyInt(), anyString(), anyInt(), anyLong());
    byte[] key = "foo".getBytes();
    byte[] updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);

    KafkaProducerWrapper mockedProducer = mock(KafkaProducerWrapper.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    AtomicLong offset = new AtomicLong(0);
    when(mockedProducer.sendMessage(anyString(), any(), any(), anyInt(), any())).thenAnswer(new Answer<Future>() {
      @Override
      public Future answer(InvocationOnMock invocation) throws Throwable {
        KafkaKey kafkaKey = (KafkaKey) invocation.getArgument(1);
        KafkaMessageEnvelope kafkaMessageEnvelope = (KafkaMessageEnvelope) invocation.getArgument(2);
        Callback callback = (Callback) invocation.getArgument(4);
        RecordMetadata recordMetadata = mock(RecordMetadata.class);
        offset.addAndGet(1);
        when(recordMetadata.offset()).thenReturn(offset.get());
        when(recordMetadata.serializedKeySize()).thenReturn(kafkaKey.getKeyLength());
        // when(recordMetadata.serializedValueSize()).thenReturn(((Put)(kafkaMessageEnvelope.payloadUnion)).putValue.remaining());
        when(recordMetadata.serializedValueSize()).thenReturn(123);
        LogManager.getLogger()
            .info(
                "DEBUGGING: GET CALLBACK: " + callback + " " + recordMetadata.offset() + " "
                    + recordMetadata.serializedKeySize() + " " + recordMetadata.serializedValueSize());
        callback.onCompletion(recordMetadata, null);
        return mockedFuture;
      }

    });
    Properties writerProperties = new Properties();
    writerProperties.put(ENABLE_CHUNKING, true);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic)
        // .setKeySerializer(serializer)
        // .setValueSerializer(serializer)
        // .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), () -> mockedProducer);

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    int valueSchemaId = 1;
    int rmdProtocolVersionID = 1;
    boolean resultReuseInput = true;
    String valueString = stringBuilder.toString();
    byte[] valueBytes = valueString.getBytes();
    byte[] schemaIdPrependedValueBytes = new byte[4 + valueBytes.length];
    ByteUtils.writeInt(schemaIdPrependedValueBytes, 1, 0);
    System.arraycopy(valueBytes, 0, schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedValueBytes = ByteBuffer.wrap(schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedRmdBytes = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PutMetadata putMetadata = new PutMetadata(1, updatedRmdBytes);

    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = mock(ConsumerRecord.class);
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    StoreIngestionTask.ProduceToTopic produceFunction = getProduceFunction(
        updatedKeyBytes,
        updatedValueBytes,
        updatedRmdBytes,
        valueSchemaId,
        rmdProtocolVersionID,
        writer,
        resultReuseInput);
    int subPartition = 0;
    String kafkaUrl = "kafkaUrl";
    int kafkaClusterId = 0;
    long beforeProcessingRecordTimestamp = 0;
    ingestionTask.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        produceFunction,
        subPartition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingRecordTimestamp);

    // Send 1 SOS, 2 Chunks, 1 Manifest.
    verify(mockedProducer, atLeast(4)).sendMessage(eq(testTopic), any(), any(), anyInt(), any());
    // Exactly once onCompletion call for manifest
    verify(mockLeaderProducerCallback, atMost(1)).onCompletion(any(), any());
    verify(mockLeaderProducerCallback, atLeast(1)).onCompletion(any(), any());
    // No Chunking Manifest is set.
    verify(mockLeaderProducerCallback, atMost(0)).setChunkingInfo(any(), any(), any(), any(), any());
  }

  public StoreIngestionTask.ProduceToTopic getProduceFunction(
      byte[] updatedKeyBytes,
      ByteBuffer updatedValueBytes,
      ByteBuffer updatedRmdBytes,
      int valueSchemaId,
      int rmdProtocolVersionID,
      VeniceWriter veniceWriter,
      boolean resultReuseInput) {
    return (callback, sourceTopicOffset) -> {
      final Callback newCallback = (recordMetadata, exception) -> {
        if (resultReuseInput) {
          // Restore the original header so this function is eventually idempotent as the original KME ByteBuffer
          // will be recovered after producing the message to Kafka or if the production failing.
          ByteUtils.prependIntHeaderToByteBuffer(
              updatedValueBytes,
              ByteUtils.getIntHeaderFromByteBuffer(updatedValueBytes),
              true);
        }
        callback.onCompletion(recordMetadata, exception);
      };
      return veniceWriter.put(
          updatedKeyBytes,
          ByteUtils.extractByteArray(updatedValueBytes),
          valueSchemaId,
          newCallback,
          sourceTopicOffset,
          VeniceWriter.APP_DEFAULT_LOGICAL_TS,
          Optional.of(new PutMetadata(rmdProtocolVersionID, updatedRmdBytes)));
    };
  }
}
