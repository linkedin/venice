package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.testng.annotations.Test;


public class IngestionBatchProcessorTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test
  public void isAllMessagesFromRTTopicTest() {
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic("store_v1");
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");

    PubSubTopicPartition versionTopicPartition = new PubSubTopicPartitionImpl(versionTopic, 1);
    PubSubTopicPartition rtTopicPartition = new PubSubTopicPartitionImpl(rtTopic, 1);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> vtMessage1 = new ImmutablePubSubMessage<>(
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        versionTopicPartition,
        1,
        100,
        100);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> vtMessage2 = new ImmutablePubSubMessage<>(
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        versionTopicPartition,
        2,
        101,
        100);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage1 = new ImmutablePubSubMessage<>(
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        1,
        100,
        100);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage2 = new ImmutablePubSubMessage<>(
        mock(KafkaKey.class),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        2,
        101,
        100);

    assertFalse(IngestionBatchProcessor.isAllMessagesFromRTTopic(Arrays.asList(vtMessage1, vtMessage2)));
    assertFalse(IngestionBatchProcessor.isAllMessagesFromRTTopic(Arrays.asList(vtMessage1, rtMessage1)));
    assertTrue(IngestionBatchProcessor.isAllMessagesFromRTTopic(Arrays.asList(rtMessage1, rtMessage2)));
  }

  @Test
  public void lockKeysTest() {
    KeyLevelLocksManager mockKeyLevelLocksManager = mock(KeyLevelLocksManager.class);
    ReentrantLock lockForKey1 = mock(ReentrantLock.class);
    ReentrantLock lockForKey2 = mock(ReentrantLock.class);
    byte[] key1 = "key1".getBytes();
    byte[] key2 = "key2".getBytes();
    when(mockKeyLevelLocksManager.acquireLockByKey(ByteArrayKey.wrap(key1))).thenReturn(lockForKey1);
    when(mockKeyLevelLocksManager.acquireLockByKey(ByteArrayKey.wrap(key2))).thenReturn(lockForKey2);

    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTopicPartition = new PubSubTopicPartitionImpl(rtTopic, 1);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage1 = new ImmutablePubSubMessage<>(
        new KafkaKey(MessageType.PUT, key1),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        1,
        100,
        100);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage2 = new ImmutablePubSubMessage<>(
        new KafkaKey(MessageType.PUT, key2),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        2,
        101,
        100);

    IngestionBatchProcessor batchProcessor = new IngestionBatchProcessor(
        "store_v1",
        mock(ExecutorService.class),
        mockKeyLevelLocksManager,
        (ignored1, ignored2, ignored3, ignored4, ignored5, ignored6, ignored7) -> null,
        true,
        true,
        mock(AggVersionedIngestionStats.class),
        mock(HostLevelIngestionStats.class));
    List<ReentrantLock> locks = batchProcessor.lockKeys(Arrays.asList(rtMessage1, rtMessage2));
    verify(mockKeyLevelLocksManager).acquireLockByKey(ByteArrayKey.wrap(key1));
    verify(mockKeyLevelLocksManager).acquireLockByKey(ByteArrayKey.wrap(key2));
    verify(lockForKey1).lock();
    verify(lockForKey2).lock();
    assertEquals(locks.get(0), lockForKey1);
    assertEquals(locks.get(1), lockForKey2);

    // unlock test
    batchProcessor.unlockKeys(Arrays.asList(rtMessage1, rtMessage2), locks);

    verify(lockForKey1).unlock();
    verify(lockForKey2).unlock();
    verify(mockKeyLevelLocksManager).releaseLock(ByteArrayKey.wrap(key1));
    verify(mockKeyLevelLocksManager).releaseLock(ByteArrayKey.wrap(key2));
  }

  @Test
  public void processTest() {
    KeyLevelLocksManager mockKeyLevelLocksManager = mock(KeyLevelLocksManager.class);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    byte[] key1 = "key1".getBytes();
    byte[] key2 = "key2".getBytes();
    PubSubTopicPartition rtTopicPartition = new PubSubTopicPartitionImpl(rtTopic, 1);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage1 = new ImmutablePubSubMessage<>(
        new KafkaKey(MessageType.PUT, key1),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        1,
        100,
        100);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> rtMessage2 = new ImmutablePubSubMessage<>(
        new KafkaKey(MessageType.PUT, key2),
        mock(KafkaMessageEnvelope.class),
        rtTopicPartition,
        2,
        101,
        100);

    AggVersionedIngestionStats mockAggVersionedIngestionStats = mock(AggVersionedIngestionStats.class);
    HostLevelIngestionStats mockHostLevelIngestionStats = mock(HostLevelIngestionStats.class);

    IngestionBatchProcessor batchProcessor = new IngestionBatchProcessor(
        "store_v1",
        Executors.newFixedThreadPool(1, new DaemonThreadFactory("test")),
        mockKeyLevelLocksManager,
        (consumerRecord, ignored2, ignored3, ignored4, ignored5, ignored6, ignored7) -> {
          if (Arrays.equals(consumerRecord.getKey().getKey(), "key1".getBytes())) {
            Put put = new Put();
            put.setPutValue(ByteBuffer.wrap("value1".getBytes()));
            WriteComputeResultWrapper writeComputeResultWrapper = new WriteComputeResultWrapper(put, null, true);
            return new PubSubMessageProcessedResult(writeComputeResultWrapper);
          } else if (Arrays.equals(consumerRecord.getKey().getKey(), "key2".getBytes())) {
            Put put = new Put();
            put.setPutValue(ByteBuffer.wrap("value2".getBytes()));
            WriteComputeResultWrapper writeComputeResultWrapper = new WriteComputeResultWrapper(put, null, true);
            return new PubSubMessageProcessedResult(writeComputeResultWrapper);
          }
          return null;
        },
        true,
        true,
        mockAggVersionedIngestionStats,
        mockHostLevelIngestionStats);

    List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>> result = batchProcessor.process(
        Arrays.asList(rtMessage1, rtMessage2),
        mock(PartitionConsumptionState.class),
        1,
        "test_kafka",
        1,
        1,
        1);

    assertEquals(result.size(), 2);
    PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> resultForKey1 = result.get(0);
    assertEquals(
        resultForKey1.getProcessedResult().getWriteComputeResultWrapper().getNewPut().putValue.array(),
        "value1".getBytes());
    PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> resultForKey2 = result.get(1);
    assertEquals(
        resultForKey2.getProcessedResult().getWriteComputeResultWrapper().getNewPut().putValue.array(),
        "value2".getBytes());
    verify(mockAggVersionedIngestionStats).recordBatchProcessingRequest(eq("store"), eq(1), eq(2), anyLong());
    verify(mockAggVersionedIngestionStats).recordBatchProcessingLatency(eq("store"), eq(1), anyDouble(), anyLong());
    verify(mockHostLevelIngestionStats).recordBatchProcessingRequest(2);
    verify(mockHostLevelIngestionStats).recordBatchProcessingRequestLatency(anyDouble());

    // Error path
    batchProcessor = new IngestionBatchProcessor(
        "store_v1",
        Executors.newFixedThreadPool(1, new DaemonThreadFactory("test")),
        mockKeyLevelLocksManager,
        (consumerRecord, ignored2, ignored3, ignored4, ignored5, ignored6, ignored7) -> {
          if (Arrays.equals(consumerRecord.getKey().getKey(), "key1".getBytes())) {
            Put put = new Put();
            put.setPutValue(ByteBuffer.wrap("value1".getBytes()));
            WriteComputeResultWrapper writeComputeResultWrapper = new WriteComputeResultWrapper(put, null, true);
            return new PubSubMessageProcessedResult(writeComputeResultWrapper);
          } else if (Arrays.equals(consumerRecord.getKey().getKey(), "key2".getBytes())) {
            throw new VeniceException("Fake");
          }
          return null;
        },
        true,
        true,
        mockAggVersionedIngestionStats,
        mockHostLevelIngestionStats);
    final IngestionBatchProcessor finalBatchProcessor = batchProcessor;
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> finalBatchProcessor.process(
            Arrays.asList(rtMessage1, rtMessage2),
            mock(PartitionConsumptionState.class),
            1,
            "test_kafka",
            1,
            1,
            1));
    assertTrue(exception.getMessage().contains("Failed to execute the batch processing"));
    verify(mockAggVersionedIngestionStats).recordBatchProcessingRequestError("store", 1);
    verify(mockHostLevelIngestionStats).recordBatchProcessingRequestError();
  }

}
