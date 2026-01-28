package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.testng.annotations.Test;


public class LeaderProducerCallbackTest {
  @Test
  public void testOnCompletionWithNonNullException() {
    LeaderFollowerStoreIngestionTask ingestionTaskMock = mock(LeaderFollowerStoreIngestionTask.class);
    DefaultPubSubMessage sourceConsumerRecordMock = mock(DefaultPubSubMessage.class);
    PartitionConsumptionState partitionConsumptionStateMock = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContextMock = mock(LeaderProducedRecordContext.class);
    AggVersionedDIVStats statsMock = mock(AggVersionedDIVStats.class);
    int partition = 5;
    String kafkaUrl = "dc-0.kafka.venice.org";
    long beforeProcessingRecordTimestamp = 67454542;
    String storeName = Utils.getUniqueString("test-store");
    AtomicInteger reportedStatsCounter = new AtomicInteger();

    when(ingestionTaskMock.getStoreName()).thenReturn(storeName);
    when(ingestionTaskMock.getVersionedDIVStats()).thenReturn(statsMock);
    // return: Unique Topic Partitions: (T1:1), (T2:21), and (T2:22)
    when(sourceConsumerRecordMock.getTopicName()).thenReturn("T1", "T2", "T1", "T2", "T1", "T2");
    when(sourceConsumerRecordMock.getPartition()).thenReturn(1, 21, 1, 22, 1, 22);
    doAnswer(i -> reportedStatsCounter.getAndIncrement()).when(statsMock).recordLeaderProducerFailure(storeName, 0);

    InMemoryLogAppender inMemoryLogAppender = new InMemoryLogAppender.Builder().build();
    inMemoryLogAppender.start();
    LoggerContext ctx = ((LoggerContext) LogManager.getContext(false));
    Configuration config = ctx.getConfiguration();
    doReturn(true).when(ingestionTaskMock).isTransientRecordBufferUsed(any());
    doReturn(null).when(partitionConsumptionStateMock).getTransientRecord(any());
    doReturn(true).when(partitionConsumptionStateMock).isEndOfPushReceived();
    doReturn(mock(KafkaKey.class)).when(sourceConsumerRecordMock).getKey();

    try {
      config.addLoggerAppender(
          (org.apache.logging.log4j.core.Logger) LogManager.getLogger(LeaderProducerCallback.class),
          inMemoryLogAppender);

      LeaderProducerCallback leaderProducerCallback = new LeaderProducerCallback(
          ingestionTaskMock,
          sourceConsumerRecordMock,
          partitionConsumptionStateMock,
          leaderProducedRecordContextMock,
          partition,
          kafkaUrl,
          beforeProcessingRecordTimestamp);

      int cbInvocations = 6; // (T1:1), (T2:21), (T1:1), (T2:22), (T1:1), (T2:22)
      String exMessage = "Producer is closed forcefully";

      for (int i = 0; i < cbInvocations; i++) {
        leaderProducerCallback.onCompletion(null, new VeniceException(exMessage));
        byte[] serializedKey = ("key" + i).getBytes();
        leaderProducerCallback.setChunkingInfo(serializedKey, null, null, null, null, null, null);
      }

      // Message should be logged only three time as there are just 3 unique Topic-Partition
      // N.B. That assertion seems to be flaky... it sometimes get 4 logs instead of 3.
      // TODO: Figure out why it's flaky... and maybe remove the assertion entirely if it's expected...
      List<String> logs = inMemoryLogAppender.getLogs();
      long matchedLogs = logs.stream()
          .filter(log -> log.contains("Leader failed to send out message to version topic when consuming "))
          .count();
      assertEquals(matchedLogs, 3L);
      assertEquals(reportedStatsCounter.get(), cbInvocations); // stats should be reported for all invocations
    } finally {
      LoggerConfig loggerConfig = config.getLoggerConfig(LeaderFollowerStoreIngestionTask.class.getName());
      if (loggerConfig.getName().equals(LeaderFollowerStoreIngestionTask.class.getCanonicalName())) {
        loggerConfig.removeAppender(inMemoryLogAppender.getName());
      }
      ctx.updateLoggers();
      inMemoryLogAppender.stop();
    }
  }

  @Test
  public void testLeaderProducerCallbackProduceDeprecatedChunkDeletion() throws InterruptedException {
    LeaderFollowerStoreIngestionTask storeIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    DefaultPubSubMessage sourceConsumerRecord = mock(DefaultPubSubMessage.class);
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    LeaderProducerCallback leaderProducerCallback = new LeaderProducerCallback(
        storeIngestionTask,
        sourceConsumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        0,
        "url",
        0);

    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      manifest.keysWithChunkIdSuffix.add(ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    }
    leaderProducerCallback.produceDeprecatedChunkDeletionToStoreBufferService(manifest, 0);
    verify(storeIngestionTask, times(10))
        .produceToStoreBufferService(any(), any(), anyInt(), anyString(), anyLong(), anyLong());

  }

  @Test
  public void testHighProducerCompletionLatencyWarning() throws Exception {
    LeaderFollowerStoreIngestionTask ingestionTaskMock = mock(LeaderFollowerStoreIngestionTask.class);
    DefaultPubSubMessage sourceConsumerRecordMock = mock(DefaultPubSubMessage.class);
    PartitionConsumptionState partitionConsumptionStateMock = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContextMock = mock(LeaderProducedRecordContext.class);
    AggVersionedIngestionStats versionedIngestionStatsMock = mock(AggVersionedIngestionStats.class);
    com.linkedin.venice.pubsub.api.PubSubProduceResult produceResultMock =
        mock(com.linkedin.venice.pubsub.api.PubSubProduceResult.class);
    com.linkedin.venice.pubsub.api.PubSubTopicPartition topicPartitionMock =
        mock(com.linkedin.venice.pubsub.api.PubSubTopicPartition.class);
    com.linkedin.venice.pubsub.api.PubSubTopic pubSubTopicMock = mock(com.linkedin.venice.pubsub.api.PubSubTopic.class);
    String storeName = Utils.getUniqueString("test-store");
    String replicaId = "test_store_v1_0";

    when(ingestionTaskMock.getStoreName()).thenReturn(storeName);
    when(ingestionTaskMock.isUserSystemStore()).thenReturn(false);
    when(ingestionTaskMock.getVersionIngestionStats()).thenReturn(versionedIngestionStatsMock);
    when(partitionConsumptionStateMock.getReplicaId()).thenReturn(replicaId);
    when(partitionConsumptionStateMock.isEndOfPushReceived()).thenReturn(false);
    when(sourceConsumerRecordMock.getTopicPartition()).thenReturn(topicPartitionMock);
    when(topicPartitionMock.getPubSubTopic()).thenReturn(pubSubTopicMock);
    when(pubSubTopicMock.isRealTime()).thenReturn(false);
    when(produceResultMock.getSerializedSize()).thenReturn(100);
    when(produceResultMock.getPubSubPosition()).thenReturn(mock(com.linkedin.venice.pubsub.api.PubSubPosition.class));
    doReturn(mock(com.linkedin.venice.pubsub.api.PubSubPosition.class)).when(leaderProducedRecordContextMock)
        .getConsumedPosition();

    InMemoryLogAppender inMemoryLogAppender = new InMemoryLogAppender.Builder().build();
    inMemoryLogAppender.start();
    LoggerContext ctx = ((LoggerContext) LogManager.getContext(false));
    Configuration config = ctx.getConfiguration();

    try {
      config.addLoggerAppender(
          (org.apache.logging.log4j.core.Logger) LogManager.getLogger(LeaderProducerCallback.class),
          inMemoryLogAppender);

      LeaderProducerCallback leaderProducerCallback = new LeaderProducerCallback(
          ingestionTaskMock,
          sourceConsumerRecordMock,
          partitionConsumptionStateMock,
          leaderProducedRecordContextMock,
          0,
          "kafka-url",
          0);

      // Use reflection to set produceTimeNs to simulate high latency (>30 seconds ago)
      java.lang.reflect.Field produceTimeField = LeaderProducerCallback.class.getDeclaredField("produceTimeNs");
      produceTimeField.setAccessible(true);
      long thirtyFiveSecondsAgoInNs = System.nanoTime() - (35_000L * 1_000_000L); // 35 seconds in nanoseconds
      produceTimeField.set(leaderProducerCallback, thirtyFiveSecondsAgoInNs);

      // Call onCompletion with success (null exception)
      leaderProducerCallback.onCompletion(produceResultMock, null);

      // Verify warning log was generated
      List<String> logs = inMemoryLogAppender.getLogs();
      long matchedLogs = logs.stream()
          .filter(log -> log.contains("High leader producer completion latency detected"))
          .filter(log -> log.contains(replicaId))
          .filter(log -> log.contains("threshold: 30000.0 ms"))
          .count();
      assertEquals(matchedLogs, 1L, "Expected exactly one warning log for high producer latency");
    } finally {
      LoggerConfig loggerConfig = config.getLoggerConfig(LeaderProducerCallback.class.getName());
      if (loggerConfig.getName().equals(LeaderProducerCallback.class.getCanonicalName())) {
        loggerConfig.removeAppender(inMemoryLogAppender.getName());
      }
      ctx.updateLoggers();
      inMemoryLogAppender.stop();
    }
  }
}
