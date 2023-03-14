package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.Utils;
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
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sourceConsumerRecordMock = mock(PubSubMessage.class);
    PartitionConsumptionState partitionConsumptionStateMock = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContextMock = mock(LeaderProducedRecordContext.class);
    AggVersionedDIVStats statsMock = mock(AggVersionedDIVStats.class);
    int subPartition = 5;
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

    try {
      config.addLoggerAppender(
          (org.apache.logging.log4j.core.Logger) LogManager.getLogger(LeaderFollowerStoreIngestionTask.class),
          inMemoryLogAppender);

      LeaderProducerCallback leaderProducerCallback = new LeaderProducerCallback(
          ingestionTaskMock,
          sourceConsumerRecordMock,
          partitionConsumptionStateMock,
          leaderProducedRecordContextMock,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestamp);

      int cbInvocations = 6; // (T1:1), (T2:21), (T1:1), (T2:22), (T1:1), (T2:22)
      String exMessage = "Producer is closed forcefully";

      for (int i = 0; i < cbInvocations; i++) {
        leaderProducerCallback.onCompletion(null, new VeniceException(exMessage));
      }

      // Message should be logged only three time as there are just 3 unique Topic-Partition
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
}
