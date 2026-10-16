package com.linkedin.davinci.kafka.consumer;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.StoreBufferServiceStats;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.mockito.MockedConstruction;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StoreBufferServiceTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final KafkaKey key = new KafkaKey(MessageType.PUT, new byte[0]);
  private final Put put = new Put(ByteBuffer.allocate(0), 0, 0, ByteBuffer.allocate(0));
  private final KafkaMessageEnvelope value =
      new KafkaMessageEnvelope(MessageType.PUT.getValue(), new ProducerMetadata(), put, null);
  private final LeaderProducedRecordContext leaderContext =
      LeaderProducedRecordContext.newPutRecord(0, mock(PubSubPosition.class), key.getKey(), put);
  private static final int TIMEOUT_IN_MS = 1000;
  private final MetricsRepository mockMetricRepo = mock(MetricsRepository.class);
  private StoreBufferServiceStats mockedStats;
  private PubSubPosition mockPosition;

  @BeforeMethod
  public void setUp() {
    mockPosition = mock(PubSubPosition.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricRepo).sensor(anyString(), any());
    mockedStats = mock(StoreBufferServiceStats.class);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRun(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    int partition1 = 1;
    int partition2 = 2;
    int partition3 = 3;
    int partition4 = 4;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    PubSubTopicPartition pubSubTopicPartition3 = new PubSubTopicPartitionImpl(pubSubTopic, partition3);
    PubSubTopicPartition pubSubTopicPartition4 = new PubSubTopicPartitionImpl(pubSubTopic, partition4);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr1 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mockPosition, 0, 0);
    DefaultPubSubMessage cr2 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition2, mockPosition, 0, 0);
    DefaultPubSubMessage cr3 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition3, mockPosition, 0, 0);
    DefaultPubSubMessage cr4 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition4, mockPosition, 0, 0);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr3, mockTask, leaderContext, partition3, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr4, mockTask, leaderContext, partition4, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, partition2, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr3, leaderContext, partition3, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr4, leaderContext, partition4, kafkaUrl, 0L);
    bufferService.stop();
    // We have 4 records in total, 2 of them are leader writes and 2 of them are not
    // When queueLeaderWrites is true, for leader writes, it'd be also added to the drainer queue for queueing and
    // processing
    // otherwise SIT will handle the processing directly.
    verify(mockedStats, times(queueLeaderWrites ? 4 : 2)).recordInternalProcessingLatency(anyLong(), any());
    Assert.assertThrows(
        VeniceException.class,
        () -> bufferService.drainBufferedRecordsFromTopicPartition(pubSubTopicPartition1, 50000));
  }

  @DataProvider
  public Object[][] ingestionExceptionRegistrationFailures() {
    return new Object[][] { { false, false }, { true, false }, { false, true }, { true, true } };
  }

  @Test(dataProvider = "ingestionExceptionRegistrationFailures")
  public void testRunWhenThrowException(boolean queueLeaderWrites, boolean registrationFails) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr1 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mockPosition, 0, 0);
    DefaultPubSubMessage cr2 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition2, mockPosition, 0, 0);
    Exception e = new VeniceException("test_exception");
    VeniceException registrationException = new VeniceException("exception registration failed");

    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    if (registrationFails) {
      doThrow(registrationException).when(mockTask).setIngestionException(partition1, e);
    }

    CompletableFuture<Void> persistedFuture = putRecordAndGetPersistedFuture(bufferService, mockTask, cr1);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, partition2, kafkaUrl, 0L);
    verify(mockTask).setIngestionException(partition1, e);
    bufferService.stop();
    if (registrationFails) {
      verify(mockTask).setLastStoreIngestionException(registrationException);
    } else {
      verify(mockTask, never()).setLastStoreIngestionException(any());
    }
    Assert.assertSame(
        Assert.expectThrows(ExecutionException.class, () -> persistedFuture.get(10, SECONDS)).getCause(),
        e);
    verify(mockedStats).recordInternalProcessingError(any());
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferedRecordsWhenNotExists(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    int partition = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr = new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mockPosition, 0, 0);
    bufferService.start();
    bufferService.putConsumerRecord(cr, mockTask, null, partition, kafkaUrl, 0L);
    int nonExistingPartition = 2;
    bufferService
        .drainBufferedRecordsFromTopicPartition(new PubSubTopicPartitionImpl(pubSubTopic, nonExistingPartition), 150);
    bufferService.stop();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferedRecordsWhenExists(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr = new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mockPosition, 0, 0);
    bufferService.start();
    bufferService.putConsumerRecord(cr, mockTask, null, partition, kafkaUrl, 0L);
    bufferService.drainBufferedRecordsFromTopicPartition(pubSubTopicPartition1, 150);
    bufferService.stop();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferRecordsWhenPCSIsNull(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    when(mockTask.getPartitionConsumptionState(partition)).thenReturn(null);
    when(mockTask.isGlobalRtDivEnabled()).thenReturn(false);
    doCallRealMethod().when(mockTask).updateOffsetMetadataAndSyncOffset(any());
    doCallRealMethod().when(mockTask).updateOffsetMetadataAndSyncOffset(any(), any());
    bufferService.start();
    CompletableFuture<Void> cmdFuture = bufferService.execSyncOffsetCommandAsync(pubSubTopicPartition1, mockTask);
    bufferService.drainBufferedRecordsFromTopicPartition(pubSubTopicPartition1, 50000);
    cmdFuture.get(SECONDS.toMillis(30), MILLISECONDS);
    Assert.assertTrue(cmdFuture.isDone()); // Make sure the command future is done
    bufferService.stop();
    verify(mockTask, never()).updateOffsetMetadataAndSyncOffset(any());
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testGetDrainerIndexForConsumerRecordSeparateDrainer(boolean queueLeaderWrites) {
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    int partitionCount = 32;
    int drainerNum = 16;
    int[] drainerPartitionCount = new int[drainerNum];
    for (int i = 0; i < drainerNum; ++i) {
      drainerPartitionCount[i] = 0;
    }
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(8).when(serverConfig).getDrainerPoolSizeSortedInput();
    doReturn(8).when(serverConfig).getDrainerPoolSizeUnsortedInput();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferNotifyDelta();
    doReturn(10000l).when(serverConfig).getStoreWriterBufferMemoryCapacity();
    doReturn(queueLeaderWrites).when(serverConfig).isStoreWriterBufferAfterLeaderLogicEnabled();
    doReturn(VeniceServerConfig.DEFAULT_WRITE_PATH_THREAD_PRIORITY).when(serverConfig).getWritePathThreadPriority();
    SeparatedStoreBufferService bufferService =
        new SeparatedStoreBufferService(serverConfig, mockMetricRepo, "test-cluster");
    for (int partition = 0; partition < partitionCount; ++partition) {
      DefaultPubSubMessage cr = new ImmutablePubSubMessage(
          key,
          value,
          new PubSubTopicPartitionImpl(pubSubTopic, partition),
          mock(PubSubPosition.class),
          0,
          0);
      int drainerIndex;
      if (partition < 16) {
        drainerIndex = bufferService.sortedStoreBufferServiceDelegate.getDrainerIndexForConsumerRecord(cr, partition);
        ++drainerPartitionCount[drainerIndex];
      } else {
        drainerIndex = bufferService.unsortedStoreBufferServiceDelegate.getDrainerIndexForConsumerRecord(cr, partition);
        ++drainerPartitionCount[drainerIndex + 8];
      }
    }

    int avgPartitionCountPerDrainer = partitionCount / drainerNum;
    for (int i = 0; i < drainerNum; ++i) {
      Assert.assertEquals(drainerPartitionCount[i], avgPartitionCountPerDrainer);
    }
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testGetDrainerIndexForConsumerRecord(boolean queueLeaderWrites) {
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    int partitionCount = 64;
    int drainerNum = 8;
    int[] drainerPartitionCount = new int[drainerNum];
    for (int i = 0; i < drainerNum; ++i) {
      drainerPartitionCount[i] = 0;
    }
    StoreBufferService bufferService = new StoreBufferService(8, 10000, 1000, queueLeaderWrites, mockedStats, null);
    for (int partition = 0; partition < partitionCount; ++partition) {
      DefaultPubSubMessage cr = new ImmutablePubSubMessage(
          key,
          value,
          new PubSubTopicPartitionImpl(pubSubTopic, partition),
          mockPosition,
          0,
          0);
      int drainerIndex = bufferService.getDrainerIndexForConsumerRecord(cr, partition);
      ++drainerPartitionCount[drainerIndex];
    }
    int avgPartitionCountPerDrainer = partitionCount / drainerNum;
    for (int i = 0; i < drainerNum; ++i) {
      Assert.assertEquals(drainerPartitionCount[i], avgPartitionCountPerDrainer);
    }
  }

  /**
   * Tests that {@link StoreBufferService#getDrainerIndexForConsumerRecord} assigns the same drainer index for both
   * real-time (RT) and separate real-time (Separate RT) topics for the same partition.
   */
  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testGetDrainerIndexForConsumerRecordSeparateRt(boolean queueLeaderWrites) {
    String baseTopicName = Utils.getUniqueString("test_topic");
    String realTimeTopic = Utils.composeRealTimeTopic(baseTopicName, 1);
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(realTimeTopic);
    PubSubTopic separateRtTopic = pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(realTimeTopic));
    List<PubSubTopic> topics = new ArrayList<>(Arrays.asList(rtTopic, separateRtTopic));
    StoreBufferService bufferService = new StoreBufferService(8, 10000, 1000, queueLeaderWrites, mockedStats, null);
    for (int partition = 0; partition < 64; ++partition) {
      int firstDrainerIndex = -1;
      for (PubSubTopic topic: topics) {
        PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, partition);
        DefaultPubSubMessage cr = new ImmutablePubSubMessage(key, value, topicPartition, mockPosition, 0, 0);
        int drainerIndex = bufferService.getDrainerIndexForConsumerRecord(cr, partition);
        if (firstDrainerIndex == -1) {
          firstDrainerIndex = drainerIndex;
        } else {
          Assert.assertEquals(drainerIndex, firstDrainerIndex, "Separate RT drainer should be the same as RT drainer");
        }
      }
    }
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRunWhenThrowVeniceCheckSumFailException(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr1 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mockPosition, 0, 0);
    DefaultPubSubMessage cr2 = new ImmutablePubSubMessage(key, value, pubSubTopicPartition2, mockPosition, 0, 0);
    Exception e = new VeniceChecksumException("test_exception", partition1);
    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, partition2, kafkaUrl, 0L);
    bufferService.getMaxMemoryUsagePerDrainer();
    for (int i = 0; i < 1; ++i) {
      // Verify map the cleared out
      Assert.assertTrue(bufferService.getTopicToTimeSpentMap(i).size() == 0);
    }
    verify(mockTask).setIngestionException(partition1, e);
    verify(mockTask).recordChecksumVerificationFailure();
    bufferService.stop();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testPutConsumerRecord(boolean queueLeaderWrites) throws InterruptedException {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(8).when(serverConfig).getDrainerPoolSizeSortedInput();
    doReturn(8).when(serverConfig).getDrainerPoolSizeUnsortedInput();
    doReturn(1000l).when(serverConfig).getStoreWriterBufferNotifyDelta();
    doReturn(10000l).when(serverConfig).getStoreWriterBufferMemoryCapacity();
    doReturn(queueLeaderWrites).when(serverConfig).isStoreWriterBufferAfterLeaderLogicEnabled();
    StoreBufferService sortedSBS = mock(StoreBufferService.class);
    StoreBufferService unsortedSBS = mock(StoreBufferService.class);
    SeparatedStoreBufferService bufferService = new SeparatedStoreBufferService(8, 8, sortedSBS, unsortedSBS);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    doReturn(false).when(mockTask).isHybridMode();
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    DefaultPubSubMessage cr1 =
        new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mock(PubSubPosition.class), 0, 0);
    DefaultPubSubMessage cr2 =
        new ImmutablePubSubMessage(key, value, pubSubTopicPartition2, mock(PubSubPosition.class), 0, 0);
    DefaultPubSubMessage cr3 =
        new ImmutablePubSubMessage(key, value, pubSubTopicPartition1, mock(PubSubPosition.class), 0, 0);
    DefaultPubSubMessage cr4 =
        new ImmutablePubSubMessage(key, value, pubSubTopicPartition2, mock(PubSubPosition.class), 0, 0);
    doReturn(true).when(mockTask).isHybridMode();

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0);
    verify(unsortedSBS).putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0);

    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    when(partitionConsumptionState.isDeferredWrite()).thenReturn(true);
    when(mockTask.getPartitionConsumptionState(partition1)).thenReturn(partitionConsumptionState);
    doReturn(false).when(mockTask).isHybridMode();

    bufferService.putConsumerRecord(cr2, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS).putConsumerRecord(cr2, mockTask, null, partition1, kafkaUrl, 0);

    bufferService.putConsumerRecord(cr3, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS).putConsumerRecord(cr3, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS, never()).drainBufferedRecordsFromTopicPartition(any(), anyLong());
    verify(unsortedSBS, never()).drainBufferedRecordsFromTopicPartition(any(), anyLong());

    when(partitionConsumptionState.isDeferredWrite()).thenReturn(false);
    doReturn(true).when(mockTask).isHybridMode();
    bufferService.putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
    verify(unsortedSBS).putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void testStoreWriterThreadFactoryUsesConfiguredPriority(boolean sorted, boolean queueFailureEnabled)
      throws Exception {
    int priority = Thread.NORM_PRIORITY - 2;
    AtomicReference<List<?>> constructorArguments = new AtomicReference<>();
    try (MockedConstruction<DaemonThreadFactory> ignored =
        mockConstruction(DaemonThreadFactory.class, (mock, context) -> {
          constructorArguments.set(context.arguments());
          doAnswer(invocation -> {
            Thread thread = new Thread(invocation.getArgument(0, Runnable.class));
            thread.setDaemon(true);
            return thread;
          }).when(mock).newThread(any());
        })) {
      StoreBufferService bufferService = queueFailureEnabled
          ? new StoreBufferService(
              1,
              10000,
              1000,
              false,
              null,
              mockMetricRepo,
              sorted,
              "test-cluster",
              0,
              priority,
              true)
          : new StoreBufferService(1, 10000, 1000, false, null, mockMetricRepo, sorted, "test-cluster", 0, priority);
      try {
        bufferService.start();
        Assert.assertEquals(constructorArguments.get().get(0), sorted ? "Store-writer-sorted" : "Store-writer-hybrid");
        Assert.assertEquals(constructorArguments.get().get(1), priority);
      } finally {
        bufferService.stop();
      }
    }
  }

  /**
   * If the previous drainer message's future is completed exceptionally, updateAndSyncOffsetFromSnapshot() isn't called
   */
  @Test
  public void testExecSyncOffsetFromSnapshotAsync() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, false, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    PartitionTracker mockSnapshot = mock(PartitionTracker.class); // VT DIV Snapshot

    int partition = 1;
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);

    // Mock PartitionConsumptionState with a CompletableFuture
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    when(mockTask.getPartitionConsumptionState(partition)).thenReturn(null);
    bufferService.start();

    // Case 1: PCS is null -> updateAndSyncOffsetFromSnapshot() should be called, returned future completes
    CompletableFuture<Void> case1 =
        bufferService.execSyncOffsetFromSnapshotAsync(topicPartition, mockSnapshot, future, mockTask);
    verify(mockTask, timeout(TIMEOUT_IN_MS).times(1)).updateAndSyncOffsetFromSnapshot(mockSnapshot, topicPartition);
    case1.get(TIMEOUT_IN_MS, MILLISECONDS);

    // Case 2: Future is null
    bufferService.execSyncOffsetFromSnapshotAsync(topicPartition, mockSnapshot, future, mockTask);
    verify(mockTask, timeout(TIMEOUT_IN_MS).times(2)).updateAndSyncOffsetFromSnapshot(mockSnapshot, topicPartition);

    // Case 3: Future is completed -> updateAndSyncOffsetFromSnapshot() is safe to be called
    bufferService.execSyncOffsetFromSnapshotAsync(topicPartition, mockSnapshot, future, mockTask);
    verify(mockTask, timeout(TIMEOUT_IN_MS).times(3)).updateAndSyncOffsetFromSnapshot(mockSnapshot, topicPartition);

    // Case 4: Previous message's future is completed exceptionally -> updateAndSyncOffsetFromSnapshot() not called, but
    // the waitable node's future still completes (normally) so the graceful-shutdown leader await never hangs.
    clearInvocations(mockTask);
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("Test exception"));
    CompletableFuture<Void> case4 =
        bufferService.execSyncOffsetFromSnapshotAsync(topicPartition, mockSnapshot, failedFuture, mockTask);
    case4.get(TIMEOUT_IN_MS, MILLISECONDS);
    verify(mockTask, never()).updateAndSyncOffsetFromSnapshot(mockSnapshot, topicPartition);

    bufferService.stop();
  }

  /**
   * The waitable {@code SyncGlobalRtDivNode} routes to {@link StoreIngestionTask#syncGlobalRtDivFromSnapshot} in the
   * drainer thread and completes the returned future (the null/EARLIEST guards live inside that method and are covered
   * at the ingestion-task level). If the snapshot sync throws, the future still completes exceptionally so the
   * graceful-shutdown await never hangs.
   */
  @Test
  public void testExecSyncGlobalRtDivAsync() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, false, mockedStats, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    int partition = 1;
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    bufferService.start();

    // Case 1: the drainer invokes syncGlobalRtDivFromSnapshot and the returned future completes.
    // syncGlobalRtDivFromSnapshot is mocked here (its real snapshot logic is covered in StoreIngestionTaskTest);
    // this test asserts only the routing.
    CompletableFuture<Void> syncFuture = bufferService.execSyncGlobalRtDivAsync(topicPartition, mockTask);
    syncFuture.get(SECONDS.toMillis(30), MILLISECONDS);
    Assert.assertTrue(syncFuture.isDone());
    verify(mockTask, timeout(TIMEOUT_IN_MS).times(1)).syncGlobalRtDivFromSnapshot(topicPartition);
    // The SYNC_OFFSET command path must not be used for this node.
    verify(mockTask, never()).updateOffsetMetadataAndSyncOffset(any());

    // Case 2: if the snapshot sync throws, the future still completes (exceptionally) so shutdown never hangs.
    clearInvocations(mockTask);
    doThrow(new VeniceException("boom")).when(mockTask).syncGlobalRtDivFromSnapshot(topicPartition);
    CompletableFuture<Void> failedFuture = bufferService.execSyncGlobalRtDivAsync(topicPartition, mockTask);
    Throwable error = failedFuture.handle((result, throwable) -> throwable).get(SECONDS.toMillis(30), MILLISECONDS);
    Assert.assertNotNull(error, "Future should complete exceptionally when the snapshot sync throws");
    Assert.assertTrue(failedFuture.isCompletedExceptionally());

    bufferService.stop();
  }

  private StoreBufferService newBufferServiceWithStallMonitor(long thresholdMs, MetricsRepository metricsRepository) {
    return newBufferServiceWithStallMonitor(thresholdMs, metricsRepository, false);
  }

  private StoreBufferService newBufferServiceWithStallMonitor(
      long thresholdMs,
      MetricsRepository metricsRepository,
      boolean queueFailureEnabled) {
    return new StoreBufferService(
        1,
        10000,
        1000,
        false,
        mockedStats,
        null,
        metricsRepository,
        true,
        "test-cluster",
        thresholdMs,
        queueFailureEnabled);
  }

  private CompletableFuture<Void> putRecordAndGetPersistedFuture(
      StoreBufferService bufferService,
      StoreIngestionTask ingestionTask,
      DefaultPubSubMessage record) throws InterruptedException {
    int partition = record.getTopicPartition().getPartitionNumber();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doCallRealMethod().when(pcs).setLastQueuedRecordPersistedFuture(any());
    doCallRealMethod().when(pcs).getLastQueuedRecordPersistedFuture();
    when(ingestionTask.getPartitionConsumptionState(partition)).thenReturn(pcs);
    bufferService.putConsumerRecord(record, ingestionTask, null, partition, "blah", 0L);
    return pcs.getLastQueuedRecordPersistedFuture();
  }

  @DataProvider
  public Object[][] idleStallThresholds() {
    return new Object[][] { { 1L, false }, { 0L, false }, { -1L, false }, { 1L, true }, { 0L, true }, { -1L, true } };
  }

  @Test(dataProvider = "idleStallThresholds")
  public void testIdleDrainersAreNotReportedAsBlocked(long thresholdMs, boolean queueFailureEnabled) throws Exception {
    try (AsyncGauge.AsyncGaugeExecutor gaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build()) {
      MetricsRepository metricsRepository = new MetricsRepository(new MetricConfig(gaugeExecutor));
      try (
          StoreBufferService bufferService =
              newBufferServiceWithStallMonitor(thresholdMs, metricsRepository, queueFailureEnabled);
          StallLogCapture logs = new StallLogCapture()) {
        bufferService.start();
        Assert.assertEquals(bufferService.getMaxDrainerBlockedTimeMs(), 0);
        bufferService.reportStalledDrainers();
        Assert.assertTrue(logs.getStallReports().isEmpty());
        Assert.assertEquals(bufferService.getDrainer(0).getProcessingStartedAtMs(), 0);
        Assert.assertNull(bufferService.getDrainer(0).getCurrentTopicPartition());
        Assert.assertEquals(
            metricsRepository.getMetric(".StoreBufferServiceSorted--max_blocked_time_per_writer.Gauge") != null,
            thresholdMs > 0);
        Assert.assertNotNull(metricsRepository.getMetric(".StoreBufferServiceSorted--total_memory_usage.Gauge"));
        Assert.assertNotNull(metricsRepository.getMetric(".StoreBufferServiceSorted--total_remaining_memory.Gauge"));
      } finally {
        metricsRepository.close();
      }
    }
  }

  @DataProvider
  public Object[][] queueFailureThresholds() {
    return new Object[][] { { true, 50L }, { false, 50L }, { true, 0L }, { true, -1L } };
  }

  @Test(dataProvider = "queueFailureThresholds")
  public void testAdmissionFailureCallbacksRespectKillSwitch(boolean enabled, long thresholdMs) {
    StoreBufferService bufferService = new StoreBufferService(
        1,
        10000,
        1000,
        true,
        mockedStats,
        null,
        null,
        true,
        "test-cluster",
        thresholdMs,
        enabled);
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("admission_failure_v1"), 0);
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    doReturn(partition).when(record).getTopicPartition();
    VeniceException failure = new VeniceException("record size calculation failed");
    doThrow(failure).when(record).getHeapSize();
    LeaderProducedRecordContext context = LeaderProducedRecordContext.newPutRecord(0, mockPosition, key.getKey(), put);

    VeniceException thrown = Assert.expectThrows(
        VeniceException.class,
        () -> bufferService.putConsumerRecord(record, task, context, 0, "kafka", 0L));

    Assert.assertSame(thrown, failure);
    boolean callbacksEnabled = enabled && thresholdMs > 0;
    verify(task, times(callbacksEnabled ? 1 : 0)).setIngestionException(0, failure);
    Assert.assertEquals(context.getPersistedToDBFuture().isCompletedExceptionally(), callbacksEnabled);
    if (!callbacksEnabled) {
      Assert.assertFalse(context.getPersistedToDBFuture().isDone());
    }
  }

  @Test(dataProvider = "queueFailureThresholds")
  public void testQueueFailureThresholdAndKillSwitch(boolean enabled, long thresholdMs) throws Exception {
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(thresholdMs, null, enabled);
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    DefaultPubSubMessage record = new ImmutablePubSubMessage(
        key,
        value,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("threshold_test_v1"), 0),
        mockPosition,
        0,
        0);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      entered.countDown();
      Assert.assertTrue(release.await(10, SECONDS));
      return null;
    }).when(task).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
    bufferService.start();
    try {
      CompletableFuture<Void> heldFuture = putRecordAndGetPersistedFuture(bufferService, task, record);
      Assert.assertTrue(entered.await(10, SECONDS));
      CompletableFuture<Void> queuedFuture = putRecordAndGetPersistedFuture(bufferService, task, record);
      long startedAt = bufferService.getDrainer(0).getProcessingStartedAtMs();
      if (thresholdMs > 0) {
        bufferService.reportStalledDrainers(startedAt + thresholdMs - 1);
        verify(task, never()).setIngestionException(anyInt(), any());
        Assert.assertFalse(queuedFuture.isDone(), "Sub-threshold work must remain queued");
      }
      bufferService.reportStalledDrainers(startedAt + Math.max(thresholdMs, 1));
      boolean failed = enabled && thresholdMs > 0;
      Assert.assertEquals(queuedFuture.isCompletedExceptionally(), failed);
      Assert.assertFalse(heldFuture.isDone(), "Monitor must not complete an executing write's future");
      if (failed) {
        verify(task, times(2)).setIngestionException(eq(0), any(VeniceException.class));
      } else {
        verify(task, never()).setIngestionException(anyInt(), any());
      }
      release.countDown();
      heldFuture.get(10, SECONDS);
      if (!failed) {
        queuedFuture.get(10, SECONDS);
        verify(task, times(2)).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      } else {
        verify(task).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      }
      TestUtils.waitForNonDeterministicAssertion(5, SECONDS, () -> {
        Assert.assertEquals(bufferService.getDrainer(0).getProcessingStartedAtMs(), 0L);
        Assert.assertNull(bufferService.getDrainer(0).getCurrentTopicPartition());
      });
      bufferService.reportStalledDrainers(Long.MAX_VALUE);
      DefaultPubSubMessage healthyRecord = new ImmutablePubSubMessage(
          key,
          value,
          new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("threshold_test_v1"), 1),
          mockPosition,
          0,
          0);
      putRecordAndGetPersistedFuture(bufferService, mock(StoreIngestionTask.class), healthyRecord).get(10, SECONDS);
    } finally {
      release.countDown();
      bufferService.stop();
    }
  }

  @Test(dataProvider = "ingestionExceptionRegistrationFailures")
  public void testStalledQueueFailsUnstartedWorkAndUnblocksEnqueues(boolean heldLeader, boolean registrationFails)
      throws Exception {
    StoreBufferService bufferService =
        spy(new StoreBufferService(2, 10000, 1000, true, mockedStats, null, null, true, "test-cluster", 50, true));
    PubSubTopic topic = pubSubTopicRepository.getTopic("queue_failure_test_v1");
    StoreIngestionTask heldTask = mock(StoreIngestionTask.class);
    StoreIngestionTask queuedTask = mock(StoreIngestionTask.class);
    StoreIngestionTask rejectedTask = mock(StoreIngestionTask.class);
    StoreIngestionTask healthyTask = mock(StoreIngestionTask.class);
    VeniceException registrationException = new VeniceException("exception registration failed");
    if (registrationFails) {
      for (StoreIngestionTask task: Arrays.asList(heldTask, queuedTask, rejectedTask)) {
        doThrow(registrationException).when(task).setIngestionException(anyInt(), any());
      }
    }
    List<DefaultPubSubMessage> records = new ArrayList<>();
    for (int i = 0; i <= 16; i++) {
      records.add(new ImmutablePubSubMessage(key, value, new PubSubTopicPartitionImpl(topic, i), mockPosition, 0, 0));
    }
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Thread> heldThread = new AtomicReference<>();
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch secondRelease = new CountDownLatch(1);
    doAnswer(invocation -> {
      heldThread.set(Thread.currentThread());
      entered.countDown();
      Assert.assertTrue(release.await(10, SECONDS), "Held write must be released by the test, not interrupted");
      return null;
    }).when(heldTask).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
    LeaderProducedRecordContext heldContext =
        LeaderProducedRecordContext.newPutRecord(0, mockPosition, key.getKey(), put);
    LeaderProducedRecordContext queuedContext =
        LeaderProducedRecordContext.newPutRecord(4, mockPosition, key.getKey(), put);
    LeaderProducedRecordContext blockedContext =
        LeaderProducedRecordContext.newPutRecord(12, mockPosition, key.getKey(), put);
    ExecutorService enqueuers = Executors.newFixedThreadPool(5);
    bufferService.start();
    try {
      CompletableFuture<Void> heldFuture;
      if (heldLeader) {
        bufferService.putConsumerRecord(records.get(0), heldTask, heldContext, 0, "blah", 0);
        heldFuture = heldContext.getPersistedToDBFuture();
      } else {
        heldFuture = putRecordAndGetPersistedFuture(bufferService, heldTask, records.get(0));
      }
      Assert.assertTrue(entered.await(10, SECONDS));
      List<CompletableFuture<Void>> queuedFutures = new ArrayList<>();
      queuedFutures.add(putRecordAndGetPersistedFuture(bufferService, queuedTask, records.get(2)));
      bufferService.putConsumerRecord(records.get(4), queuedTask, queuedContext, 4, "blah", 0);
      queuedFutures.add(queuedContext.getPersistedToDBFuture());
      queuedFutures.add(bufferService.execSyncOffsetCommandAsync(records.get(6).getTopicPartition(), queuedTask));
      queuedFutures.add(
          bufferService.execSyncOffsetFromSnapshotAsync(
              records.get(8).getTopicPartition(),
              mock(PartitionTracker.class),
              CompletableFuture.completedFuture(null),
              queuedTask));
      queuedFutures.add(bufferService.execSyncGlobalRtDivAsync(records.get(10).getTopicPartition(), queuedTask));
      MemoryBoundBlockingQueue<StoreBufferService.QueueNode> queue =
          bufferService.getDrainerForConsumerRecord(records.get(0), 0);
      long before = queue.remainingMemoryCapacityInByte();
      queuedFutures.add(putRecordAndGetPersistedFuture(bufferService, queuedTask, records.get(2)));
      long followerSize = before - queue.remainingMemoryCapacityInByte();
      while (queue.remainingMemoryCapacityInByte() >= followerSize) {
        queuedFutures.add(putRecordAndGetPersistedFuture(bufferService, queuedTask, records.get(2)));
      }
      int followerCount = queuedFutures.size() - 4;
      long commandSize;
      try (StoreBufferService sizeProbe = newBufferServiceWithStallMonitor(0, null)) {
        sizeProbe.execSyncGlobalRtDivAsync(records.get(10).getTopicPartition(), queuedTask);
        commandSize = sizeProbe.getTotalMemoryUsage();
      }
      // Commands can fit in the space left after filling with larger follower nodes.
      while (queue.remainingMemoryCapacityInByte() >= commandSize) {
        queuedFutures.add(bufferService.execSyncGlobalRtDivAsync(records.get(10).getTopicPartition(), queuedTask));
      }
      int globalCommandCount = queuedFutures.size() - followerCount - 3;

      // Delegate to the real queue while capturing the otherwise unreturned future of a blocked follower put.
      MemoryBoundBlockingQueue<StoreBufferService.QueueNode> recordingQueue = spy(queue);
      AtomicReference<CompletableFuture<Void>> blockedFollowerFuture = new AtomicReference<>();
      doAnswer(invocation -> {
        StoreBufferService.QueueNode node = invocation.getArgument(0);
        blockedFollowerFuture.set(node.getQueuedRecordPersistedFuture());
        queue.put(node);
        return null;
      }).when(recordingQueue).put(any());
      doReturn(recordingQueue).when(bufferService).getDrainerForConsumerRecord(records.get(14), 14);
      List<AtomicReference<Thread>> blockedThreads = new ArrayList<>();
      List<Future<?>> blockedPuts = new ArrayList<>();
      for (int type = 0; type < 5; type++) {
        int nodeType = type;
        AtomicReference<Thread> thread = new AtomicReference<>();
        blockedThreads.add(thread);
        blockedPuts.add(enqueuers.submit(() -> {
          thread.set(Thread.currentThread());
          switch (nodeType) {
            case 0:
              bufferService.putConsumerRecord(records.get(12), rejectedTask, blockedContext, 12, "blah", 0);
              break;
            case 1:
              bufferService.putConsumerRecord(records.get(14), rejectedTask, null, 14, "blah", 0);
              break;
            case 2:
              bufferService.execSyncOffsetCommandAsync(records.get(16).getTopicPartition(), rejectedTask);
              break;
            case 3:
              bufferService.execSyncGlobalRtDivAsync(records.get(16).getTopicPartition(), rejectedTask);
              break;
            default:
              bufferService.execSyncOffsetFromSnapshotAsync(
                  records.get(16).getTopicPartition(),
                  mock(PartitionTracker.class),
                  CompletableFuture.completedFuture(null),
                  rejectedTask);
          }
          return null;
        }));
      }
      TestUtils.waitForNonDeterministicAssertion(5, SECONDS, () -> {
        for (Future<?> blockedPut: blockedPuts) {
          Assert.assertFalse(blockedPut.isDone(), "Each enqueue must actually be blocked inside put");
        }
        for (AtomicReference<Thread> thread: blockedThreads) {
          Assert.assertNotNull(thread.get());
          Assert.assertEquals(thread.get().getState(), Thread.State.WAITING);
        }
      });
      int drainerIndex = bufferService.getDrainerIndexForConsumerRecord(records.get(0), 0);
      long startedAt = bufferService.getDrainer(drainerIndex).getProcessingStartedAtMs();
      bufferService.reportStalledDrainers(startedAt + 50);
      Throwable failure = queuedContext.getPersistedToDBFuture().handle((result, error) -> error).get(10, SECONDS);
      Assert.assertTrue(failure instanceof VeniceException);
      for (CompletableFuture<Void> future: queuedFutures) {
        Assert.assertSame(
            Assert.expectThrows(ExecutionException.class, () -> future.get(10, SECONDS)).getCause(),
            failure);
      }
      for (Future<?> blockedPut: blockedPuts) {
        Assert.assertSame(
            Assert.expectThrows(ExecutionException.class, () -> blockedPut.get(10, SECONDS)).getCause(),
            failure);
      }
      Assert.assertSame(
          Assert.expectThrows(ExecutionException.class, () -> blockedFollowerFuture.get().get(10, SECONDS)).getCause(),
          failure);
      Assert.assertTrue(blockedContext.getPersistedToDBFuture().isCompletedExceptionally());
      verify(heldTask).setIngestionException(0, (Exception) failure);
      for (int partition: new int[] { 4, 6, 8 }) {
        verify(queuedTask).setIngestionException(partition, (Exception) failure);
      }
      verify(queuedTask, times(followerCount)).setIngestionException(2, (Exception) failure);
      verify(queuedTask, times(globalCommandCount)).setIngestionException(10, (Exception) failure);
      verify(rejectedTask).setIngestionException(12, (Exception) failure);
      verify(rejectedTask).setIngestionException(14, (Exception) failure);
      verify(rejectedTask, times(3)).setIngestionException(16, (Exception) failure);
      if (registrationFails) {
        verify(heldTask).setLastStoreIngestionException(registrationException);
        verify(queuedTask, times(followerCount + globalCommandCount + 3))
            .setLastStoreIngestionException(registrationException);
        verify(rejectedTask, times(5)).setLastStoreIngestionException(registrationException);
      } else {
        for (StoreIngestionTask task: Arrays.asList(heldTask, queuedTask, rejectedTask)) {
          verify(task, never()).setLastStoreIngestionException(any());
        }
      }
      Assert.assertEquals(queue.getMemoryUsage(), 0L);
      Assert.assertTrue(queue.isEmpty());
      Assert.assertFalse(heldFuture.isDone(), "The real held write still owns its persistence future");
      Assert.assertFalse(heldThread.get().isInterrupted());
      bufferService.reportStalledDrainers(startedAt + 100);
      verify(heldTask).setIngestionException(0, (Exception) failure);

      StoreIngestionTask futureTask = mock(StoreIngestionTask.class);
      if (registrationFails) {
        doThrow(registrationException).when(futureTask).setIngestionException(anyInt(), any());
      }
      Assert.assertSame(
          Assert.expectThrows(
              VeniceException.class,
              () -> bufferService.putConsumerRecord(records.get(2), futureTask, null, 2, "blah", 0)),
          failure);
      verify(futureTask).setIngestionException(2, (Exception) failure);
      if (registrationFails) {
        verify(futureTask).setLastStoreIngestionException(registrationException);
      } else {
        verify(futureTask, never()).setLastStoreIngestionException(any());
      }
      putRecordAndGetPersistedFuture(bufferService, healthyTask, records.get(1)).get(10, SECONDS);
      verify(healthyTask).processConsumerRecord(records.get(1), null, 1, "blah", 0);
      verify(healthyTask, never()).setIngestionException(anyInt(), any());
      verify(queuedTask, never()).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      verify(queuedTask, never()).updateOffsetMetadataAndSyncOffset(any());
      verify(queuedTask, never()).updateAndSyncOffsetFromSnapshot(any(), any());
      verify(queuedTask, never()).syncGlobalRtDivFromSnapshot(any());
      verify(rejectedTask, never()).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      release.countDown();
      heldFuture.get(10, SECONDS);
      TestUtils.waitForNonDeterministicAssertion(
          5,
          SECONDS,
          () -> Assert.assertEquals(bufferService.getDrainer(drainerIndex).getProcessingStartedAtMs(), 0L));
      StoreIngestionTask resetTask = mock(StoreIngestionTask.class);
      doAnswer(invocation -> {
        Assert.assertSame(Thread.currentThread(), heldThread.get(), "Recovery must reuse the original worker");
        return null;
      }).when(resetTask).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      putRecordAndGetPersistedFuture(bufferService, resetTask, records.get(2)).get(10, SECONDS);
      verify(resetTask).processConsumerRecord(records.get(2), null, 2, "blah", 0);
      verify(resetTask, never()).setIngestionException(anyInt(), any());
      doAnswer(invocation -> {
        Assert.assertSame(Thread.currentThread(), heldThread.get());
        secondEntered.countDown();
        Assert.assertTrue(secondRelease.await(10, SECONDS));
        throw new VeniceException("actual held write failure");
      }).when(resetTask).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      CompletableFuture<Void> secondHeld = putRecordAndGetPersistedFuture(bufferService, resetTask, records.get(4));
      Assert.assertTrue(secondEntered.await(10, SECONDS));
      CompletableFuture<Void> secondCommand =
          bufferService.execSyncOffsetCommandAsync(records.get(6).getTopicPartition(), resetTask);
      bufferService.reportStalledDrainers(bufferService.getDrainer(drainerIndex).getProcessingStartedAtMs() + 50);
      Assert.assertTrue(secondCommand.isCompletedExceptionally(), "A recovered worker must detect another episode");
      Assert.assertFalse(secondHeld.isDone());
      secondRelease.countDown();
      ExecutionException actualWriteFailure =
          Assert.expectThrows(ExecutionException.class, () -> secondHeld.get(10, SECONDS));
      Assert.assertEquals(actualWriteFailure.getCause().getMessage(), "actual held write failure");
      TestUtils.waitForNonDeterministicAssertion(
          5,
          SECONDS,
          () -> Assert.assertEquals(bufferService.getDrainer(drainerIndex).getProcessingStartedAtMs(), 0L));
      putRecordAndGetPersistedFuture(bufferService, mock(StoreIngestionTask.class), records.get(2)).get(10, SECONDS);
    } finally {
      release.countDown();
      secondRelease.countDown();
      enqueuers.shutdownNow();
      Assert.assertTrue(enqueuers.awaitTermination(10, SECONDS));
      bufferService.stop();
    }
  }

  @DataProvider
  public Object[][] drainerExitFailures() {
    return new Object[][] { { new InterruptedException("held operation interrupted") },
        { new AssertionError("fatal held operation failure") } };
  }

  @Test(dataProvider = "drainerExitFailures")
  public void testExitingDrainerDoesNotRecoverFailedQueue(Throwable exitFailure) throws Exception {
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(50, null, true);
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("exit_test_v1"), 0);
    DefaultPubSubMessage record = new ImmutablePubSubMessage(key, value, partition, mockPosition, 0, 0);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(invocation -> {
      entered.countDown();
      Assert.assertTrue(release.await(10, SECONDS));
      throw exitFailure;
    }).when(task).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
    CompletableFuture<Void> heldFuture = putRecordAndGetPersistedFuture(bufferService, task, record);
    MemoryBoundBlockingQueue<StoreBufferService.QueueNode> queue = bufferService.getDrainerForConsumerRecord(record, 0);
    StoreBufferService.StoreBufferDrainer drainer =
        new StoreBufferService.StoreBufferDrainer(queue, 0, mockedStats, true, true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> worker = executor.submit(drainer);
    try {
      Assert.assertTrue(entered.await(10, SECONDS));
      CompletableFuture<Void> queuedFuture = putRecordAndGetPersistedFuture(bufferService, task, record);
      drainer.failStalledQueue(drainer.getProcessingStartedAtMs(), queue);
      Throwable stallFailure =
          Assert.expectThrows(ExecutionException.class, () -> queuedFuture.get(10, SECONDS)).getCause();
      Assert.assertFalse(heldFuture.isDone());
      release.countDown();
      worker.get(10, SECONDS);
      Assert.assertEquals(drainer.getProcessingStartedAtMs(), 0L);
      Assert.assertNull(drainer.getCurrentTopicPartition());
      Assert.assertSame(
          Assert.expectThrows(
              VeniceException.class,
              () -> bufferService.putConsumerRecord(record, task, null, 0, "blah", 0L)),
          stallFailure);
      verify(task, times(1)).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
      verify(task, times(3)).setIngestionException(0, (Exception) stallFailure);
    } finally {
      release.countDown();
      drainer.stop();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, SECONDS));
      bufferService.close();
    }
  }

  @Test
  public void testQueueRecoveryWaitsForDiscardedWorkFailureReporting() throws Exception {
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(50, null, true);
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("report_test_v1"), 0);
    DefaultPubSubMessage record = new ImmutablePubSubMessage(key, value, partition, mockPosition, 0, 0);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch releaseWrite = new CountDownLatch(1);
    CountDownLatch reporting = new CountDownLatch(1);
    CountDownLatch releaseReporting = new CountDownLatch(1);
    doAnswer(invocation -> {
      entered.countDown();
      Assert.assertTrue(releaseWrite.await(10, SECONDS));
      return null;
    }).when(task).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
    ExecutorService monitor = Executors.newSingleThreadExecutor();
    bufferService.start();
    try {
      CompletableFuture<Void> held = putRecordAndGetPersistedFuture(bufferService, task, record);
      Assert.assertTrue(entered.await(10, SECONDS));
      CompletableFuture<Void> queued = putRecordAndGetPersistedFuture(bufferService, task, record);
      CompletableFuture<Void> callback = queued.whenComplete((result, failure) -> {
        reporting.countDown();
        try {
          Assert.assertTrue(releaseReporting.await(10, SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new VeniceException(e);
        }
      });
      long startedAt = bufferService.getDrainer(0).getProcessingStartedAtMs();
      Future<?> report = monitor.submit(() -> bufferService.reportStalledDrainers(startedAt + 50));
      Assert.assertTrue(reporting.await(10, SECONDS));
      releaseWrite.countDown();
      held.get(10, SECONDS);
      Assert.expectThrows(
          VeniceException.class,
          () -> bufferService.putConsumerRecord(record, mock(StoreIngestionTask.class), null, 0, "blah", 0));
      releaseReporting.countDown();
      report.get(10, SECONDS);
      Assert.assertTrue(callback.isCompletedExceptionally());
      TestUtils.waitForNonDeterministicAssertion(
          5,
          SECONDS,
          () -> Assert.assertEquals(bufferService.getDrainer(0).getProcessingStartedAtMs(), 0L));
      putRecordAndGetPersistedFuture(bufferService, mock(StoreIngestionTask.class), record).get(10, SECONDS);
    } finally {
      releaseWrite.countDown();
      releaseReporting.countDown();
      monitor.shutdownNow();
      Assert.assertTrue(monitor.awaitTermination(10, SECONDS));
      bufferService.stop();
    }
  }

  @DataProvider
  public Object[][] stallReportingThresholds() {
    return new Object[][] { { 50L, true }, { HOURS.toMillis(1), false }, { 0L, false }, { -1L, false } };
  }

  @Test(dataProvider = "stallReportingThresholds")
  public void testStalledDrainerReportingAcrossEpisodesAndRestart(long thresholdMs, boolean reportExpected)
      throws Exception {
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(thresholdMs, null);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic("stall_test_v1");
    CountDownLatch releaseDrainer = new CountDownLatch(1);
    bufferService.start();
    try (StallLogCapture logs = new StallLogCapture()) {
      StoreBufferService.StoreBufferDrainer originalDrainer = bufferService.getDrainer(0);
      for (int episode = 0; episode < 3; episode++) {
        if (episode == 2) {
          bufferService.stop();
          Assert.assertEquals(bufferService.getMaxDrainerBlockedTimeMs(), 0);
          bufferService.start();
          Assert.assertSame(bufferService.getDrainer(0), originalDrainer, "Restart must retain older workers");
          Assert.assertNotSame(bufferService.getDrainer(1), originalDrainer);
        }

        CountDownLatch enteredDrainer = new CountDownLatch(1);
        releaseDrainer = new CountDownLatch(1);
        CountDownLatch releaseEpisode = releaseDrainer;
        doAnswer(invocation -> {
          enteredDrainer.countDown();
          Assert.assertTrue(releaseEpisode.await(10, SECONDS), "Drainer was not released");
          return null;
        }).when(mockTask).processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());

        PubSubTopicPartition heldPartition = new PubSubTopicPartitionImpl(pubSubTopic, episode);
        DefaultPubSubMessage held = new ImmutablePubSubMessage(key, value, heldPartition, mockPosition, 0, 0);
        long beforeEnqueueMs = System.currentTimeMillis();
        CompletableFuture<Void> persisted = putRecordAndGetPersistedFuture(bufferService, mockTask, held);
        Assert.assertTrue(enteredDrainer.await(10, SECONDS), "Drainer never picked up the queued record");
        StoreBufferService.StoreBufferDrainer drainer = bufferService.getDrainer(episode == 2 ? 1 : 0);
        if (thresholdMs > 0) {
          Assert.assertTrue(drainer.getProcessingStartedAtMs() >= beforeEnqueueMs);
          Assert.assertEquals(drainer.getCurrentTopicPartition(), heldPartition);
          TestUtils.waitForNonDeterministicAssertion(
              5,
              SECONDS,
              () -> Assert.assertTrue(bufferService.getMaxDrainerBlockedTimeMs() >= 50));
        } else {
          Assert.assertEquals(drainer.getProcessingStartedAtMs(), 0);
          Assert.assertNull(drainer.getCurrentTopicPartition(), "Disabled monitoring must not publish stall state");
          Assert.assertEquals(bufferService.getMaxDrainerBlockedTimeMs(), 0);
        }
        bufferService.reportStalledDrainers();
        List<String> reports = logs.getStallReports();
        int expectedReports = reportExpected ? episode + 1 : 0;
        Assert.assertEquals(reports.size(), expectedReports, "Only stalls reaching the threshold must report");
        if (reportExpected) {
          long queuedBytes = bufferService.getDrainerForConsumerRecord(held, episode).getMemoryUsage();
          Assert.assertTrue(
              reports.get(episode).contains("Drainer 0 has been holding topic-partition: " + heldPartition + " for "),
              "The report must use the worker's queue index and current partition");
          Assert.assertTrue(reports.get(episode).contains("; its queue holds " + queuedBytes + " of 10000 bytes"));
        }

        bufferService.reportStalledDrainers();
        Assert.assertEquals(logs.getStallReports().size(), expectedReports, "Each episode must be reported only once");
        releaseDrainer.countDown();
        persisted.get(10, SECONDS);
      }
    } finally {
      releaseDrainer.countDown();
      bufferService.stop();
    }
    Assert.assertEquals(bufferService.getMaxDrainerBlockedTimeMs(), 0);
    Assert.assertNull(bufferService.getDrainer(1).getCurrentTopicPartition());
    Assert.assertEquals(bufferService.getDrainer(1).getProcessingStartedAtMs(), 0);
    verify(mockedStats, times(3)).recordInternalProcessingLatency(anyLong(), any());
  }

  @Test
  public void testBlockedTimeGaugeCanOverlapRestart() throws Exception {
    CountDownLatch readingGauge = new CountDownLatch(1);
    CountDownLatch releaseGauge = new CountDownLatch(1);
    AtomicReference<Thread> gaugeThread = new AtomicReference<>();
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(50, null);
    ExecutorService gaugeReader = Executors.newSingleThreadExecutor();
    try (MockedConstruction<StoreBufferService.StoreBufferDrainer> drainers =
        mockConstruction(StoreBufferService.StoreBufferDrainer.class, (drainer, context) -> {
          when(drainer.getProcessingStartedAtMs()).thenAnswer(invocation -> {
            if (Thread.currentThread() == gaugeThread.get()) {
              readingGauge.countDown();
              Assert.assertTrue(releaseGauge.await(10, SECONDS), "Gauge was not released");
            }
            return 0L;
          });
        })) {
      bufferService.start();
      Future<Long> blockedTime = gaugeReader.submit(() -> {
        gaugeThread.set(Thread.currentThread());
        return bufferService.getMaxDrainerBlockedTimeMs();
      });
      Assert.assertTrue(readingGauge.await(10, SECONDS), "Gauge must pause inside its worker iteration");
      bufferService.stop();
      bufferService.start();
      Assert.assertEquals(drainers.constructed().size(), 2);
      releaseGauge.countDown();
      Assert.assertEquals(blockedTime.get(10, SECONDS).longValue(), 0);
      Assert.assertEquals(bufferService.getMaxDrainerBlockedTimeMs(), 0);
    } finally {
      releaseGauge.countDown();
      bufferService.stop();
      gaugeReader.shutdownNow();
      Assert.assertTrue(gaugeReader.awaitTermination(10, SECONDS));
    }
  }

  @DataProvider
  public Object[][] stallTrackingResetPaths() {
    return new Object[][] { { "record" }, { "exception" }, { "syncOffset" }, { "syncVtDiv" }, { "syncGlobalRtDiv" } };
  }

  @Test(dataProvider = "stallTrackingResetPaths")
  public void testStallTrackingResetsBeforeNextTake(String nodeType) throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("reset_test_v1"), 1);
    DefaultPubSubMessage record = new ImmutablePubSubMessage(key, value, partition, mockPosition, 0, 0);
    StoreBufferService bufferService = newBufferServiceWithStallMonitor(50, null);
    switch (nodeType) {
      case "syncOffset":
        bufferService.execSyncOffsetCommandAsync(partition, mockTask);
        break;
      case "syncVtDiv":
        bufferService.execSyncOffsetFromSnapshotAsync(
            partition,
            mock(PartitionTracker.class),
            CompletableFuture.completedFuture(null),
            mockTask);
        break;
      case "syncGlobalRtDiv":
        bufferService.execSyncGlobalRtDivAsync(partition, mockTask);
        break;
      default:
        if ("exception".equals(nodeType)) {
          doThrow(new VeniceException("test failure")).when(mockTask)
              .processConsumerRecord(any(), any(), anyInt(), anyString(), anyLong());
        }
        bufferService.putConsumerRecord(record, mockTask, null, 1, "blah", 0L);
    }
    StoreBufferService.QueueNode node = bufferService.getDrainerForConsumerRecord(record, 1).take();
    BlockingQueue<StoreBufferService.QueueNode> queue = mock(BlockingQueue.class);
    CountDownLatch idle = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(queue.take()).thenReturn(node).thenAnswer(invocation -> {
      idle.countDown();
      Assert.assertTrue(release.await(10, SECONDS), "Idle drainer was not released");
      throw new InterruptedException();
    });
    StoreBufferService.StoreBufferDrainer drainer =
        new StoreBufferService.StoreBufferDrainer(queue, 0, mockedStats, true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> task = executor.submit(drainer);
    try {
      Assert.assertTrue(idle.await(10, SECONDS), "Drainer never returned to take()");
      Assert.assertEquals(drainer.getProcessingStartedAtMs(), 0);
      Assert.assertNull(drainer.getCurrentTopicPartition());
      release.countDown();
      task.get(10, SECONDS);
    } finally {
      release.countDown();
      drainer.stop();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, SECONDS));
    }
  }

  private static class StallLogCapture implements AutoCloseable {
    private final Logger logger = (Logger) LogManager.getLogger(StoreBufferService.class);
    private final InMemoryLogAppender appender = new InMemoryLogAppender.Builder().build();

    private StallLogCapture() {
      appender.start();
      logger.addAppender(appender);
    }

    private List<String> getStallReports() {
      return appender.getLogs()
          .stream()
          .filter(log -> log.contains("has been holding topic-partition:"))
          .collect(toList());
    }

    @Override
    public void close() {
      logger.removeAppender(appender);
      appender.stop();
    }
  }
}
