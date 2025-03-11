package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.StoreBufferServiceStats;
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
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreBufferServiceTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final KafkaKey key = new KafkaKey(MessageType.PUT, new byte[0]);
  private final Put put = new Put(ByteBuffer.allocate(0), 0, 0, ByteBuffer.allocate(0));
  private final KafkaMessageEnvelope value =
      new KafkaMessageEnvelope(MessageType.PUT.getValue(), new ProducerMetadata(), put, null);
  private final LeaderProducedRecordContext leaderContext =
      LeaderProducedRecordContext.newPutRecord(0, 0, key.getKey(), put);
  private static final int TIMEOUT_IN_MS = 1000;
  private final MetricsRepository mockMetricRepo = mock(MetricsRepository.class);
  private StoreBufferServiceStats mockedStats;
  PubSubPosition mockPosition;

  @BeforeMethod
  public void setUp() {
    mockPosition = mock(PubSubPosition.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricRepo).sensor(anyString(), any());
    mockedStats = mock(StoreBufferServiceStats.class);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRun(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats);
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
    verify(mockedStats, times(queueLeaderWrites ? 4 : 2)).recordInternalProcessingLatency(anyLong());
    Assert.assertThrows(
        VeniceException.class,
        () -> bufferService.drainBufferedRecordsFromTopicPartition(pubSubTopicPartition1));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRunWhenThrowException(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats);
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

    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, partition2, kafkaUrl, 0L);
    verify(mockTask).setIngestionException(partition1, e);
    bufferService.stop();
    verify(mockedStats).recordInternalProcessingError();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferedRecordsWhenNotExists(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats);
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
    bufferService.internalDrainBufferedRecordsFromTopicPartition(
        new PubSubTopicPartitionImpl(pubSubTopic, nonExistingPartition),
        3,
        50);
    bufferService.stop();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferedRecordsWhenExists(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats);
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
    bufferService.internalDrainBufferedRecordsFromTopicPartition(pubSubTopicPartition1, 3, 50);
    bufferService.stop();
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
    SeparatedStoreBufferService bufferService = new SeparatedStoreBufferService(serverConfig, mockMetricRepo);
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
    StoreBufferService bufferService = new StoreBufferService(8, 10000, 1000, queueLeaderWrites, mockedStats);
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

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRunWhenThrowVeniceCheckSumFailException(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites, mockedStats);
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
    Exception e = new VeniceChecksumException("test_exception");
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
    verify(sortedSBS, never()).drainBufferedRecordsFromTopicPartition(any());
    verify(unsortedSBS, never()).drainBufferedRecordsFromTopicPartition(any());

    when(partitionConsumptionState.isDeferredWrite()).thenReturn(false);
    doReturn(true).when(mockTask).isHybridMode();
    bufferService.putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
    verify(unsortedSBS).putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
  }
}
