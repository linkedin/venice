package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
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
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreBufferServiceTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final KafkaKey key = new KafkaKey(MessageType.PUT, null);
  private final Put put = new Put(ByteBuffer.allocate(0), 0, 0, ByteBuffer.allocate(0));
  private final KafkaMessageEnvelope value =
      new KafkaMessageEnvelope(MessageType.PUT.getValue(), new ProducerMetadata(), put, null);
  private final LeaderProducedRecordContext leaderContext =
      LeaderProducedRecordContext.newPutRecord(0, 0, key.getKey(), put);
  private static final int TIMEOUT_IN_MS = 1000;

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRun(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
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
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr1 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, -1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr2 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition2, -1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr3 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition3, -1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr4 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition4, -1, 0, 0);

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
    Assert.assertThrows(
        VeniceException.class,
        () -> bufferService.drainBufferedRecordsFromTopicPartition(pubSubTopicPartition1));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRunWhenThrowException(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr1 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, -1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr2 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition2, -1, 0, 0);
    Exception e = new VeniceException("test_exception");

    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, partition1, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, partition2, kafkaUrl, 0L);
    verify(mockTask).setIngestionException(partition1, e);
    bufferService.stop();
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDrainBufferedRecordsWhenNotExists(boolean queueLeaderWrites) throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    String kafkaUrl = "blah";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, -1, 0, 0);
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
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    String kafkaUrl = "blah";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, 100, 0, 0);
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
    SeparatedStoreBufferService bufferService = new SeparatedStoreBufferService(serverConfig);
    for (int partition = 0; partition < partitionCount; ++partition) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr =
          new ImmutablePubSubMessage<>(key, value, new PubSubTopicPartitionImpl(pubSubTopic, partition), 100, 0, 0);
      int drainerIndex;
      if (partition < 16) {
        drainerIndex = bufferService.sortedServiceDelegate.getDrainerIndexForConsumerRecord(cr, partition);
        ++drainerPartitionCount[drainerIndex];
      } else {
        drainerIndex = bufferService.unsortedServiceDelegate.getDrainerIndexForConsumerRecord(cr, partition);
        ++drainerPartitionCount[drainerIndex + 8];
      }
    }

    for (int i = 0; i < drainerNum; i++) {
      Assert.assertNotNull(bufferService.getDrainerQueueMemoryUsage(i));
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
    StoreBufferService bufferService = new StoreBufferService(8, 10000, 1000, queueLeaderWrites);
    for (int partition = 0; partition < partitionCount; ++partition) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr =
          new ImmutablePubSubMessage<>(key, value, new PubSubTopicPartitionImpl(pubSubTopic, partition), 100, 0, 0);
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
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000, queueLeaderWrites);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr1 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, -1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr2 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition2, -1, 0, 0);
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
    String topic = Utils.getUniqueString("test_topic") + "_v1";
    int partition1 = 1;
    int partition2 = 2;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topic);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubSubTopic, partition1);
    PubSubTopicPartition pubSubTopicPartition2 = new PubSubTopicPartitionImpl(pubSubTopic, partition2);
    String kafkaUrl = "blah";
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr1 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, 0, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr2 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition2, 0, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr3 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition1, 1, 0, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> cr4 =
        new ImmutablePubSubMessage<>(key, value, pubSubTopicPartition2, 1, 0, 0);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0);
    verify(unsortedSBS).putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0);

    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    when(partitionConsumptionState.isDeferredWrite()).thenReturn(true);
    when(mockTask.getPartitionConsumptionState(partition1)).thenReturn(partitionConsumptionState);

    bufferService.putConsumerRecord(cr2, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS).putConsumerRecord(cr2, mockTask, null, partition1, kafkaUrl, 0);

    bufferService.putConsumerRecord(cr3, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS).putConsumerRecord(cr3, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS, never()).drainBufferedRecordsFromTopicPartition(any());
    verify(unsortedSBS, never()).drainBufferedRecordsFromTopicPartition(any());

    when(partitionConsumptionState.isDeferredWrite()).thenReturn(false);
    bufferService.putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
    verify(unsortedSBS).putConsumerRecord(cr4, mockTask, null, partition1, kafkaUrl, 0);
    verify(sortedSBS).drainBufferedRecordsFromTopicPartition(any());
    verify(unsortedSBS).drainBufferedRecordsFromTopicPartition(any());
  }
}
