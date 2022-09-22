package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreBufferServiceTest {
  private static int TIMEOUT_IN_MS = 1000;

  @Test
  public void testRun() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic");
    int partition1 = 1;
    int partition2 = 2;
    String kafkaUrl = "blah";
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr1 = new ConsumerRecord<>(topic, partition1, -1, null, null);
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr2 = new ConsumerRecord<>(topic, partition2, -1, null, null);
    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, kafkaUrl, 0L);

    bufferService.stop();
    Assert.assertThrows(
        VeniceException.class,
        () -> bufferService.drainBufferedRecordsFromTopicPartition(topic, partition1));
  }

  @Test
  public void testRunWhenThrowException() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic");
    int partition1 = 1;
    int partition2 = 2;
    String kafkaUrl = "blah";
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr1 = new ConsumerRecord<>(topic, partition1, -1, null, null);
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr2 = new ConsumerRecord<>(topic, partition2, -1, null, null);
    Exception e = new VeniceException("test_exception");

    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, kafkaUrl, 0L);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, kafkaUrl, 0L);
    verify(mockTask).offerDrainerException(e, partition1);
    bufferService.stop();
  }

  @Test
  public void testDrainBufferedRecordsWhenNotExists() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic");
    int partition = 1;
    String kafkaUrl = "blah";
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr = new ConsumerRecord<>(topic, partition, -1, null, null);
    bufferService.start();
    bufferService.putConsumerRecord(cr, mockTask, null, partition, kafkaUrl, 0L);
    int nonExistingPartition = 2;
    bufferService.internalDrainBufferedRecordsFromTopicPartition(topic, nonExistingPartition, 3, 50);
    bufferService.stop();
  }

  @Test
  public void testDrainBufferedRecordsWhenExists() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic");
    int partition = 1;
    String kafkaUrl = "blah";
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr = new ConsumerRecord<>(topic, partition, 100, null, null);
    bufferService.start();
    bufferService.putConsumerRecord(cr, mockTask, null, partition, kafkaUrl, 0L);
    bufferService.internalDrainBufferedRecordsFromTopicPartition(topic, partition, 3, 50);
    bufferService.stop();
  }

  @Test
  public void testGetDrainerIndexForConsumerRecordSeparateDrainer() {
    String topic = Utils.getUniqueString("test_topic");
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

    SeparatedStoreBufferService bufferService = new SeparatedStoreBufferService(serverConfig);
    for (int partition = 0; partition < partitionCount; ++partition) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr = new ConsumerRecord<>(topic, partition, 100, null, null);
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

  @Test
  public void testGetDrainerIndexForConsumerRecord() {
    String topic = Utils.getUniqueString("test_topic");
    int partitionCount = 64;
    int drainerNum = 8;
    int[] drainerPartitionCount = new int[drainerNum];
    for (int i = 0; i < drainerNum; ++i) {
      drainerPartitionCount[i] = 0;
    }
    StoreBufferService bufferService = new StoreBufferService(8, 10000, 1000);
    for (int partition = 0; partition < partitionCount; ++partition) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr = new ConsumerRecord<>(topic, partition, 100, null, null);
      int drainerIndex = bufferService.getDrainerIndexForConsumerRecord(cr, partition);
      ++drainerPartitionCount[drainerIndex];
    }
    int avgPartitionCountPerDrainer = partitionCount / drainerNum;
    for (int i = 0; i < drainerNum; ++i) {
      Assert.assertEquals(drainerPartitionCount[i], avgPartitionCountPerDrainer);
    }
  }

  @Test
  public void testRunWhenThrowVeniceCheckSumFailException() throws Exception {
    StoreBufferService bufferService = new StoreBufferService(1, 10000, 1000);
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    String topic = Utils.getUniqueString("test_topic");
    int partition1 = 1;
    int partition2 = 2;
    String kafkaUrl = "blah";
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr1 = new ConsumerRecord<>(topic, partition1, -1, null, null);
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> cr2 = new ConsumerRecord<>(topic, partition2, -1, null, null);
    Exception e = new VeniceChecksumException("test_exception");
    doThrow(e).when(mockTask).processConsumerRecord(cr1, null, kafkaUrl, 0L);

    bufferService.putConsumerRecord(cr1, mockTask, null, partition1, kafkaUrl, 0L);
    bufferService.putConsumerRecord(cr2, mockTask, null, partition2, kafkaUrl, 0L);

    bufferService.start();
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr1, null, kafkaUrl, 0L);
    verify(mockTask, timeout(TIMEOUT_IN_MS)).processConsumerRecord(cr2, null, kafkaUrl, 0L);
    bufferService.getMaxMemoryUsagePerDrainer();
    for (int i = 0; i < 1; ++i) {
      // Verify map the cleared out
      Assert.assertTrue(bufferService.getTopicToTimeSpentMap(i).size() == 0);
    }
    verify(mockTask).offerDrainerException(e, partition1);
    verify(mockTask).recordChecksumVerificationFailure();
    bufferService.stop();
  }

}
