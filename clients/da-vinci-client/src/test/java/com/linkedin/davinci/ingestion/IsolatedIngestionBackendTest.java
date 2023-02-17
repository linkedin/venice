package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.ISOLATED;
import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.MAIN;
import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.NOT_EXIST;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.START_CONSUMPTION;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.STOP_CONSUMPTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainTopicIngestionStatus;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionBackendTest {
  @Test
  public void testBackendCanDirectCommandCorrectly() {
    try (MainIngestionMonitorService monitorService = mock(MainIngestionMonitorService.class);
        IsolatedIngestionBackend backend = mock(IsolatedIngestionBackend.class)) {
      String topic = "testTopic";
      int partition = 0;

      when(backend.getMainIngestionMonitorService()).thenReturn(monitorService);
      KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
      when(storeIngestionService.isPartitionConsuming(topic, partition)).thenReturn(true);
      when(backend.getStoreIngestionService()).thenReturn(storeIngestionService);

      doCallRealMethod().when(backend).executeCommandWithRetry(anyString(), anyInt(), any(), any(), any());

      AtomicInteger executionFlag = new AtomicInteger();
      Supplier<Boolean> remoteProcessSupplier = () -> {
        executionFlag.set(1);
        return true;
      };
      Runnable localCommandRunnable = () -> executionFlag.set(-1);

      Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = new VeniceConcurrentHashMap<>();
      doCallRealMethod().when(monitorService).cleanupTopicPartitionState(topic, partition);
      doCallRealMethod().when(monitorService).setVersionPartitionToLocalIngestion(topic, partition);
      doCallRealMethod().when(monitorService).setVersionPartitionToIsolatedIngestion(topic, partition);
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenCallRealMethod();

      when(monitorService.getTopicIngestionStatusMap()).thenReturn(topicIngestionStatusMap);

      // Resource does not exist. Consumption request should be executed in remote.
      executionFlag.set(0);
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), NOT_EXIST);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), ISOLATED);
      Assert.assertEquals(executionFlag.get(), 1);

      // Other request should be executed in local.
      executionFlag.set(0);
      monitorService.cleanupTopicPartitionState(topic, partition);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), NOT_EXIST);
      Assert.assertEquals(executionFlag.get(), -1);

      // Resource exists in main process.
      executionFlag.set(0);
      monitorService.setVersionPartitionToLocalIngestion(topic, partition);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), MAIN);
      Assert.assertEquals(executionFlag.get(), -1);

      // Resource exists in isolated process.
      monitorService.setVersionPartitionToIsolatedIngestion(topic, partition);
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), ISOLATED);
      Assert.assertEquals(executionFlag.get(), 1);

    }
  }

  @Test
  public void testBackendCanHandleErrorCorrectly() {
    try (MainIngestionMonitorService monitorService = mock(MainIngestionMonitorService.class);
        IsolatedIngestionBackend backend = mock(IsolatedIngestionBackend.class)) {
      String topic = "testTopic";
      int partition = 0;

      when(backend.getMainIngestionMonitorService()).thenReturn(monitorService);
      KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
      when(storeIngestionService.isPartitionConsuming(topic, partition)).thenReturn(true);
      when(backend.getStoreIngestionService()).thenReturn(storeIngestionService);

      doCallRealMethod().when(backend).executeCommandWithRetry(anyString(), anyInt(), any(), any(), any());

      AtomicInteger executionFlag = new AtomicInteger();
      Runnable localCommandRunnable = () -> executionFlag.set(-1);

      Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = new VeniceConcurrentHashMap<>();
      doCallRealMethod().when(monitorService).cleanupTopicPartitionState(topic, partition);
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenCallRealMethod();
      when(monitorService.getTopicIngestionStatusMap()).thenReturn(topicIngestionStatusMap);

      /**
       * Test Case (1): Resource metadata in-sync: Expect resource in forked process but found in main process.
       * It should eventually execute command locally.
       */
      MainTopicIngestionStatus topicIngestionStatus = new MainTopicIngestionStatus(topic);
      topicIngestionStatus.setPartitionIngestionStatusToIsolatedIngestion(partition);
      topicIngestionStatusMap.put(topic, topicIngestionStatus);
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, () -> false, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);

      /**
       * Test Case (2): START_CONSUMPTION failed remotely. Ingestion status should be cleaned up so rollback can be
       * successful.
       */
      executionFlag.set(0);
      topicIngestionStatusMap.clear();
      Assert.assertThrows(() -> {
        backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, () -> {
          monitorService.setVersionPartitionToIsolatedIngestion(topic, partition);
          throw new VeniceException("Ingestion request failed due to timeout!");
        }, localCommandRunnable);
      });
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), NOT_EXIST);
      Assert.assertEquals(executionFlag.get(), 0);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, () -> false, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);
    }
  }
}
