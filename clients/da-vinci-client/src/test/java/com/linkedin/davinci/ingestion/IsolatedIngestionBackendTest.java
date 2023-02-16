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
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
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

      // Resource does not exist.
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenReturn(NOT_EXIST);
      // Consumption request should be executed in remote.
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), 1);

      // Other request should be executed in local.
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);

      // Resource exists in main process.
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenReturn(MAIN);
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);

      // Resource exists in isolated process.
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenReturn(ISOLATED);
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, remoteProcessSupplier, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), 1);

      // Resource metadata in-sync: Expect resource in forked process but found in main process. It should eventually
      // execute command locally.
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenReturn(ISOLATED);
      executionFlag.set(0);
      backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, () -> false, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);
    }
  }
}
