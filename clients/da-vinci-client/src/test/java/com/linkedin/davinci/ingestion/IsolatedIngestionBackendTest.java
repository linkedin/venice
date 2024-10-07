package com.linkedin.davinci.ingestion;

import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.ISOLATED;
import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.MAIN;
import static com.linkedin.davinci.ingestion.main.MainPartitionIngestionStatus.NOT_EXIST;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.START_CONSUMPTION;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType.STOP_CONSUMPTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.main.MainIngestionMonitorService;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.davinci.ingestion.main.MainTopicIngestionStatus;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionBackendTest {
  @Test
  public void testBackendCanDirectCommandCorrectly() {
    try (MainIngestionMonitorService monitorService = mock(MainIngestionMonitorService.class);
        IsolatedIngestionBackend backend = mock(IsolatedIngestionBackend.class)) {
      String topic = "testTopic_v1";
      int partition = 0;
      when(backend.getMainIngestionMonitorService()).thenReturn(monitorService);
      KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
      ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
      when(repository.waitVersion(anyString(), anyInt(), any()))
          .thenReturn(Pair.create(mock(Store.class), mock(Version.class)));
      when(storeIngestionService.getMetadataRepo()).thenReturn(repository);
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
      when(backend.isTopicPartitionHosted(topic, partition)).thenCallRealMethod();
      when(backend.isTopicPartitionHostedInMainProcess(topic, partition)).thenCallRealMethod();

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
      String topic = "testTopic_v1";
      int partition = 0;

      when(backend.getMainIngestionMonitorService()).thenReturn(monitorService);
      KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
      ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
      when(repository.waitVersion(anyString(), anyInt(), any()))
          .thenReturn(Pair.create(mock(Store.class), mock(Version.class)));
      when(storeIngestionService.getMetadataRepo()).thenReturn(repository);
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
       * Test Case (2): Remote START_CONSUMPTION failed remotely due to exception. Ingestion status should be cleaned up
       * so rollback can be successful.
       */
      executionFlag.set(0);
      topicIngestionStatusMap.clear();
      Assert.assertThrows(VeniceTimeoutException.class, () -> {
        backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, () -> {
          monitorService.setVersionPartitionToIsolatedIngestion(topic, partition);
          throw new VeniceTimeoutException("Ingestion request failed due to timeout!");
        }, localCommandRunnable);
      });
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), NOT_EXIST);
      Assert.assertEquals(executionFlag.get(), 0);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, () -> false, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);

      /**
       * Test Case (3): Remote START_CONSUMPTION failed locally due to no version exist. Ingestion status should be
       * cleaned up so rollback can be successful.
       */
      executionFlag.set(0);
      topicIngestionStatusMap.clear();
      when(repository.waitVersion(anyString(), anyInt(), any())).thenReturn(Pair.create(null, null));

      Assert.assertThrows(VeniceException.class, () -> {
        backend.executeCommandWithRetry(topic, partition, START_CONSUMPTION, () -> {
          monitorService.setVersionPartitionToIsolatedIngestion(topic, partition);
          throw new VeniceTimeoutException("Ingestion request failed due to timeout!");
        }, localCommandRunnable);
      });
      Assert.assertEquals(monitorService.getTopicPartitionIngestionStatus(topic, partition), NOT_EXIST);
      Assert.assertEquals(executionFlag.get(), 0);
      backend.executeCommandWithRetry(topic, partition, STOP_CONSUMPTION, () -> false, localCommandRunnable);
      Assert.assertEquals(executionFlag.get(), -1);
    }
  }

  @Test
  public void testIsolatedIngestionNotifierAsyncCompletionHandling() {
    IsolatedIngestionBackend backend = mock(IsolatedIngestionBackend.class);
    VeniceNotifier ingestionNotifier = mock(VeniceNotifier.class);
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    VeniceStoreVersionConfig storeVersionConfig = mock(VeniceStoreVersionConfig.class);
    MainIngestionMonitorService mainIngestionMonitorService = mock(MainIngestionMonitorService.class);
    ExecutorService executor = Executors.newFixedThreadPool(10);

    when(backend.getMainIngestionMonitorService()).thenReturn(mainIngestionMonitorService);
    when(backend.getCompletionHandlingExecutor()).thenReturn(executor);
    when(backend.getIsolatedIngestionNotifier(any())).thenCallRealMethod();
    when(backend.getConfigLoader()).thenReturn(configLoader);

    String topic = "topic_v1";
    when(configLoader.getStoreConfig(topic)).thenReturn(storeVersionConfig);
    when(backend.isTopicPartitionHosted(topic, 0)).thenReturn(false);
    when(backend.isTopicPartitionHosted(topic, 1)).thenReturn(true);
    when(backend.isTopicPartitionHosted(topic, 2)).thenReturn(true);
    backend.getIsolatedIngestionNotifier(ingestionNotifier).completed(topic, 0, 123L, "");
    verify(backend, times(0)).getCompletionHandlingExecutor();

    backend.getIsolatedIngestionNotifier(ingestionNotifier).completed(topic, 1, 123L, "");
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      verify(backend, times(1)).getCompletionHandlingExecutor();
      verify(mainIngestionMonitorService, times(1)).setVersionPartitionToLocalIngestion(topic, 1);
    });
    // Throw exception with calling startConsumptionLocally for next partition
    doThrow(new VeniceException("Store not in repo")).when(backend).startConsumptionLocally(any(), anyInt());
    backend.getIsolatedIngestionNotifier(ingestionNotifier).completed(topic, 2, 123L, "");
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      verify(backend, times(2)).getCompletionHandlingExecutor();
      // It should also set the state to locally no matter what.
      verify(mainIngestionMonitorService, times(1)).setVersionPartitionToLocalIngestion(topic, 2);
    });
  }

  @Test
  public void testBackendCanMaintainMetadataCorrectlyForDroppingPartition() {
    try (MainIngestionMonitorService monitorService = mock(MainIngestionMonitorService.class);
        IsolatedIngestionBackend backend = mock(IsolatedIngestionBackend.class)) {
      String topic = "testTopic_v1";
      int partition = 0;
      VeniceStoreVersionConfig storeVersionConfig = mock(VeniceStoreVersionConfig.class);
      when(storeVersionConfig.getStoreVersionName()).thenReturn(topic);
      MainIngestionRequestClient ingestionRequestClient = mock(MainIngestionRequestClient.class);
      when(backend.getMainIngestionRequestClient()).thenReturn(ingestionRequestClient);
      when(backend.getMainIngestionMonitorService()).thenReturn(monitorService);
      Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = new VeniceConcurrentHashMap<>();
      MainTopicIngestionStatus topicIngestionStatus = new MainTopicIngestionStatus(topic);
      topicIngestionStatus.setPartitionIngestionStatusToLocalIngestion(partition);
      topicIngestionStatusMap.put(topic, topicIngestionStatus);
      doCallRealMethod().when(monitorService).cleanupTopicPartitionState(topic, partition);
      when(monitorService.getTopicPartitionIngestionStatus(topic, partition)).thenCallRealMethod();
      when(monitorService.getTopicIngestionStatusMap()).thenReturn(topicIngestionStatusMap);
      when(backend.isTopicPartitionHostedInMainProcess(anyString(), anyInt())).thenCallRealMethod();
      when(backend.isTopicPartitionHosted(anyString(), anyInt())).thenCallRealMethod();
      doCallRealMethod().when(backend).dropStoragePartitionGracefully(any(), anyInt(), anyInt(), anyBoolean());
      doCallRealMethod().when(backend).executeCommandWithRetry(anyString(), anyInt(), any(), any(), any());
      when(monitorService.getTopicPartitionCount(topic)).thenReturn(2L);

      // Case 1: Local remove topic partition executed successfully.
      backend.dropStoragePartitionGracefully(storeVersionConfig, partition, 180, false);
      verify(backend, times(1)).removeTopicPartitionLocally(any(), anyInt(), anyInt(), anyBoolean());
      verify(ingestionRequestClient, times(1)).resetTopicPartition(topic, partition);
      verify(monitorService, times(1)).cleanupTopicPartitionState(topic, partition);
      Assert.assertEquals(topicIngestionStatusMap.get(topic).getPartitionIngestionStatus(partition), NOT_EXIST);

      // Case 2: Local remove topic partition executed throws exception.
      topicIngestionStatusMap.get(topic).setPartitionIngestionStatusToLocalIngestion(partition);
      doThrow(new VeniceException("test")).when(backend)
          .removeTopicPartitionLocally(any(), anyInt(), anyInt(), anyBoolean());
      Assert.assertThrows(() -> backend.dropStoragePartitionGracefully(storeVersionConfig, partition, 180, false));
      verify(backend, times(2)).removeTopicPartitionLocally(any(), anyInt(), anyInt(), anyBoolean());
      verify(ingestionRequestClient, times(1)).resetTopicPartition(topic, partition);
      verify(monitorService, times(1)).cleanupTopicPartitionState(topic, partition);
      Assert.assertEquals(topicIngestionStatusMap.get(topic).getPartitionIngestionStatus(partition), MAIN);

      // Case 3: Remote remove topic partition executed successfully.
      topicIngestionStatusMap.get(topic).setPartitionIngestionStatusToIsolatedIngestion(partition);
      when(ingestionRequestClient.removeTopicPartition(topic, partition)).thenReturn(true);
      backend.dropStoragePartitionGracefully(storeVersionConfig, partition, 180, false);
      verify(ingestionRequestClient, times(1)).removeTopicPartition(topic, partition);
      verify(ingestionRequestClient, times(2)).resetTopicPartition(topic, partition);
      verify(monitorService, times(2)).cleanupTopicPartitionState(topic, partition);
      Assert.assertEquals(topicIngestionStatusMap.get(topic).getPartitionIngestionStatus(partition), NOT_EXIST);
    }
  }

  @Test
  public void testHasCurrentVersionBootstrapping() {
    IsolatedIngestionBackend mockBackend = mock(IsolatedIngestionBackend.class);
    MainIngestionMonitorService mockMonitorService = mock(MainIngestionMonitorService.class);

    Map<String, MainTopicIngestionStatus> ingestionStatusMap = new HashMap<>();
    MainTopicIngestionStatus store1V1IngestionStatus = new MainTopicIngestionStatus("store1_v1");
    MainTopicIngestionStatus store1V2IngestionStatus = new MainTopicIngestionStatus("store1_v2");
    MainTopicIngestionStatus store2V2IngestionStatus = new MainTopicIngestionStatus("store2_v2");
    ingestionStatusMap.put("store1_v1", store1V1IngestionStatus);
    ingestionStatusMap.put("store1_v2", store1V2IngestionStatus);
    ingestionStatusMap.put("store2_v2", store2V2IngestionStatus);

    doReturn(ingestionStatusMap).when(mockMonitorService).getTopicIngestionStatusMap();
    store1V1IngestionStatus.setPartitionIngestionStatusToIsolatedIngestion(1);
    store1V1IngestionStatus.setPartitionIngestionStatusToLocalIngestion(2);
    store1V2IngestionStatus.setPartitionIngestionStatusToLocalIngestion(1);
    store1V2IngestionStatus.setPartitionIngestionStatusToLocalIngestion(2);
    store2V2IngestionStatus.setPartitionIngestionStatusToLocalIngestion(1);

    Function<String, Integer> currentVersionSupplier = s -> {
      if (s.equals("store1")) {
        return 1;
      }
      if (s.equals("store2")) {
        return 2;
      }
      return 3;
    };
    doReturn(currentVersionSupplier).when(mockBackend).getCurrentVersionSupplier();
    doReturn(mockMonitorService).when(mockBackend).getMainIngestionMonitorService();

    KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
    doReturn(false).when(mockIngestionService).hasCurrentVersionBootstrapping();
    doReturn(mockIngestionService).when(mockBackend).getStoreIngestionService();

    doCallRealMethod().when(mockBackend).hasCurrentVersionBootstrapping();

    assertTrue(mockBackend.hasCurrentVersionBootstrapping());

    // Move current version ingestion to main process
    store1V1IngestionStatus.setPartitionIngestionStatusToLocalIngestion(1);
    assertFalse(mockBackend.hasCurrentVersionBootstrapping());
  }
}
