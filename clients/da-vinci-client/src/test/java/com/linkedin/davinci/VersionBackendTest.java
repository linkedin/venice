package com.linkedin.davinci;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformerConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.transformer.TestStringRecordTransformer;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VersionBackendTest {
  private static final Random RANDOM = new Random();
  private static final String TEST_STORE_NAME = "test_store";
  private static final int TEST_PARTITION_COUNT = 6;

  private IngestionBackend mockIngestionBackend;
  private InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig;
  private VersionBackend versionBackend;

  @BeforeMethod
  public void setUp() {
    DaVinciBackend mockBackend = mock(DaVinciBackend.class);
    Version version = new VersionImpl(TEST_STORE_NAME, 1);
    version.setPartitionCount(TEST_PARTITION_COUNT);

    File baseDataPath = Utils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .put(ConfigKeys.LOCAL_REGION_NAME, "dc-0")
        .build();
    when(mockBackend.getConfigLoader()).thenReturn(new VeniceConfigLoader(backendConfig));
    when(mockBackend.getStorageService()).thenReturn(mock(StorageService.class));

    mockIngestionBackend = mock(IngestionBackend.class);
    when(mockBackend.getIngestionBackend()).thenReturn(mockIngestionBackend);

    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    when(mockBackend.getStoreRepository()).thenReturn(mockStoreRepository);
    when(mockBackend.getStoreOrThrow(anyString())).thenReturn(mock(StoreBackend.class));
    when(mockBackend.getHeartbeatMonitoringService()).thenReturn(mock(HeartbeatMonitoringService.class));

    ZKStore store = TestUtils.populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(RANDOM.nextLong()),
            Long.toString(RANDOM.nextLong()),
            System.currentTimeMillis()),
        RANDOM);
    when(mockStoreRepository.getStoreOrThrow(TEST_STORE_NAME)).thenReturn(new ReadOnlyStore(store));

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();
    internalRecordTransformerConfig = spy(
        new InternalDaVinciRecordTransformerConfig(
            recordTransformerConfig,
            mock(AggVersionedDaVinciRecordTransformerStats.class)));
    when(mockBackend.getInternalRecordTransformerConfig(TEST_STORE_NAME)).thenReturn(internalRecordTransformerConfig);

    when(mockBackend.getIngestionService()).thenReturn(mock(KafkaStoreIngestionService.class));
    when(mockBackend.getExecutor()).thenReturn(mock(java.util.concurrent.ScheduledExecutorService.class));

    versionBackend = new VersionBackend(mockBackend, version, mock(StoreBackendStats.class));
  }

  @Test
  public void testMaybeReportIncrementalPushStatus() {
    VersionBackend versionBackend = mock(VersionBackend.class);
    Map<Integer, List<String>> partitionToPendingReportIncrementalPushList = new VeniceConcurrentHashMap<>();
    List<String> pendingReportVersionList = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      pendingReportVersionList.add("version_" + i);
    }
    partitionToPendingReportIncrementalPushList.put(0, pendingReportVersionList);

    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    Version version = new VersionImpl("test_store", 1, "dummy");
    doReturn(version).when(versionBackend).getVersion();
    doCallRealMethod().when(versionBackend).maybeReportIncrementalPushStatus(anyInt(), anyString(), any(), any());
    Consumer<String> mockConsumer = mock(Consumer.class);

    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(1)).accept("a");
    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");

    partitionToBatchReportEOIPEnabled.put(0, true);
    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).size(), 50);
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(0), "version_0");
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(49), "version_49");

    versionBackend
        .maybeReportIncrementalPushStatus(0, "a", ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, mockConsumer);
    verify(mockConsumer, times(2)).accept("a");
    Assert.assertTrue(partitionToPendingReportIncrementalPushList.containsKey(0));
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).size(), 50);
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(0), "version_1");
    Assert.assertEquals(partitionToPendingReportIncrementalPushList.get(0).get(49), "a");
  }

  @Test
  public void testMaybeReportBatchEOIPStatus() {
    VersionBackend versionBackend = mock(VersionBackend.class);
    List<String> incPushList = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      incPushList.add("inc_" + i);
    }
    Map<Integer, List<String>> partitionToPendingReportIncrementalPushList = Collections.singletonMap(0, incPushList);
    Map<Integer, Boolean> partitionToBatchReportEOIPEnabled = new VeniceConcurrentHashMap<>();
    partitionToBatchReportEOIPEnabled.put(0, true);
    doReturn(partitionToBatchReportEOIPEnabled).when(versionBackend).getPartitionToBatchReportEOIPEnabled();
    doReturn(partitionToPendingReportIncrementalPushList).when(versionBackend)
        .getPartitionToPendingReportIncrementalPushList();
    doCallRealMethod().when(versionBackend).maybeReportBatchEOIPStatus(anyInt(), any());
    Version version = new VersionImpl("test_store", 1, "dummy");
    doReturn(version).when(versionBackend).getVersion();
    Consumer<String> mockConsumer = mock(Consumer.class);
    versionBackend.maybeReportBatchEOIPStatus(0, mockConsumer);
    verify(mockConsumer, times(50)).accept(anyString());
    verify(mockConsumer, times(1)).accept("inc_0");
    verify(mockConsumer, times(1)).accept("inc_49");
    Assert.assertFalse(partitionToBatchReportEOIPEnabled.get(0));
  }

  @Test
  public void testSendOutHeartBeat() {
    String storeName = "test_store";
    DaVinciBackend backend = mock(DaVinciBackend.class);
    doReturn(true).when(backend).hasCurrentVersionBootstrapping();
    PushStatusStoreWriter mockWriter = mock(PushStatusStoreWriter.class);
    doReturn(mockWriter).when(backend).getPushStatusStoreWriter();

    Version currentVersion = mock(Version.class);
    doReturn(storeName).when(currentVersion).getStoreName();
    doReturn(1).when(currentVersion).getNumber();
    Version futureVersion = mock(Version.class);
    doReturn(storeName).when(futureVersion).getStoreName();
    doReturn(2).when(futureVersion).getNumber();

    VersionBackend.sendOutHeartbeat(backend, currentVersion);
    VersionBackend.sendOutHeartbeat(backend, futureVersion);

    verify(mockWriter, times(2)).writeHeartbeatForBootstrappingInstance(storeName);
    verify(mockWriter, never()).writeHeartbeat(storeName);

    doReturn(false).when(backend).hasCurrentVersionBootstrapping();
    VersionBackend.sendOutHeartbeat(backend, currentVersion);
    VersionBackend.sendOutHeartbeat(backend, futureVersion);

    verify(mockWriter, times(2)).writeHeartbeat(storeName);
  }

  @Test
  public void testRecordTransformerSubscribe() {
    Collection<Integer> partitionList = Arrays.asList(0, 1, 2);
    ComplementSet<Integer> complementSet = ComplementSet.newSet(partitionList);

    // First subscription
    versionBackend.subscribe(complementSet, null, null);

    // Verify the latch count is set to 3 (number of partitions)
    verify(internalRecordTransformerConfig).setStartConsumptionLatchCount(3);

    // Verify consumption started for each partition
    verify(mockIngestionBackend).startConsumption(any(), eq(0), any(), any());
    verify(mockIngestionBackend).startConsumption(any(), eq(1), any(), any());
    verify(mockIngestionBackend).startConsumption(any(), eq(2), any(), any());

    // Reset mocks for next test case
    clearInvocations(internalRecordTransformerConfig);
    clearInvocations(mockIngestionBackend);

    // Test with overlapping partitions
    partitionList = Arrays.asList(2, 3, 4);
    complementSet = ComplementSet.newSet(partitionList);
    versionBackend.subscribe(complementSet, null, null);

    // Shouldn't try to start consumption on already subscribed partition (2)
    verify(mockIngestionBackend, never()).startConsumption(any(), eq(2), any(), any());
    // Should start consumption for new partitions (3, 4)
    verify(mockIngestionBackend).startConsumption(any(), eq(3), any(), any());
    verify(mockIngestionBackend).startConsumption(any(), eq(4), any(), any());
    // Shouldn't set latch count again
    verify(internalRecordTransformerConfig, never()).setStartConsumptionLatchCount(anyInt());

    // Test empty subscription
    versionBackend.subscribe(ComplementSet.emptySet(), null, null);
    verify(mockIngestionBackend, never()).startConsumption(any(), eq(0), any(), any());
    verify(internalRecordTransformerConfig, never()).setStartConsumptionLatchCount(anyInt());
  }

  @Test
  public void testCloseRemovesReplicaState() {
    // Subscribe to partitions
    Collection<Integer> partitionList = Arrays.asList(0, 1, 2);
    ComplementSet<Integer> complementSet = ComplementSet.newSet(partitionList);
    versionBackend.subscribe(complementSet, null, null);

    // Close the version backend
    versionBackend.close();

    // Verify removeReplicaState was called for each subscribed partition
    String topicName = Version.composeKafkaTopic(TEST_STORE_NAME, 1);
    verify(mockIngestionBackend).removeReplicaState(eq(Utils.getReplicaId(topicName, 0)));
    verify(mockIngestionBackend).removeReplicaState(eq(Utils.getReplicaId(topicName, 1)));
    verify(mockIngestionBackend).removeReplicaState(eq(Utils.getReplicaId(topicName, 2)));
  }

  @Test
  public void testPushStatusDisabledForVersionSpecificClient() {
    DaVinciBackend mockDaVinciBackend = mock(DaVinciBackend.class);
    String storeName = "test_store";
    Version version = new VersionImpl(storeName, 1);
    version.setPartitionCount(3);

    File baseDataPath = Utils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .put(ConfigKeys.LOCAL_REGION_NAME, "dc-0")
        .put(ConfigKeys.PUSH_STATUS_STORE_ENABLED, true)
        .build();
    VeniceConfigLoader veniceConfigLoader = new VeniceConfigLoader(backendConfig);
    when(mockDaVinciBackend.getConfigLoader()).thenReturn(veniceConfigLoader);
    when(mockDaVinciBackend.getStorageService()).thenReturn(mock(StorageService.class));
    when(mockDaVinciBackend.getIngestionBackend()).thenReturn(mock(IngestionBackend.class));

    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    when(mockDaVinciBackend.getStoreRepository()).thenReturn(mockStoreRepository);

    StoreBackend mockStoreBackend = mock(StoreBackend.class);
    when(mockDaVinciBackend.getStoreOrThrow(anyString())).thenReturn(mockStoreBackend);

    HeartbeatMonitoringService mockHeartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    when(mockDaVinciBackend.getHeartbeatMonitoringService()).thenReturn(mockHeartbeatMonitoringService);
    when(mockDaVinciBackend.getIngestionService()).thenReturn(mock(KafkaStoreIngestionService.class));
    when(mockDaVinciBackend.getExecutor()).thenReturn(mock(java.util.concurrent.ScheduledExecutorService.class));

    Store store = mock(Store.class);
    when(store.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(store.getName()).thenReturn(storeName);
    when(mockStoreRepository.getStoreOrThrow(storeName)).thenReturn(store);

    // Version-specific client should have push status disabled
    when(mockDaVinciBackend.getStoreClientType(storeName)).thenReturn(DaVinciBackend.ClientType.VERSION_SPECIFIC);
    VersionBackend versionSpecificBackend =
        new VersionBackend(mockDaVinciBackend, version, mock(StoreBackendStats.class));
    assertFalse(versionSpecificBackend.isReportingPushStatus());

    // Regular client should have push status enabled
    when(mockDaVinciBackend.getStoreClientType(storeName)).thenReturn(DaVinciBackend.ClientType.REGULAR);
    VersionBackend regularBackend = new VersionBackend(mockDaVinciBackend, version, mock(StoreBackendStats.class));
    assertTrue(regularBackend.isReportingPushStatus());
  }
}
