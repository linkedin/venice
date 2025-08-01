package com.linkedin.davinci;

import static org.apache.kafka.test.TestUtils.RANDOM;
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

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformerConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.transformer.TestStringRecordTransformer;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.meta.ReadOnlyStore;
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
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VersionBackendTest {
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
    DaVinciBackend mockDaVinciBackend = mock(DaVinciBackend.class);
    String storeName = "test_store";
    Version version = new VersionImpl(storeName, 1);
    int partitionCount = 6;
    version.setPartitionCount(partitionCount);
    StoreBackendStats mockStoreBackendStats = mock(StoreBackendStats.class);

    File baseDataPath = Utils.getTempDataDirectory();
    VeniceProperties backendConfig = new PropertyBuilder().put(ConfigKeys.CLUSTER_NAME, "test-cluster")
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, "test-zookeeper")
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "test-kafka")
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath.getAbsolutePath())
        .put(ConfigKeys.LOCAL_REGION_NAME, "dc-0")
        .build();
    VeniceConfigLoader veniceConfigLoader = new VeniceConfigLoader(backendConfig);
    when(mockDaVinciBackend.getConfigLoader()).thenReturn(veniceConfigLoader);

    StorageService mockStorageService = mock(StorageService.class);
    when(mockDaVinciBackend.getStorageService()).thenReturn(mockStorageService);

    IngestionBackend mockIngestionBackend = mock(IngestionBackend.class);
    when(mockDaVinciBackend.getIngestionBackend()).thenReturn(mockIngestionBackend);

    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    when(mockDaVinciBackend.getStoreRepository()).thenReturn(mockStoreRepository);

    StoreBackend mockStoreBackend = mock(StoreBackend.class);
    when(mockDaVinciBackend.getStoreOrThrow(anyString())).thenReturn(mockStoreBackend);

    ZKStore store = TestUtils.populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(RANDOM.nextLong()),
            Long.toString(RANDOM.nextLong()),
            System.currentTimeMillis()),
        RANDOM);
    ReadOnlyStore readOnlyStore = new ReadOnlyStore(store);
    when(mockStoreRepository.getStoreOrThrow(storeName)).thenReturn(readOnlyStore);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig = spy(
        new InternalDaVinciRecordTransformerConfig(
            recordTransformerConfig,
            mock(AggVersionedDaVinciRecordTransformerStats.class)));
    when(mockDaVinciBackend.getInternalRecordTransformerConfig(storeName)).thenReturn(internalRecordTransformerConfig);

    VersionBackend versionBackend = new VersionBackend(mockDaVinciBackend, version, mockStoreBackendStats);

    Collection<Integer> partitionList = Arrays.asList(0, 1, 2);
    ComplementSet<Integer> complementSet = ComplementSet.newSet(partitionList);

    versionBackend.subscribe(complementSet);
    verify(internalRecordTransformerConfig).setStartConsumptionLatchCount(3);
    verify(mockIngestionBackend).startConsumption(any(), eq(0));
    verify(mockIngestionBackend).startConsumption(any(), eq(1));
    verify(mockIngestionBackend).startConsumption(any(), eq(2));

    clearInvocations(internalRecordTransformerConfig);
    clearInvocations(mockIngestionBackend);
    partitionList = Arrays.asList(2, 3, 4);
    complementSet = ComplementSet.newSet(partitionList);
    versionBackend.subscribe(complementSet);
    // Shouldn't try to start consumption on already subscribed partitions
    verify(mockIngestionBackend, never()).startConsumption(any(), eq(2));
    verify(mockIngestionBackend).startConsumption(any(), eq(3));
    verify(mockIngestionBackend).startConsumption(any(), eq(4));
    verify(internalRecordTransformerConfig, never()).setStartConsumptionLatchCount(anyInt());
  }
}
