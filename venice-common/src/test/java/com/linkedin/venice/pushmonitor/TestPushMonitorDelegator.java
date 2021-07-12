package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.MetadataStoreWriter;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class TestPushMonitorDelegator {
  private PushMonitorType pushMonitorType = PushMonitorType.WRITE_COMPUTE_STORE;
  private String clusterName = "test-cluster";
  private RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
  private OfflinePushAccessor offlinePushAccessor = Mockito.mock(OfflinePushAccessor.class);
  private StoreCleaner storeCleaner = Mockito.mock(StoreCleaner.class);
  private ReadWriteStoreRepository metadataRepo = Mockito.mock(ReadWriteStoreRepository.class);
  private AggPushHealthStats aggPushHealthStats = Mockito.mock(AggPushHealthStats.class);
  private MetadataStoreWriter metadataStoreWriter = Mockito.mock(MetadataStoreWriter.class);
  private ClusterLockManager clusterLockManager = Mockito.mock(ClusterLockManager.class);
  private String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";

  @Test
  public void testDelegatorCanCleanupLegacyStatus() {
    PushMonitorDelegator delegator = new PushMonitorDelegator(pushMonitorType, clusterName, routingDataRepository,
        offlinePushAccessor, storeCleaner, metadataRepo, aggPushHealthStats, false,
        Optional.empty(), Optional.empty(), metadataStoreWriter, clusterLockManager, aggregateRealTimeSourceKafkaUrl, Collections.emptyList());

    OfflinePushStatus legacyStatus = new OfflinePushStatus("legacy_v1", 1, 1,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Mockito.doReturn(Arrays.asList(legacyStatus)).when(offlinePushAccessor).loadOfflinePushStatusesAndPartitionStatuses();

    delegator.loadAllPushes();
    Mockito.verify(offlinePushAccessor).deleteOfflinePushStatusAndItsPartitionStatuses(legacyStatus.getKafkaTopic());
  }
}
