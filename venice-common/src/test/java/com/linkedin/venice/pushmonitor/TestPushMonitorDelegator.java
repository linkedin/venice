package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Collections;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class TestPushMonitorDelegator {
  private final PushMonitorType pushMonitorType = PushMonitorType.WRITE_COMPUTE_STORE;
  private final RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
  private final OfflinePushAccessor offlinePushAccessor = Mockito.mock(OfflinePushAccessor.class);
  private final StoreCleaner storeCleaner = Mockito.mock(StoreCleaner.class);
  private final ReadWriteStoreRepository metadataRepo = Mockito.mock(ReadWriteStoreRepository.class);
  private final AggPushHealthStats aggPushHealthStats = Mockito.mock(AggPushHealthStats.class);
  private final ClusterLockManager clusterLockManager = Mockito.mock(ClusterLockManager.class);

  @Test
  public void testDelegatorCanCleanupLegacyStatus() {
    String clusterName = "test-cluster";
    String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";
    PushMonitorDelegator delegator = new PushMonitorDelegator(pushMonitorType, clusterName, routingDataRepository,
        offlinePushAccessor, storeCleaner, metadataRepo, aggPushHealthStats, Optional.empty(), Optional.empty(),
        clusterLockManager, aggregateRealTimeSourceKafkaUrl, Collections.emptyList());

    OfflinePushStatus legacyStatus = new OfflinePushStatus("legacy_v1", 1, 1,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Mockito.doReturn(Collections.singletonList(legacyStatus)).when(offlinePushAccessor).loadOfflinePushStatusesAndPartitionStatuses();

    delegator.loadAllPushes();
    Mockito.verify(offlinePushAccessor).deleteOfflinePushStatusAndItsPartitionStatuses(legacyStatus.getKafkaTopic());
  }
}
