package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HeartbeatMonitoringServiceTest {
  private static final String LOCAL_FABRIC = "local";
  private static final String REMOTE_FABRIC = "remote";

  private static final String TEST_STORE = "Vivaldi_store";

  @Test
  public void testAddLeaderLagMonitor() {

    // Default hybrid store config
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(1L, 1L, 1L, DataReplicationPolicy.ACTIVE_ACTIVE, BufferReplayPolicy.REWIND_FROM_SOP);

    // Version configs
    Version backupVersion = new VersionImpl(TEST_STORE, 1, "1"); // Non hybrid version
    Version currentVersion = new VersionImpl(TEST_STORE, 2, "2"); // hybrid version, active/active
    Version futureVersion = new VersionImpl(TEST_STORE, 3, "3"); // hybrid version, non AA
    currentVersion.setHybridStoreConfig(hybridStoreConfig);
    futureVersion.setHybridStoreConfig(hybridStoreConfig);

    currentVersion.setActiveActiveReplicationEnabled(true);

    Store mockStore = Mockito.mock(Store.class);
    Mockito.when(mockStore.getName()).thenReturn(TEST_STORE);
    Mockito.when(mockStore.getCurrentVersion()).thenReturn(currentVersion.getNumber());
    Mockito.when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    Mockito.when(mockStore.getVersion(1)).thenReturn(Optional.of(backupVersion));
    Mockito.when(mockStore.getVersion(2)).thenReturn(Optional.of(currentVersion));
    Mockito.when(mockStore.getVersion(3)).thenReturn(Optional.of(futureVersion));

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    Mockito.when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);
    regions.add(REMOTE_FABRIC);
    HeartbeatMonitoringService heartbeatMonitoringService =
        new HeartbeatMonitoringService(mockMetricsRepository, mockReadOnlyRepository, regions, LOCAL_FABRIC);

    // Let's emit some heartbeats that don't exist in the registry yet
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, 1000L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, 1000L);
    // and throw a null at it too for good measure
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, null, 1000L);

    // Since we haven't gotten a signal to handle these heartbeats, we discard them.
    Assert.assertNull(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE));
    Assert.assertNull(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE));

    // Let's do some state transitions!

    // Follower state transitions
    heartbeatMonitoringService.addFollowerLagMonitor(currentVersion, 0);
    heartbeatMonitoringService.addFollowerLagMonitor(currentVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(currentVersion, 2);

    heartbeatMonitoringService.addFollowerLagMonitor(backupVersion, 0);
    heartbeatMonitoringService.addFollowerLagMonitor(backupVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(backupVersion, 2);

    heartbeatMonitoringService.addFollowerLagMonitor(futureVersion, 0);
    heartbeatMonitoringService.addFollowerLagMonitor(futureVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(futureVersion, 2);

    // Follower heartbeats
    // local fabric heartbeats
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 0, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 2, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 2, LOCAL_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 2, LOCAL_FABRIC, 1001L);

    // remote fabric heartbeats
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 0, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 0, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, REMOTE_FABRIC, 1001L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 3, REMOTE_FABRIC, 1001L);

    // bogus heartbeats
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, 1002L);
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 3, 0, LOCAL_FABRIC, 1002L);

    Assert.assertEquals(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().size(), 1);
    // We only expect two entries as version 1 is a non-hybrid version
    Assert.assertEquals(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).size(), 2);
    Assert.assertEquals(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(2).size(), 3);

    // Check we got the right amount of regions
    Assert.assertEquals(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(3).get(0).size(),
        2);

    // make sure we didn't get any leader heartbeats yet recorded
    Assert.assertNull(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE));

    // check heartbeat value
    Long value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(futureVersion.getNumber())
        .get(1)
        .get(LOCAL_FABRIC);
    Assert.assertTrue(value == 1001L);

    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(futureVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC);
    Assert.assertTrue(value == 1001L);

    // Leader state transitions
    heartbeatMonitoringService.addLeaderLagMonitor(currentVersion, 1);
    heartbeatMonitoringService.addLeaderLagMonitor(currentVersion, 2);
    heartbeatMonitoringService.addLeaderLagMonitor(backupVersion, 1);
    heartbeatMonitoringService.addLeaderLagMonitor(backupVersion, 2);
    heartbeatMonitoringService.addLeaderLagMonitor(futureVersion, 1);
    heartbeatMonitoringService.addLeaderLagMonitor(futureVersion, 2);

    // alright, no longer null
    Assert.assertNotNull(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE));

    // make sure the follower entries are no longer there
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(1));
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(2));

    // Non hybrid version shouldn't be recorded
    Assert.assertNull(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));

    // Go back to follower
    heartbeatMonitoringService.addFollowerLagMonitor(currentVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(backupVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(futureVersion, 1);

    // make sure non hybrid is still not in there
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));
    Assert.assertNull(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));

    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, REMOTE_FABRIC, 1003L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, REMOTE_FABRIC, 1003L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, REMOTE_FABRIC, 1003L);

    // make sure leaders are cleared out
    Assert.assertNull(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(1));
    Assert.assertNull(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(futureVersion.getNumber())
            .get(1));

    // make sure followers are added
    Assert.assertNotNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(1));
    Assert.assertNotNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(futureVersion.getNumber())
            .get(1));

    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(futureVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC);
    Assert.assertTrue(value == 1003L);
    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(currentVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC);
    Assert.assertTrue(value == 1003L);

    // Drop/Error some
    heartbeatMonitoringService.removeLagMonitor(currentVersion, 0);
    heartbeatMonitoringService.removeLagMonitor(futureVersion, 1);
    heartbeatMonitoringService.removeLagMonitor(backupVersion, 2);

    // Send heartbeats to resources we just dropped
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, backupVersion.getNumber(), 2, LOCAL_FABRIC, 1005L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, currentVersion.getNumber(), 0, LOCAL_FABRIC, 1005L);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, futureVersion.getNumber(), 1, LOCAL_FABRIC, 1005L);

    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(0));
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(futureVersion.getNumber())
            .get(1));

    heartbeatMonitoringService.record();
  }
}
