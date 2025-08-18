package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatStatReporter.CATCHUP_UP_FOLLOWER_METRIC_PREFIX;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatStatReporter.FOLLOWER_METRIC_PREFIX;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatStatReporter.LEADER_METRIC_PREFIX;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatStatReporter.MAX;
import static com.linkedin.venice.utils.Utils.SEPARATE_TOPIC_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HeartbeatMonitoringServiceTest {
  private static final String LOCAL_FABRIC = "local";
  private static final String REMOTE_FABRIC = "remote";

  private static final String TEST_STORE = "Vivaldi_store";

  @Test
  public void testGetHeartbeatInfo() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    doCallRealMethod().when(heartbeatMonitoringService).getHeartbeatInfo(anyString(), anyInt(), anyBoolean());
    heartbeatMonitoringService.getHeartbeatInfo("", -1, false);
    Mockito.verify(heartbeatMonitoringService, times(2))
        .getHeartbeatInfoFromMap(any(), anyString(), anyLong(), anyString(), anyInt(), anyBoolean());
  }

  @Test
  public void testGetHeartbeatLag() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaLeaderMaxHeartbeatLag(any(), anyString(), anyInt(), anyBoolean());
    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaLeaderMinHeartbeatTimestamp(any(), anyString(), anyInt(), anyBoolean());
    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaLeaderMaxHeartbeatLag(any(), anyString(), anyInt(), anyBoolean(), anyLong());

    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaFollowerHeartbeatLag(any(), anyString(), anyInt(), anyBoolean());
    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaFollowerHeartbeatTimestamp(any(), anyString(), anyInt(), anyBoolean());
    doCallRealMethod().when(heartbeatMonitoringService)
        .getReplicaFollowerHeartbeatLag(any(), anyString(), anyInt(), anyBoolean(), anyLong());

    Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMap =
        new VeniceConcurrentHashMap<>();
    Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMap =
        new VeniceConcurrentHashMap<>();
    doReturn(leaderMap).when(heartbeatMonitoringService).getLeaderHeartbeatTimeStamps();
    doReturn(followerMap).when(heartbeatMonitoringService).getFollowerHeartbeatTimeStamps();
    doReturn("dc-1").when(heartbeatMonitoringService).getLocalRegionName();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);

    // Validating Leader Lag
    String store = "testStore";
    int version = 1;
    int partition = 1;
    doReturn("store_v1-1").when(pcs).getReplicaId();
    doReturn(partition).when(pcs).getPartition();
    leaderMap.put(store, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).put(version, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).get(version).put(partition, new VeniceConcurrentHashMap<>());
    long currentTime = System.currentTimeMillis();
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-0", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(5), true, true));
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, true));
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1_sep", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(100), true, true));

    // Check valid leader lag
    long lag = heartbeatMonitoringService.getReplicaLeaderMaxHeartbeatLag(pcs, store, version, true);
    Assert.assertTrue(lag >= TimeUnit.MINUTES.toMillis(10));
    Assert.assertTrue(lag < TimeUnit.MINUTES.toMillis(11));
    long timestamp = heartbeatMonitoringService.getReplicaLeaderMinHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, currentTime - TimeUnit.MINUTES.toMillis(10));

    // Add unavailable region
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-2", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(20), false, false));
    lag = heartbeatMonitoringService.getReplicaLeaderMaxHeartbeatLag(pcs, store, version, true);
    Assert.assertEquals(lag, Long.MAX_VALUE);
    timestamp = heartbeatMonitoringService.getReplicaLeaderMinHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, HeartbeatMonitoringService.INVALID_MESSAGE_TIMESTAMP);
    // Replica not found in leader map.
    lag = heartbeatMonitoringService.getReplicaLeaderMaxHeartbeatLag(pcs, store, 2, true);
    Assert.assertEquals(lag, Long.MAX_VALUE);
    timestamp = heartbeatMonitoringService.getReplicaLeaderMinHeartbeatTimestamp(pcs, store, 2, true);
    Assert.assertEquals(timestamp, HeartbeatMonitoringService.INVALID_MESSAGE_TIMESTAMP);

    /**
     * Validating Follower Lag
     */
    followerMap.put(store, new VeniceConcurrentHashMap<>());
    followerMap.get(store).put(version, new VeniceConcurrentHashMap<>());
    followerMap.get(store).get(version).put(partition, new VeniceConcurrentHashMap<>());
    followerMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, true));

    // Check valid follower lag
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, version, true);
    Assert.assertTrue(lag >= TimeUnit.MINUTES.toMillis(10));
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, currentTime - TimeUnit.MINUTES.toMillis(10));

    // Add unrelated region
    followerMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-0", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(20), true, true));
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, version, true);
    Assert.assertTrue(lag >= TimeUnit.MINUTES.toMillis(10));
    Assert.assertTrue(lag < TimeUnit.MINUTES.toMillis(20));
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, currentTime - TimeUnit.MINUTES.toMillis(10));
    // Set local region lag to be invalid
    followerMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1", new HeartbeatTimeStampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, false));
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, version, true);
    Assert.assertEquals(lag, Long.MAX_VALUE);
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, HeartbeatMonitoringService.INVALID_MESSAGE_TIMESTAMP);
    // Replica not found in follower map.
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, 2, true);
    Assert.assertEquals(lag, Long.MAX_VALUE);
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, 2, true);
    Assert.assertEquals(timestamp, HeartbeatMonitoringService.INVALID_MESSAGE_TIMESTAMP);

  }

  @Test
  public void testGetHeartbeatInfoFromMap() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    doCallRealMethod().when(heartbeatMonitoringService)
        .getHeartbeatInfoFromMap(anyMap(), anyString(), anyLong(), anyString(), anyInt(), anyBoolean());
    Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMap =
        new VeniceConcurrentHashMap<>();
    String store = "testStore";
    int version = 1;
    int partition = 1;
    String region = "dc-0";
    long timestamp = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
    leaderMap.put(store, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).put(version, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).get(version).put(partition, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).get(version).get(partition).put(region, new HeartbeatTimeStampEntry(timestamp, true, true));
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                -1,
                false)
            .size(),
        1);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                -1,
                true)
            .size(),
        0);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                -1,
                false)
            .size(),
        1);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                1,
                false)
            .size(),
        1);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                2,
                false)
            .size(),
        0);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, 2),
                -1,
                false)
            .size(),
        0);
    Assert.assertEquals(
        heartbeatMonitoringService
            .getHeartbeatInfoFromMap(
                leaderMap,
                LeaderFollowerStateType.LEADER.name(),
                System.currentTimeMillis(),
                Version.composeKafkaTopic(store, version),
                1,
                false)
            .size(),
        1);

  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAddLeaderLagMonitor(boolean enableSepRT) {

    // Default hybrid store config
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    // Version configs
    Version backupVersion = new VersionImpl(TEST_STORE, 1, "1"); // Non-hybrid version
    Version currentVersion = new VersionImpl(TEST_STORE, 2, "2"); // hybrid version, active/active
    Version futureVersion = new VersionImpl(TEST_STORE, 3, "3"); // hybrid version, non AA
    currentVersion.setHybridStoreConfig(hybridStoreConfig);
    futureVersion.setHybridStoreConfig(hybridStoreConfig);

    currentVersion.setActiveActiveReplicationEnabled(true);
    if (enableSepRT) {
      currentVersion.setSeparateRealTimeTopicEnabled(true);
    }

    Store mockStore = mock(Store.class);
    Mockito.when(mockStore.getName()).thenReturn(TEST_STORE);
    Mockito.when(mockStore.getCurrentVersion()).thenReturn(currentVersion.getNumber());
    Mockito.when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    Mockito.when(mockStore.getVersion(1)).thenReturn(backupVersion);
    Mockito.when(mockStore.getVersion(2)).thenReturn(currentVersion);
    Mockito.when(mockStore.getVersion(3)).thenReturn(futureVersion);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    HelixCustomizedViewOfflinePushRepository mockCustomizedViewOfflinePushRepository =
        mock(HelixCustomizedViewOfflinePushRepository.class);
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    Mockito.when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);
    regions.add(REMOTE_FABRIC);
    regions.add(REMOTE_FABRIC + SEPARATE_TOPIC_SUFFIX);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(regions).when(serverConfig).getRegionNames();
    doReturn(LOCAL_FABRIC).when(serverConfig).getRegionName();
    doReturn(Duration.ofSeconds(5)).when(serverConfig).getServerMaxWaitForVersionInfo();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        CompletableFuture.completedFuture(mockCustomizedViewOfflinePushRepository));

    // Let's emit some heartbeats that don't exist in the registry yet
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, 1000L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, 1000L, true);
    // and throw a null at it too for good measure
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, null, 1000L, true);

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

    // The above calls initialize entries with current time, and followers will retain the highest timestamp.
    // we'll note the current time that comes AFTER the initialization and use that from which to increment the time.
    long baseTimeStamp = System.currentTimeMillis();

    // Follower heartbeats
    // local fabric heartbeats
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 0, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 2, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 2, LOCAL_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 2, LOCAL_FABRIC, baseTimeStamp + 1001L, true);

    // remote fabric heartbeats
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 0, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 0, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, REMOTE_FABRIC, baseTimeStamp + 1001L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 3, REMOTE_FABRIC, baseTimeStamp + 1001L, true);

    // bogus heartbeats
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, baseTimeStamp + 1002L, true);
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 3, 0, LOCAL_FABRIC, baseTimeStamp + 1002L, true);

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
        .get(LOCAL_FABRIC).timestamp;
    Assert.assertTrue(value >= baseTimeStamp + 1001L);

    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(futureVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC).timestamp;
    Assert.assertTrue(value >= baseTimeStamp + 1001L);

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

    Assert.assertEquals(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps()
            .get(TEST_STORE)
            .get(currentVersion.getNumber())
            .get(1)
            .size(),
        2 + (enableSepRT ? 1 : 0));

    // Go back to follower
    heartbeatMonitoringService.addFollowerLagMonitor(currentVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(backupVersion, 1);
    heartbeatMonitoringService.addFollowerLagMonitor(futureVersion, 1);

    // make sure non hybrid is still not in there
    Assert.assertNull(
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));
    Assert.assertNull(
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(backupVersion.getNumber()));

    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 1, REMOTE_FABRIC, baseTimeStamp + 1003L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 1, REMOTE_FABRIC, baseTimeStamp + 1003L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 1, REMOTE_FABRIC, baseTimeStamp + 1003L, true);

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
        .get(REMOTE_FABRIC).timestamp;
    Assert.assertEquals((long) value, baseTimeStamp + 1003L);
    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(currentVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC).timestamp;
    Assert.assertEquals((long) value, baseTimeStamp + 1003L);

    // Drop/Error some
    heartbeatMonitoringService.removeLagMonitor(currentVersion, 0);
    heartbeatMonitoringService.removeLagMonitor(futureVersion, 1);
    heartbeatMonitoringService.removeLagMonitor(backupVersion, 2);

    // Send heartbeats to resources we just dropped
    heartbeatMonitoringService
        .recordFollowerHeartbeat(TEST_STORE, backupVersion.getNumber(), 2, LOCAL_FABRIC, baseTimeStamp + 1005L, true);
    heartbeatMonitoringService
        .recordFollowerHeartbeat(TEST_STORE, currentVersion.getNumber(), 0, LOCAL_FABRIC, baseTimeStamp + 1005L, true);
    heartbeatMonitoringService
        .recordFollowerHeartbeat(TEST_STORE, futureVersion.getNumber(), 1, LOCAL_FABRIC, baseTimeStamp + 1005L, true);

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

  @Test
  public void testHeartbeatReporter() {
    MetricsRepository repository = new MetricsRepository();
    String regionName = "dc-0";
    Set<String> regionSet = new HashSet<>();
    regionSet.add(regionName);
    regionSet.add(regionName + SEPARATE_TOPIC_SUFFIX);
    String storeName = "abc";
    HeartbeatStatReporter heartbeatStatReporter = new HeartbeatStatReporter(repository, storeName, regionSet);
    // Leader should not register separate region metric.

    String leaderMetricName = "." + storeName + "--" + LEADER_METRIC_PREFIX + regionName + MAX + ".Gauge";
    String leaderMetricNameForSepRT = "." + storeName + "--" + LEADER_METRIC_PREFIX + regionName + MAX + ".Gauge";
    Assert.assertTrue(heartbeatStatReporter.getMetricsRepository().metrics().containsKey(leaderMetricName));
    Assert.assertTrue(heartbeatStatReporter.getMetricsRepository().metrics().containsKey(leaderMetricNameForSepRT));
    // Follower should not register separate region metric.
    String followerMetricName = "." + storeName + "--" + FOLLOWER_METRIC_PREFIX + regionName + MAX + ".Gauge";
    String followerMetricNameForSepRT =
        "." + storeName + "--" + FOLLOWER_METRIC_PREFIX + regionName + SEPARATE_TOPIC_SUFFIX + MAX + ".Gauge";
    Assert.assertTrue(heartbeatStatReporter.getMetricsRepository().metrics().containsKey(followerMetricName));
    Assert.assertFalse(heartbeatStatReporter.getMetricsRepository().metrics().containsKey(followerMetricNameForSepRT));

    // Catching-Up Follower should not register separate region metric.
    String catchingUpFollowerMetricName =
        "." + storeName + "--" + CATCHUP_UP_FOLLOWER_METRIC_PREFIX + regionName + MAX + ".Gauge";
    String catchingUpFollowerMetricNameForSepRT = "." + storeName + "--" + CATCHUP_UP_FOLLOWER_METRIC_PREFIX
        + regionName + SEPARATE_TOPIC_SUFFIX + MAX + ".Gauge";
    Assert.assertTrue(heartbeatStatReporter.getMetricsRepository().metrics().containsKey(catchingUpFollowerMetricName));
    Assert.assertFalse(
        heartbeatStatReporter.getMetricsRepository().metrics().containsKey(catchingUpFollowerMetricNameForSepRT));
  }

  @Test
  public void testUpdateLagMonitor() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    doCallRealMethod().when(heartbeatMonitoringService).updateLagMonitor(anyString(), anyInt(), any());
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(metadataRepo).when(heartbeatMonitoringService).getMetadataRepository();
    Store store = mock(Store.class);
    Version version = mock(Version.class);

    String storeName = "foo";
    int storeVersion = 1;
    int partition = 256;
    String resourceName = Version.composeKafkaTopic(storeName, storeVersion);

    // 1. Test when both store and version are null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(new StoreVersionInfo(null, null));
    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    verify(metadataRepo).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addLeaderLagMonitor(any(Version.class), anyInt());

    heartbeatMonitoringService
        .updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    verify(metadataRepo, times(2)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addFollowerLagMonitor(any(Version.class), anyInt());

    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.REMOVE_MONITOR);
    verify(metadataRepo, times(3)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).removeLagMonitor(any(Version.class), anyInt());

    // 2. Test when store is not null and version is null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(new StoreVersionInfo(store, null));
    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    verify(metadataRepo, times(4)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addLeaderLagMonitor(any(Version.class), anyInt());

    heartbeatMonitoringService
        .updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    verify(metadataRepo, times(5)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, never()).addFollowerLagMonitor(any(Version.class), anyInt());

    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.REMOVE_MONITOR);
    verify(metadataRepo, times(6)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService).removeLagMonitor(any(Version.class), anyInt());

    // 3. Test both store and version are not null
    when(metadataRepo.waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong()))
        .thenReturn(new StoreVersionInfo(store, version));
    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    verify(metadataRepo, times(7)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService).addLeaderLagMonitor(version, partition);

    heartbeatMonitoringService
        .updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    verify(metadataRepo, times(8)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService).addFollowerLagMonitor(version, partition);

    heartbeatMonitoringService.updateLagMonitor(resourceName, partition, HeartbeatLagMonitorAction.REMOVE_MONITOR);
    verify(metadataRepo, times(9)).waitVersion(eq(storeName), eq(storeVersion), any(Duration.class), anyLong());
    verify(heartbeatMonitoringService, times(2)).removeLagMonitor(any(Version.class), anyInt());
  }

  @Test
  public void testCleanupLagMonitor() {
    // Default hybrid store config
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    // Version configs
    Version backupVersion = new VersionImpl(TEST_STORE, 1, "1");
    backupVersion.setHybridStoreConfig(hybridStoreConfig);
    backupVersion.setActiveActiveReplicationEnabled(true);
    Version currentVersion = new VersionImpl(TEST_STORE, 2, "2");
    currentVersion.setHybridStoreConfig(hybridStoreConfig);
    currentVersion.setActiveActiveReplicationEnabled(true);

    Store mockStore = mock(Store.class);
    Mockito.when(mockStore.getName()).thenReturn(TEST_STORE);
    Mockito.when(mockStore.getCurrentVersion()).thenReturn(currentVersion.getNumber());
    Mockito.when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    Mockito.when(mockStore.getVersion(1)).thenReturn(backupVersion);
    Mockito.when(mockStore.getVersion(2)).thenReturn(currentVersion);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    Mockito.when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    doReturn(new StoreVersionInfo(mockStore, backupVersion)).when(mockReadOnlyRepository)
        .waitVersion(eq(TEST_STORE), eq(1), any(), anyLong());
    doReturn(new StoreVersionInfo(mockStore, currentVersion)).when(mockReadOnlyRepository)
        .waitVersion(eq(TEST_STORE), eq(2), any(), anyLong());
    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);
    regions.add(REMOTE_FABRIC);
    regions.add(REMOTE_FABRIC + SEPARATE_TOPIC_SUFFIX);
    String hostname = "localhost";
    int port = 123;
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(regions).when(serverConfig).getRegionNames();
    doReturn(LOCAL_FABRIC).when(serverConfig).getRegionName();
    doReturn(Duration.ofSeconds(5)).when(serverConfig).getServerMaxWaitForVersionInfo();
    doReturn(hostname).when(serverConfig).getListenerHostname();
    doReturn(port).when(serverConfig).getListenerPort();
    String backupVersionTopic = Version.composeKafkaTopic(mockStore.getName(), 1);
    String currentVersionTopic = Version.composeKafkaTopic(mockStore.getName(), 2);

    HelixCustomizedViewOfflinePushRepository mockCustomizedViewOfflinePushRepository =
        mock(HelixCustomizedViewOfflinePushRepository.class);
    // Mock CV repository so that the test node has assignment for all partitions except p1 of v1 and p1 of v2
    Instance thisInstance = Instance.fromNodeId(hostname + "_" + port);
    Instance otherInstance = Instance.fromNodeId("otherInstance_321");
    Set<Instance> instancesWithThisNode = new HashSet<>();
    instancesWithThisNode.add(thisInstance);
    instancesWithThisNode.add(otherInstance);
    Set<Instance> instancesWithoutThisNode = new HashSet<>();
    instancesWithoutThisNode.add(otherInstance);
    PartitionAssignment mockPartitionAssignment = mock(PartitionAssignment.class);
    Partition mockPartition1 = mock(Partition.class);
    doReturn(instancesWithoutThisNode).when(mockPartition1).getAllInstancesSet();
    Partition mockPartition0And2 = mock(Partition.class);
    doReturn(instancesWithThisNode).when(mockPartition0And2).getAllInstancesSet();
    doReturn(mockPartition0And2).when(mockPartitionAssignment).getPartition(0);
    doReturn(mockPartition1).when(mockPartitionAssignment).getPartition(1);
    doReturn(mockPartition0And2).when(mockPartitionAssignment).getPartition(2);
    doReturn(mockPartitionAssignment).when(mockCustomizedViewOfflinePushRepository)
        .getPartitionAssignments(backupVersionTopic);
    doReturn(mockPartitionAssignment).when(mockCustomizedViewOfflinePushRepository)
        .getPartitionAssignments(currentVersionTopic);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);
    // Initialize lag monitor for leader of p0 and follower of p1 and p2 for v1
    heartbeatMonitoringService.updateLagMonitor(backupVersionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    heartbeatMonitoringService.updateLagMonitor(backupVersionTopic, 1, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    heartbeatMonitoringService.updateLagMonitor(backupVersionTopic, 2, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    // Initialize lag monitor for leader of p1 and follower of p0 and p2 for v2
    heartbeatMonitoringService.updateLagMonitor(currentVersionTopic, 0, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    heartbeatMonitoringService.updateLagMonitor(currentVersionTopic, 1, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    heartbeatMonitoringService.updateLagMonitor(currentVersionTopic, 2, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    // No-op before cv repository is ready
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    Mockito.verify(mockCustomizedViewOfflinePushRepository, never()).getPartitionAssignments(anyString());
    Assert.assertTrue(heartbeatMonitoringService.getCleanupHeartbeatMap().isEmpty());
    mockCVRepositoryFuture.complete(mockCustomizedViewOfflinePushRepository);
    Assert.assertEquals(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).size(), 1);
    Assert.assertEquals(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(2).size(), 1);
    Assert.assertEquals(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(1).size(), 2);
    Assert.assertEquals(heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(1).size(), 2);
    // 2 topic partitions should be marked for cleanup
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    Assert.assertEquals(heartbeatMonitoringService.getCleanupHeartbeatMap().size(), 2);
    Assert.assertEquals(
        heartbeatMonitoringService.getCleanupHeartbeatMap().get(Utils.getReplicaId(backupVersionTopic, 1)).intValue(),
        1);
    Assert.assertEquals(
        heartbeatMonitoringService.getCleanupHeartbeatMap().get(Utils.getReplicaId(currentVersionTopic, 1)).intValue(),
        1);
    // Mimic the cleanup cycle 3 more times and a new assignment for p1 of v1
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    heartbeatMonitoringService.updateLagMonitor(backupVersionTopic, 1, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    Assert.assertEquals(heartbeatMonitoringService.getCleanupHeartbeatMap().size(), 1);
    Assert.assertEquals(
        heartbeatMonitoringService.getCleanupHeartbeatMap().get(Utils.getReplicaId(currentVersionTopic, 1)).intValue(),
        4);
    // One more cycle and p1 of v2 should be cleaned up. p1 of v1 will be marked again since it's still absent from CV
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    Assert.assertEquals(heartbeatMonitoringService.getCleanupHeartbeatMap().size(), 1);
    Assert.assertEquals(
        heartbeatMonitoringService.getCleanupHeartbeatMap().get(Utils.getReplicaId(backupVersionTopic, 1)).intValue(),
        1);
    Assert.assertEquals(heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(2).size(), 0);
    // Mimic the CV is in-sync again and the marked p1 of v1 should be removed from cleanup map
    doReturn(mockPartition0And2).when(mockPartitionAssignment).getPartition(1);
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    Assert.assertEquals(heartbeatMonitoringService.getCleanupHeartbeatMap().size(), 0);
    // If resources are deleted, their lag monitors should be marked for cleanup too
    doThrow(new VeniceNoHelixResourceException("resource deleted test")).when(mockCustomizedViewOfflinePushRepository)
        .getPartitionAssignments(anyString());
    heartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    Assert.assertEquals(heartbeatMonitoringService.getCleanupHeartbeatMap().size(), 5);
  }
}
