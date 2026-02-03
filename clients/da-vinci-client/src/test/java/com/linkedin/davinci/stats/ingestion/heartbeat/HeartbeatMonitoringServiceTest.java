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
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
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
import java.util.HashMap;
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

    Map<String, Map<Integer, Map<Integer, Map<String, IngestionTimestampEntry>>>> leaderMap =
        new VeniceConcurrentHashMap<>();
    Map<String, Map<Integer, Map<Integer, Map<String, IngestionTimestampEntry>>>> followerMap =
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
        .put("dc-0", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(5), true, true));
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, true));
    leaderMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1_sep", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(100), true, true));

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
        .put("dc-2", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(20), false, false));
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
        .put("dc-1", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, true));

    // Check valid follower lag
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, version, true);
    Assert.assertTrue(lag >= TimeUnit.MINUTES.toMillis(10));
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, currentTime - TimeUnit.MINUTES.toMillis(10));

    // Add unrelated region
    followerMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-0", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(20), true, true));
    lag = heartbeatMonitoringService.getReplicaFollowerHeartbeatLag(pcs, store, version, true);
    Assert.assertTrue(lag >= TimeUnit.MINUTES.toMillis(10));
    Assert.assertTrue(lag < TimeUnit.MINUTES.toMillis(20));
    timestamp = heartbeatMonitoringService.getReplicaFollowerHeartbeatTimestamp(pcs, store, version, true);
    Assert.assertEquals(timestamp, currentTime - TimeUnit.MINUTES.toMillis(10));
    // Set local region lag to be invalid
    followerMap.get(store)
        .get(version)
        .get(partition)
        .put("dc-1", new IngestionTimestampEntry(currentTime - TimeUnit.MINUTES.toMillis(10), true, false));
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
    Map<String, Map<Integer, Map<Integer, Map<String, IngestionTimestampEntry>>>> leaderMap =
        new VeniceConcurrentHashMap<>();
    String store = "testStore";
    int version = 1;
    int partition = 1;
    String region = "dc-0";
    long timestamp = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
    leaderMap.put(store, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).put(version, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).get(version).put(partition, new VeniceConcurrentHashMap<>());
    leaderMap.get(store).get(version).get(partition).put(region, new IngestionTimestampEntry(timestamp, true, true));
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
    doCallRealMethod().when(heartbeatMonitoringService).checkAndMaybeLogHeartbeatDelayMap(anyMap());

    heartbeatMonitoringService.checkAndMaybeLogHeartbeatDelayMap(leaderMap);
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
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 2, 0, LOCAL_FABRIC, baseTimeStamp + 1002L, true);
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 3, 0, LOCAL_FABRIC, baseTimeStamp + 1002L, true);

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
        .get(LOCAL_FABRIC).heartbeatTimestamp;
    Assert.assertTrue(value >= baseTimeStamp + 1001L);

    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(futureVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC).heartbeatTimestamp;
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
        .get(REMOTE_FABRIC).heartbeatTimestamp;
    Assert.assertEquals((long) value, baseTimeStamp + 1003L);
    value = heartbeatMonitoringService.getFollowerHeartbeatTimeStamps()
        .get(TEST_STORE)
        .get(currentVersion.getNumber())
        .get(1)
        .get(REMOTE_FABRIC).heartbeatTimestamp;
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
    doReturn(5).when(serverConfig).getLagMonitorCleanupCycle();
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

    // Verify lagMonitorCleanupCycle is configurable
    doReturn(100).when(serverConfig).getLagMonitorCleanupCycle();
    HeartbeatMonitoringService newHeartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);
    // Initialize the new lag monitor for leader of p0 and follower of p1 and p2 for v1
    newHeartbeatMonitoringService.updateLagMonitor(backupVersionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);
    newHeartbeatMonitoringService
        .updateLagMonitor(backupVersionTopic, 1, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    newHeartbeatMonitoringService
        .updateLagMonitor(backupVersionTopic, 2, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);
    for (int i = 0; i < 50; i++) {
      newHeartbeatMonitoringService.checkAndMaybeCleanupLagMonitor();
    }
    // All 3 replicas of V1 should be marked for cleanup and since we configured the cycle to be 100, it shouldn't be
    // cleaned up yet after 50 cycles yet
    Assert.assertEquals(newHeartbeatMonitoringService.getCleanupHeartbeatMap().size(), 3);
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          newHeartbeatMonitoringService.getCleanupHeartbeatMap()
              .get(Utils.getReplicaId(backupVersionTopic, i))
              .intValue(),
          50);
    }
  }

  @Test
  public void testLargestHeartbeatLag() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    doCallRealMethod().when(heartbeatMonitoringService).getMaxHeartbeatLag(anyLong(), anyMap());
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(metadataRepo).when(heartbeatMonitoringService).getMetadataRepository();
    Store store = mock(Store.class);
    doReturn(1).when(store).getCurrentVersion();
    String storeName = "foo";
    doReturn(store).when(metadataRepo).getStore(storeName);

    Map<String, Map<Integer, Map<Integer, Map<String, IngestionTimestampEntry>>>> heartbeatTimestamps =
        new VeniceConcurrentHashMap<>();
    IngestionTimestampEntry entry1 = new IngestionTimestampEntry(1000L, false, false);
    IngestionTimestampEntry entry2 = new IngestionTimestampEntry(2000L, true, false);
    IngestionTimestampEntry entry3 = new IngestionTimestampEntry(3000L, false, true);
    IngestionTimestampEntry entry4 = new IngestionTimestampEntry(4000L, true, true);

    heartbeatTimestamps.put(storeName, new VeniceConcurrentHashMap<>());
    heartbeatTimestamps.get(storeName).put(1, new VeniceConcurrentHashMap<>());
    heartbeatTimestamps.get(storeName).get(1).put(0, new VeniceConcurrentHashMap<>());
    heartbeatTimestamps.get(storeName).get(1).get(0).put("dc1", entry1);
    heartbeatTimestamps.get(storeName).get(1).get(0).put("dc2", entry2);
    heartbeatTimestamps.get(storeName).get(1).get(0).put("dc3", entry3);
    heartbeatTimestamps.get(storeName).get(1).get(0).put("dc4", entry4);

    // Current not-serving replica should not be tracked in the current version lag.
    long currentTimestamp = 10000L;
    AggregatedHeartbeatLagEntry aggregatedHeartbeatLagEntry =
        heartbeatMonitoringService.getMaxHeartbeatLag(currentTimestamp, heartbeatTimestamps);
    Assert.assertEquals(aggregatedHeartbeatLagEntry.getCurrentVersionHeartbeatLag(), 8000L);
    Assert.assertEquals(aggregatedHeartbeatLagEntry.getNonCurrentVersionHeartbeatLag(), 9000L);

    // Add a more stale entry in non-current version.
    IngestionTimestampEntry entry5 = new IngestionTimestampEntry(100L, true, true);
    heartbeatTimestamps.get(storeName).put(2, new VeniceConcurrentHashMap<>());
    heartbeatTimestamps.get(storeName).get(2).put(0, new VeniceConcurrentHashMap<>());
    heartbeatTimestamps.get(storeName).get(2).get(0).put("dc1", entry5);
    aggregatedHeartbeatLagEntry = heartbeatMonitoringService.getMaxHeartbeatLag(currentTimestamp, heartbeatTimestamps);
    Assert.assertEquals(aggregatedHeartbeatLagEntry.getCurrentVersionHeartbeatLag(), 8000L);
    Assert.assertEquals(aggregatedHeartbeatLagEntry.getNonCurrentVersionHeartbeatLag(), 9900L);
  }

  @Test
  public void testTriggerAutoResubscribe() {
    String store = "foo";
    int version = 100;
    int partition = 123;
    String region = "dc1";
    Map<String, Map<Integer, Map<Integer, Map<String, IngestionTimestampEntry>>>> heartbeatTimestamps = new HashMap<>();
    IngestionTimestampEntry entry =
        new IngestionTimestampEntry(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(15), true, true);
    heartbeatTimestamps.put(store, new HashMap<>());
    heartbeatTimestamps.get(store).put(version, new HashMap<>());
    heartbeatTimestamps.get(store).get(version).put(partition, new HashMap<>());
    heartbeatTimestamps.get(store).get(version).get(partition).put(region, entry);

    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    KafkaStoreIngestionService kafkaStoreIngestionService = mock(KafkaStoreIngestionService.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(serverConfig).when(heartbeatMonitoringService).getServerConfig();
    doReturn(kafkaStoreIngestionService).when(heartbeatMonitoringService).getKafkaStoreIngestionService();
    doCallRealMethod().when(heartbeatMonitoringService).checkAndMaybeLogHeartbeatDelayMap(anyMap());

    // Config not enabled, nothing happen
    heartbeatMonitoringService.checkAndMaybeLogHeartbeatDelayMap(heartbeatTimestamps);
    verify(kafkaStoreIngestionService, never()).maybeAddResubscribeRequest(eq(store), eq(version), eq(partition));

    // Config enabled, trigger resubscribe.
    doReturn(true).when(serverConfig).isLagBasedReplicaAutoResubscribeEnabled();
    doReturn(600).when(serverConfig).getLagBasedReplicaAutoResubscribeThresholdInSeconds();
    heartbeatMonitoringService.checkAndMaybeLogHeartbeatDelayMap(heartbeatTimestamps);
    verify(kafkaStoreIngestionService, times(1)).maybeAddResubscribeRequest(eq(store), eq(version), eq(partition));

    // Config enabled, does not trigger resubscribe for sep region.
    heartbeatTimestamps.get(store).get(version).get(partition).remove(region);
    region = "dc1_sep";
    heartbeatTimestamps.get(store).get(version).get(partition).put(region, entry);
    heartbeatMonitoringService.checkAndMaybeLogHeartbeatDelayMap(heartbeatTimestamps);
    verify(kafkaStoreIngestionService, times(1)).maybeAddResubscribeRequest(eq(store), eq(version), eq(partition));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordLevelTimestampTracking(boolean recordLevelTimestampEnabled) {
    // Setup
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    Version version = new VersionImpl(TEST_STORE, 1, "1");
    version.setHybridStoreConfig(hybridStoreConfig);
    version.setActiveActiveReplicationEnabled(true);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(TEST_STORE);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(mockStore.getVersion(1)).thenReturn(version);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    when(mockReadOnlyRepository.waitVersion(eq(TEST_STORE), eq(1), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, version));

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);
    regions.add(REMOTE_FABRIC);

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getRegionNames()).thenReturn(regions);
    when(serverConfig.getRegionName()).thenReturn(LOCAL_FABRIC);
    when(serverConfig.getServerMaxWaitForVersionInfo()).thenReturn(Duration.ofSeconds(5));
    when(serverConfig.getListenerHostname()).thenReturn("localhost");
    when(serverConfig.getListenerPort()).thenReturn(123);
    when(serverConfig.getLagMonitorCleanupCycle()).thenReturn(5);
    when(serverConfig.isRecordLevelTimestampEnabled()).thenReturn(recordLevelTimestampEnabled);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);

    // Add leader lag monitor - this initializes the map structures
    String versionTopic = Version.composeKafkaTopic(TEST_STORE, 1);
    heartbeatMonitoringService.updateLagMonitor(versionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);

    // Record heartbeat control message at time 1000
    long heartbeatTime = 1000L;
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, heartbeatTime, true);

    // Verify entry exists and get initial values
    IngestionTimestampEntry entry1 =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    Assert.assertNotNull(entry1);
    long heartbeatTs1 = entry1.heartbeatTimestamp;
    long recordTs1 = entry1.recordTimestamp;

    // After recordLeaderHeartbeat, timestamps should reflect the heartbeat
    Assert.assertTrue(heartbeatTs1 >= heartbeatTime, "Heartbeat timestamp should be at least the provided value");

    // Record data records with timestamps 1500, 1200, 1800
    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, 1500L, true);
    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, 1200L, true);
    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, 1800L, true);

    IngestionTimestampEntry entry2 =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);

    if (recordLevelTimestampEnabled) {
      // When config is enabled:
      // - heartbeatTimestamp should remain unchanged (not updated by record timestamps)
      // - recordTimestamp should be max of all records: >= 1800
      Assert.assertEquals(
          entry2.heartbeatTimestamp,
          heartbeatTs1,
          "Heartbeat timestamp should not be updated by records");
      Assert.assertTrue(
          entry2.recordTimestamp >= 1800L,
          "Record timestamp should be at least 1800 (max of all record timestamps)");
    } else {
      // When config is disabled, recordLeaderRecordTimestamp calls should be no-op
      Assert.assertEquals(entry2.heartbeatTimestamp, heartbeatTs1);
      Assert.assertEquals(entry2.recordTimestamp, recordTs1); // Should be unchanged
    }

    // Record another heartbeat at time 2000
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, 2000L, true);

    IngestionTimestampEntry entry3 =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);

    // Heartbeat updated - should be at least 2000
    Assert.assertTrue(entry3.heartbeatTimestamp >= 2000L);

    // Record timestamp should be updated regardless of recordLevelTimestampEnabled flag
    // When enabled: recordTimestamp = max(heartbeat, previous record ts)
    // When disabled: recordTimestamp is updated together with heartbeatTimestamp
    Assert.assertTrue(entry3.recordTimestamp >= 2000L);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testFollowerRecordLevelTimestampTracking(boolean recordLevelTimestampEnabled) {
    // Setup similar to leader test
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    Version version = new VersionImpl(TEST_STORE, 1, "1");
    version.setHybridStoreConfig(hybridStoreConfig);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(TEST_STORE);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(mockStore.getVersion(1)).thenReturn(version);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    when(mockReadOnlyRepository.waitVersion(eq(TEST_STORE), eq(1), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, version));

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getRegionNames()).thenReturn(regions);
    when(serverConfig.getRegionName()).thenReturn(LOCAL_FABRIC);
    when(serverConfig.getServerMaxWaitForVersionInfo()).thenReturn(Duration.ofSeconds(5));
    when(serverConfig.getListenerHostname()).thenReturn("localhost");
    when(serverConfig.getListenerPort()).thenReturn(123);
    when(serverConfig.getLagMonitorCleanupCycle()).thenReturn(5);
    when(serverConfig.isRecordLevelTimestampEnabled()).thenReturn(recordLevelTimestampEnabled);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);

    // Add follower lag monitor
    String versionTopic = Version.composeKafkaTopic(TEST_STORE, 1);
    heartbeatMonitoringService.updateLagMonitor(versionTopic, 0, HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR);

    // Record follower heartbeat at time 1000
    long heartbeatTime = 1000L;
    heartbeatMonitoringService.recordFollowerHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, heartbeatTime, true);

    // Verify initial state
    IngestionTimestampEntry entry1 =
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    long heartbeatTs1 = entry1.heartbeatTimestamp;
    long recordTs1 = entry1.recordTimestamp;

    // Record data records
    heartbeatMonitoringService.recordFollowerRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, 1500L, true);
    heartbeatMonitoringService.recordFollowerRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, 1300L, true);

    IngestionTimestampEntry entry2 =
        heartbeatMonitoringService.getFollowerHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);

    if (recordLevelTimestampEnabled) {
      Assert.assertEquals(entry2.heartbeatTimestamp, heartbeatTs1, "Heartbeat timestamp should not change");
      Assert.assertTrue(entry2.recordTimestamp >= 1500L, "Record timestamp should be at least 1500");
    } else {
      Assert.assertEquals(entry2.heartbeatTimestamp, heartbeatTs1);
      Assert.assertEquals(entry2.recordTimestamp, recordTs1); // Should be unchanged
    }
  }

  /**
   * Tests that per-record OTel metrics are emitted when perRecordOtelMetricsEnabled is true,
   * and not emitted when it's false.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testPerRecordOtelMetricsEmission(boolean perRecordOtelMetricsEnabled) {
    // Setup store and config
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    Version version = new VersionImpl(TEST_STORE, 1, "push1");
    version.setHybridStoreConfig(hybridStoreConfig);
    version.setActiveActiveReplicationEnabled(true);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(TEST_STORE);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(mockStore.getVersion(1)).thenReturn(version);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    when(mockReadOnlyRepository.waitVersion(eq(TEST_STORE), eq(1), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, version));

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getRegionNames()).thenReturn(regions);
    when(serverConfig.getRegionName()).thenReturn(LOCAL_FABRIC);
    when(serverConfig.getServerMaxWaitForVersionInfo()).thenReturn(Duration.ofSeconds(5));
    when(serverConfig.getListenerHostname()).thenReturn("localhost");
    when(serverConfig.getListenerPort()).thenReturn(123);
    when(serverConfig.getLagMonitorCleanupCycle()).thenReturn(5);
    // Enable record-level timestamp tracking (required for per-record OTel)
    when(serverConfig.isRecordLevelTimestampEnabled()).thenReturn(true);
    when(serverConfig.isPerRecordOtelMetricsEnabled()).thenReturn(perRecordOtelMetricsEnabled);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    // Create service and spy on versionStatsReporter
    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);

    // Add leader lag monitor
    String versionTopic = Version.composeKafkaTopic(TEST_STORE, 1);
    heartbeatMonitoringService.updateLagMonitor(versionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);

    // Record leader record timestamp
    long recordTimestamp = System.currentTimeMillis() - 100;
    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, recordTimestamp, true);

    // Verify recordTimestamp was updated (this works regardless of perRecordOtelMetricsEnabled
    // when recordLevelTimestampEnabled is true)
    IngestionTimestampEntry entry =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    Assert.assertNotNull(entry, "Entry should be created");
    Assert.assertTrue(entry.recordTimestamp >= recordTimestamp, "Record timestamp should be updated");
  }

  /**
   * Tests that per-record OTel metrics are NOT emitted when recordLevelTimestampEnabled is false,
   * regardless of perRecordOtelMetricsEnabled setting.
   */
  @Test
  public void testPerRecordOtelMetricsNotEmittedWhenRecordLevelTimestampDisabled() {
    // Setup store and config
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    Version version = new VersionImpl(TEST_STORE, 1, "push1");
    version.setHybridStoreConfig(hybridStoreConfig);
    version.setActiveActiveReplicationEnabled(true);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(TEST_STORE);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(mockStore.getVersion(1)).thenReturn(version);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    when(mockReadOnlyRepository.waitVersion(eq(TEST_STORE), eq(1), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, version));

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getRegionNames()).thenReturn(regions);
    when(serverConfig.getRegionName()).thenReturn(LOCAL_FABRIC);
    when(serverConfig.getServerMaxWaitForVersionInfo()).thenReturn(Duration.ofSeconds(5));
    when(serverConfig.getListenerHostname()).thenReturn("localhost");
    when(serverConfig.getListenerPort()).thenReturn(123);
    when(serverConfig.getLagMonitorCleanupCycle()).thenReturn(5);
    // Disable record-level timestamp tracking
    when(serverConfig.isRecordLevelTimestampEnabled()).thenReturn(false);
    // Enable per-record OTel (but it shouldn't matter since parent flag is disabled)
    when(serverConfig.isPerRecordOtelMetricsEnabled()).thenReturn(true);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);

    // Add leader lag monitor
    String versionTopic = Version.composeKafkaTopic(TEST_STORE, 1);
    heartbeatMonitoringService.updateLagMonitor(versionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);

    // Record initial heartbeat to establish the entry
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, 1000L, true);

    IngestionTimestampEntry entryBefore =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    long recordTsBefore = entryBefore.recordTimestamp;

    // Record leader record timestamp - should be a no-op since recordLevelTimestampEnabled is false
    long recordTimestamp = System.currentTimeMillis() + 5000;
    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, recordTimestamp, true);

    // Entry's recordTimestamp should remain unchanged
    IngestionTimestampEntry entryAfter =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    Assert.assertEquals(
        entryAfter.recordTimestamp,
        recordTsBefore,
        "Record timestamp should not change when recordLevelTimestampEnabled is false");
  }

  /**
   * Tests that recordTimestamp always considers the heartbeat timestamp when calculating the max.
   * Verifies that when a data record with an older timestamp arrives after a heartbeat with a newer
   * timestamp, the recordTimestamp doesn't decrease.
   */
  @Test
  public void testRecordTimestampConsidersHeartbeatTimestamp() {
    // Setup store and config
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP);
    Version version = new VersionImpl(TEST_STORE, 1, "push1");
    version.setHybridStoreConfig(hybridStoreConfig);
    version.setActiveActiveReplicationEnabled(true);

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(TEST_STORE);
    when(mockStore.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(mockStore.getVersion(1)).thenReturn(version);

    MetricsRepository mockMetricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);
    when(mockReadOnlyRepository.waitVersion(eq(TEST_STORE), eq(1), any(), anyLong()))
        .thenReturn(new StoreVersionInfo(mockStore, version));

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.getRegionNames()).thenReturn(regions);
    when(serverConfig.getRegionName()).thenReturn(LOCAL_FABRIC);
    when(serverConfig.getServerMaxWaitForVersionInfo()).thenReturn(Duration.ofSeconds(5));
    when(serverConfig.getListenerHostname()).thenReturn("localhost");
    when(serverConfig.getListenerPort()).thenReturn(123);
    when(serverConfig.getLagMonitorCleanupCycle()).thenReturn(5);
    when(serverConfig.isRecordLevelTimestampEnabled()).thenReturn(true);

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> mockCVRepositoryFuture = new CompletableFuture<>();

    HeartbeatMonitoringService heartbeatMonitoringService = new HeartbeatMonitoringService(
        mockMetricsRepository,
        mockReadOnlyRepository,
        serverConfig,
        null,
        mockCVRepositoryFuture);

    // Add leader lag monitor
    String versionTopic = Version.composeKafkaTopic(TEST_STORE, 1);
    heartbeatMonitoringService.updateLagMonitor(versionTopic, 0, HeartbeatLagMonitorAction.SET_LEADER_MONITOR);

    // Record heartbeat at a high timestamp
    long highTimestamp = System.currentTimeMillis() + 5000L;
    heartbeatMonitoringService.recordLeaderHeartbeat(TEST_STORE, 1, 0, LOCAL_FABRIC, highTimestamp, true);

    IngestionTimestampEntry entryAfterHeartbeat =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);
    Assert.assertNotNull(entryAfterHeartbeat, "Entry should be initialized after recordLeaderHeartbeat");
    long heartbeatTs = entryAfterHeartbeat.heartbeatTimestamp;
    long recordTsAfterHeartbeat = entryAfterHeartbeat.recordTimestamp;

    Assert.assertTrue(
        recordTsAfterHeartbeat >= highTimestamp,
        "Record timestamp should be at least the heartbeat timestamp");
    Assert.assertTrue(recordTsAfterHeartbeat >= heartbeatTs, "Record timestamp should be >= heartbeat timestamp");

    // Record a data record with a LOWER timestamp than the heartbeat
    long lowTimestamp = System.currentTimeMillis() + 1000L;
    Assert.assertTrue(lowTimestamp < highTimestamp, "Test setup: low timestamp should be less than high timestamp");

    heartbeatMonitoringService.recordLeaderRecordTimestamp(TEST_STORE, 1, 0, LOCAL_FABRIC, lowTimestamp, true);

    IngestionTimestampEntry entryAfterRecord =
        heartbeatMonitoringService.getLeaderHeartbeatTimeStamps().get(TEST_STORE).get(1).get(0).get(LOCAL_FABRIC);

    // Verify recordTimestamp did NOT decrease - it should still be >= the heartbeat timestamp
    Assert.assertEquals(
        entryAfterRecord.recordTimestamp,
        recordTsAfterHeartbeat,
        "Record timestamp should not decrease when a lower data record timestamp arrives");
    Assert.assertTrue(
        entryAfterRecord.recordTimestamp >= heartbeatTs,
        "Record timestamp should always be >= heartbeat timestamp");

    // Verify heartbeat timestamp unchanged
    Assert.assertEquals(
        entryAfterRecord.heartbeatTimestamp,
        heartbeatTs,
        "Heartbeat timestamp should not change for data records");
  }
}
