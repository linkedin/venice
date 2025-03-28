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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    Mockito.verify(heartbeatMonitoringService, Mockito.times(2))
        .getHeartbeatInfoFromMap(any(), anyString(), anyLong(), anyString(), anyInt(), anyBoolean());
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
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(1L, 1L, 1L, DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_SOP);
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
    ReadOnlyStoreRepository mockReadOnlyRepository = mock(ReadOnlyStoreRepository.class);
    Mockito.when(mockReadOnlyRepository.getStoreOrThrow(TEST_STORE)).thenReturn(mockStore);

    Set<String> regions = new HashSet<>();
    regions.add(LOCAL_FABRIC);
    regions.add(REMOTE_FABRIC);
    regions.add(REMOTE_FABRIC + SEPARATE_TOPIC_SUFFIX);
    HeartbeatMonitoringService heartbeatMonitoringService =
        new HeartbeatMonitoringService(mockMetricsRepository, mockReadOnlyRepository, regions, LOCAL_FABRIC, null);

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
}
