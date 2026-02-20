package com.linkedin.davinci.helix;

import static com.linkedin.venice.helix.HelixState.DROPPED_STATE;
import static com.linkedin.venice.helix.HelixState.LEADER_STATE;
import static com.linkedin.venice.helix.HelixState.OFFLINE_STATE;
import static com.linkedin.venice.helix.HelixState.STANDBY_STATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatLagMonitorAction;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LeaderFollowerPartitionStateModelTest {
  private IngestionBackend ingestionBackend;
  private KafkaStoreIngestionService storeIngestionService;
  private VeniceStoreVersionConfig storeAndServerConfigs;
  private LeaderFollowerIngestionProgressNotifier notifier;
  private ReadOnlyStoreRepository metadataRepo;
  private CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;
  private ParticipantStateTransitionStats stateTransitionStats;
  private HeartbeatMonitoringService heartbeatMonitoringService;
  private LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModel;
  private static final String storeName = "store_85c9234588_1cce12d5";
  private static final int storeVersion = 3;
  private static final int partition = 0;
  private static final String resourceName = storeName + "_v" + storeVersion;

  @BeforeMethod
  public void setUp() {
    ingestionBackend = mock(IngestionBackend.class);
    storeIngestionService = mock(KafkaStoreIngestionService.class);
    doReturn(storeIngestionService).when(ingestionBackend).getStoreIngestionService();
    doReturn(CompletableFuture.completedFuture(null)).when(ingestionBackend).stopConsumption(any(), anyInt());
    doReturn(CompletableFuture.completedFuture(null)).when(ingestionBackend)
        .dropStoragePartitionGracefully(any(), anyInt(), anyInt());

    storeAndServerConfigs = mock(VeniceStoreVersionConfig.class);
    notifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    metadataRepo = mock(ReadOnlyStoreRepository.class);
    partitionPushStatusAccessorFuture = CompletableFuture.completedFuture(mock(HelixPartitionStatusAccessor.class));
    stateTransitionStats = mock(ParticipantStateTransitionStats.class);
    heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    leaderFollowerPartitionStateModel = new LeaderFollowerPartitionStateModel(
        ingestionBackend,
        storeAndServerConfigs,
        partition,
        notifier,
        metadataRepo,
        partitionPushStatusAccessorFuture,
        "instanceName",
        stateTransitionStats,
        heartbeatMonitoringService,
        resourceName);
  }

  @Test
  public void testUpdateLagMonitor() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    Store store = mock(Store.class);
    when(store.getVersionStatus(anyInt())).thenReturn(VersionStatus.STARTED);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // STANDBY->LEADER
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(heartbeatMonitoringService, never())
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_LEADER_MONITOR));

    // LEADER->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromLeader(message, context);
    verify(heartbeatMonitoringService, never())
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR));

    // OFFLINE->STANDBY
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromOffline(message, context);
    verify(heartbeatMonitoringService, times(1))
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.SET_FOLLOWER_MONITOR));

    // STANDBY->OFFLINE
    leaderFollowerPartitionStateModelSpy.onBecomeOfflineFromStandby(message, context);
    verify(heartbeatMonitoringService)
        .updateLagMonitor(eq(resourceName), eq(partition), eq(HeartbeatLagMonitorAction.REMOVE_MONITOR));
  }

  @Test
  public void testStateModelStats() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    Store store = mock(Store.class);
    when(store.getVersionStatus(anyInt())).thenReturn(VersionStatus.STARTED);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    LeaderFollowerPartitionStateModel leaderFollowerPartitionStateModelSpy = spy(leaderFollowerPartitionStateModel);

    // OFFLINE->STANDBY
    doReturn(OFFLINE_STATE).when(message).getFromState();
    doReturn(STANDBY_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromOffline(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);

    // STANDBY->LEADER
    doReturn(STANDBY_STATE).when(message).getFromState();
    doReturn(LEADER_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeLeaderFromStandby(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(STANDBY_STATE, LEADER_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(STANDBY_STATE, LEADER_STATE);

    // LEADER->STANDBY
    doReturn(LEADER_STATE).when(message).getFromState();
    doReturn(STANDBY_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeStandbyFromLeader(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(LEADER_STATE, STANDBY_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(LEADER_STATE, STANDBY_STATE);

    // STANDBY->OFFLINE
    doReturn(STANDBY_STATE).when(message).getFromState();
    doReturn(OFFLINE_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeOfflineFromStandby(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(STANDBY_STATE, OFFLINE_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(STANDBY_STATE, OFFLINE_STATE);

    // OFFLINE -> DROPPED
    doReturn(OFFLINE_STATE).when(message).getFromState();
    doReturn(DROPPED_STATE).when(message).getToState();
    leaderFollowerPartitionStateModelSpy.onBecomeDroppedFromOffline(message, context);
    verify(stateTransitionStats).trackStateTransitionStarted(OFFLINE_STATE, DROPPED_STATE);
    verify(stateTransitionStats).trackStateTransitionCompleted(OFFLINE_STATE, DROPPED_STATE);
  }

  /**
   * Tests timestamp tracking behavior for graceful drop timing across multiple scenarios.
   */
  @Test
  public void testTimestampTracking() {
    Message offlineMessage = mock(Message.class);
    Message droppedMessage = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(offlineMessage.getResourceName()).thenReturn(resourceName);
    when(droppedMessage.getResourceName()).thenReturn(resourceName);

    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(storeVersion);
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    // Case 1: Timestamp captured during STANDBY->OFFLINE transition
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(0); // No sleep for speed
    long beforeTransition = System.currentTimeMillis();
    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage, context);
    long afterTransition = System.currentTimeMillis();

    long capturedTimestamp = leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs();
    assertTrue(capturedTimestamp >= beforeTransition, "Case 1: Timestamp should be >= beforeTransition");
    assertTrue(capturedTimestamp <= afterTransition, "Case 1: Timestamp should be <= afterTransition");
    assertTrue(capturedTimestamp > 0, "Case 1: Timestamp should be positive");

    // Case 2: Timestamp reset after OFFLINE->DROPPED transition
    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(droppedMessage, context);
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 2: Timestamp should be reset to -1 after transition");

    // Case 3: Multiple cycles maintain independent timestamps
    when(store.getCurrentVersion()).thenReturn(storeVersion + 1); // Non-current to skip sleep

    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage, context);
    long timestamp1 = leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs();
    assertTrue(timestamp1 > 0, "Case 3: First cycle timestamp should be positive");

    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(droppedMessage, context);
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 3: First cycle timestamp should be reset");
    Utils.sleep(5); // Minimal sleep to ensure different timestamps
    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage, context);
    long timestamp2 = leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs();
    assertTrue(timestamp2 > timestamp1, "Case 3: Second cycle timestamp should be later than first");

    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(droppedMessage, context);
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 3: Second cycle timestamp should be reset");
  }

  /**
   * Tests graceful drop timing scenarios with milliseconds for faster test execution.
   */
  @Test
  public void testGracefulDropTimingScenarios() {
    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(storeVersion); // Current version
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());
    NotificationContext context = mock(NotificationContext.class);

    // Case 1: Partial wait - partition offline for some time, should wait for remaining delay
    // Using 2 second delay and 500ms sleep for more tolerance
    Message message1 = mock(Message.class);
    Message offlineMessage1 = mock(Message.class);
    when(message1.getResourceName()).thenReturn(resourceName);
    when(offlineMessage1.getResourceName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(2); // 2 seconds

    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage1, context);
    Utils.sleep(500); // Sleep 500ms to simulate time spent offline

    long startTime1 = System.currentTimeMillis();
    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message1, context);
    long elapsedTime1 = System.currentTimeMillis() - startTime1;

    // Should wait less than full 2000ms since partition already offline for 500ms+
    // But allow wide tolerance for test overhead (100ms - 1700ms range)
    assertTrue(
        elapsedTime1 >= 100 && elapsedTime1 <= 1700,
        "Case 1: Should wait less than 2000ms (some elapsed), waited: " + elapsedTime1 + "ms");
    verify(stateTransitionStats, times(1)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, times(1)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 1: Timestamp should be reset");

    // Case 2: Skip wait - partition offline longer than delay, should skip sleep entirely
    Message message2 = mock(Message.class);
    Message offlineMessage2 = mock(Message.class);
    when(message2.getResourceName()).thenReturn(resourceName);
    when(offlineMessage2.getResourceName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(0); // 0 seconds delay

    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage2, context);
    Utils.sleep(100); // Sleep 100ms to simulate time spent offline

    long startTime2 = System.currentTimeMillis();
    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message2, context);
    long elapsedTime2 = System.currentTimeMillis() - startTime2;

    assertTrue(
        elapsedTime2 < 100,
        "Case 2: Should skip sleep (offline longer than delay), waited: " + elapsedTime2 + "ms");
    verify(stateTransitionStats, times(1)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, times(1)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 2: Timestamp should be reset");

    // Case 3: No timestamp - should use full delay
    Message message3 = mock(Message.class);
    when(message3.getResourceName()).thenReturn(resourceName);

    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 3: Timestamp should be -1 initially");

    // Use 1 second delay for this case
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(1);
    long startTime3 = System.currentTimeMillis();
    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message3, context);
    long elapsedTime3 = System.currentTimeMillis() - startTime3;

    assertTrue(
        elapsedTime3 >= 900 && elapsedTime3 <= 1200,
        "Case 3: Should wait full 1000ms (no timestamp), waited: " + elapsedTime3 + "ms");
    verify(stateTransitionStats, times(2)).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, times(2)).decrementThreadBlockedOnOfflineToDroppedTransitionCount();
    assertEquals(
        leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs(),
        -1L,
        "Case 3: Timestamp should remain -1");
  }

  /**
   * Tests that non-current version skips graceful drop delay entirely.
   */
  @Test
  public void testNonCurrentVersionSkipsGracefulDrop() {
    Message message = mock(Message.class);
    NotificationContext context = mock(NotificationContext.class);
    when(message.getResourceName()).thenReturn(resourceName);
    when(storeAndServerConfigs.getPartitionGracefulDropDelaySeconds()).thenReturn(10); // 10 seconds

    Store store = mock(Store.class);
    when(store.getCurrentVersion()).thenReturn(storeVersion + 1); // NOT current version
    doReturn(store).when(metadataRepo).getStoreOrThrow(anyString());

    // Transition to OFFLINE first to capture timestamp
    Message offlineMessage = mock(Message.class);
    when(offlineMessage.getResourceName()).thenReturn(resourceName);
    leaderFollowerPartitionStateModel.onBecomeOfflineFromStandby(offlineMessage, context);

    long startTime = System.currentTimeMillis();

    // Execute OFFLINE->DROPPED transition
    leaderFollowerPartitionStateModel.onBecomeDroppedFromOffline(message, context);

    long elapsedTime = System.currentTimeMillis() - startTime;

    // Should skip graceful drop entirely for non-current version
    assertTrue(elapsedTime < 1000, "Should not wait for non-current version, waited: " + elapsedTime + "ms");

    // Stats should not be incremented since no actual waiting occurs
    // (graceful drop is skipped for non-current version, and partition removal future is already complete)
    verify(stateTransitionStats, never()).incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    verify(stateTransitionStats, never()).decrementThreadBlockedOnOfflineToDroppedTransitionCount();

    // Verify timestamp is reset
    long resetTimestamp = leaderFollowerPartitionStateModel.getOfflineTransitionTimestampMs();
    assertEquals(resetTimestamp, -1L, "Timestamp should be reset to -1 after transition");
  }

  /**
   * Integration test that verifies RocksDB deletion rate limiting is honored during
   * OFFLINE->DROPPED state transition (backup version deletion).
   * This test creates a real storage engine with data, then triggers the OFFLINE->DROPPED
   * state transition and verifies that the deletion is throttled according to the configured rate.
   *
   * This test also creates a snapshot (simulating blob transfer scenario) to verify that
   * rate limiting works correctly even when hard links exist.
   */
  @Test(groups = { "integration" })
  public void testOfflineToDroppedTransitionHonorsRateLimiting() throws Exception {
    // Setup: Create a real storage engine with rate limiting
    String testDataPath = Utils.getUniqueTempPath();
    long deletionRateBytesPerSec = 20L * 1024 * 1024; // 20 MB/s

    VeniceProperties veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put("data.base.path", testDataPath)
        .put(
            RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND,
            String.valueOf(deletionRateBytesPerSec))
        .build();

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    try {
      // Create a storage engine with ~200 MB of data
      final String testStoreName = "test_store_rate_limit";
      final int testVersion = 1;
      final String testTopic = Version.composeKafkaTopic(testStoreName, testVersion);
      final int testPartition = 0;

      VeniceStoreVersionConfig testStoreConfig =
          new VeniceStoreVersionConfig(testTopic, veniceServerProperties, PersistenceType.ROCKS_DB);
      StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
      storeEngine.addStoragePartitionIfAbsent(testPartition);

      // Write ~200 MB of data to generate SST files
      long targetDataSizeBytes = 200L * 1024 * 1024; // 200 MB
      long valueSizeBytes = 50 * 1024; // 50 KB per value
      long numRecords = targetDataSizeBytes / valueSizeBytes;

      Random random = new Random(42);
      byte[] valueBytes = new byte[(int) valueSizeBytes];

      for (int i = 0; i < numRecords; i++) {
        String keyString = "key_" + i;
        byte[] keyBytes = keyString.getBytes();
        random.nextBytes(valueBytes);
        storeEngine.put(testPartition, keyBytes, ByteBuffer.wrap(valueBytes));
      }

      // Verify data was written
      File storeDir = new File(factory.getRocksDBPath(testTopic, testPartition)).getParentFile();
      assertTrue(storeDir.exists(), "Store directory should exist before deletion");

      // Sync/flush data to disk before creating snapshot
      AbstractStoragePartition partition = storeEngine.getPartitionOrThrow(testPartition);
      partition.sync();

      // Create a snapshot to simulate blob transfer scenario
      // This creates hard links to the SST files, helping test the case where
      // snapshot exists during backup version deletion
      partition.createSnapshot();

      // Verify snapshot was created
      // Note: serverConfig.getRocksDBPath() returns dataBasePath + "/rocksdb"
      String rocksDBBasePath = serverConfig.getRocksDBPath();
      String snapshotPath = RocksDBUtils.composeSnapshotDir(rocksDBBasePath, testTopic, testPartition);
      File partitionSnapshotDir = new File(snapshotPath);
      assertTrue(partitionSnapshotDir.exists(), "Snapshot directory should exist before deletion");

      // Setup: Create a mock ingestion backend that uses the real storage service
      KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
      DefaultIngestionBackend realIngestionBackend = mock(DefaultIngestionBackend.class);

      // When dropStoragePartitionGracefully is called, actually delete the partition
      when(realIngestionBackend.dropStoragePartitionGracefully(any(), eq(testPartition), anyInt()))
          .thenAnswer(invocation -> {
            // Simulate the actual deletion by calling factory.removeStorageEngine
            factory.removeStorageEngine(storeEngine);
            return CompletableFuture.completedFuture(null);
          });

      when(realIngestionBackend.getStoreIngestionService()).thenReturn(mockIngestionService);
      when(realIngestionBackend.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

      // Setup: Create state model with real backend
      VeniceStoreVersionConfig stateModelConfig = mock(VeniceStoreVersionConfig.class);
      when(stateModelConfig.getStopConsumptionTimeoutInSeconds()).thenReturn(60);
      when(stateModelConfig.getPartitionGracefulDropDelaySeconds()).thenReturn(0); // No graceful drop delay

      ReadOnlyStoreRepository mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
      Store mockStore = mock(Store.class);
      when(mockStore.getCurrentVersion()).thenReturn(testVersion + 1); // Not current version
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(anyString());

      ParticipantStateTransitionStats mockStats = mock(ParticipantStateTransitionStats.class);
      HeartbeatMonitoringService mockHeartbeatService = mock(HeartbeatMonitoringService.class);
      CompletableFuture<HelixPartitionStatusAccessor> mockAccessorFuture =
          CompletableFuture.completedFuture(mock(HelixPartitionStatusAccessor.class));

      LeaderFollowerPartitionStateModel stateModel = new LeaderFollowerPartitionStateModel(
          realIngestionBackend,
          stateModelConfig,
          testPartition,
          mock(LeaderFollowerIngestionProgressNotifier.class),
          mockMetadataRepo,
          mockAccessorFuture,
          "testInstance",
          mockStats,
          mockHeartbeatService,
          testTopic);

      // Execute: Trigger OFFLINE->DROPPED state transition
      Message message = mock(Message.class);
      when(message.getResourceName()).thenReturn(testTopic);
      NotificationContext context = mock(NotificationContext.class);

      long startTime = System.currentTimeMillis();
      stateModel.onBecomeDroppedFromOffline(message, context);
      long elapsedTime = System.currentTimeMillis() - startTime;

      // Verify: Deletion should be throttled (should take ~10 seconds for 200 MB at 20 MB/s)
      // This validates that rate limiting works even when a snapshot exists (hard links scenario)
      double elapsedTimeSec = elapsedTime / 1000.0;
      assertTrue(
          elapsedTimeSec >= 8.0,
          String.format(
              "Deletion during OFFLINE->DROPPED should be throttled (>= 8 seconds), but took %.2f seconds. "
                  + "This test includes snapshot creation to verify rate limiting works with hard links.",
              elapsedTimeSec));

      // Verify: Store directory should be deleted
      assertFalse(storeDir.exists(), "Store directory should be deleted after OFFLINE->DROPPED transition");

      // Verify: Snapshot directory should also be deleted
      assertFalse(
          partitionSnapshotDir.exists(),
          "Snapshot directory should be deleted after OFFLINE->DROPPED transition");

    } finally {
      factory.close();
      try {
        FileUtils.deleteDirectory(new File(testDataPath));
      } catch (Exception e) {
        System.err.println("Failed to cleanup test directory: " + testDataPath + ", error: " + e.getMessage());
      }
    }
  }

}
