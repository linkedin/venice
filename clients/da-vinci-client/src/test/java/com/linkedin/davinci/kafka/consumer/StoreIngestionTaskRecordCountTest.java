package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.setField;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.logger.TestLogAppender;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.server.VersionRole;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.testng.annotations.Test;


public class StoreIngestionTaskRecordCountTest {
  private static final String TEST_TOPIC = "test_store_v1";
  private static final String TEST_STORE = "test_store";
  private static final int TEST_VERSION = 1;
  private static final long PRE_MIGRATION_VERSION_CREATED_TIME_MS = 1_000L;
  private static final long MIGRATION_STORE_CREATED_TIME_MS = 2_000L;
  private static final long DURING_MIGRATION_VERSION_CREATED_TIME_MS = 3_000L;

  private static StoreIngestionTask buildSit(boolean failOnMismatchEnabled, AggVersionedIngestionStats statsMock)
      throws Exception {
    return buildSit(failOnMismatchEnabled, statsMock, VersionRole.FUTURE, false, false);
  }

  /**
   * @param failOnMismatchEnabled server-level config
   *     {@code server.batch.push.record.count.verification.fail.on.mismatch.enabled} (default
   *     {@code true} in production).
   * @param versionRole role of the SIT's version: only {@link VersionRole#FUTURE} runs the
   *     verification — current/backup skip the entire check (re-emit-after-promotion safety).
   * @param hllEnabled toggles {@code uniqueIngestedKeyCountHllEnabled} on the SIT. When false the
   *     HLL leg of the dual check is bypassed (matches the existing-test path).
   * @param isDaVinciClient toggles the DaVinci skip-throw branch on the failure path.
   */
  private static StoreIngestionTask buildSit(
      boolean failOnMismatchEnabled,
      AggVersionedIngestionStats statsMock,
      VersionRole versionRole,
      boolean hllEnabled,
      boolean isDaVinciClient) throws Exception {
    Store store = mock(Store.class);
    return buildSit(failOnMismatchEnabled, statsMock, versionRole, hllEnabled, isDaVinciClient, store);
  }

  private static StoreIngestionTask buildSit(
      boolean failOnMismatchEnabled,
      AggVersionedIngestionStats statsMock,
      VersionRole versionRole,
      boolean hllEnabled,
      boolean isDaVinciClient,
      Store store) throws Exception {
    StoreIngestionTask sit = mock(StoreIngestionTask.class);
    setField(sit, "versionedIngestionStats", statsMock);
    setField(sit, "kafkaVersionTopic", TEST_TOPIC);
    setField(sit, "storeName", TEST_STORE);
    setField(sit, "versionNumber", TEST_VERSION);
    setField(sit, "uniqueIngestedKeyCountHllEnabled", hllEnabled);
    setField(sit, "isDaVinciClient", isDaVinciClient);
    setField(sit, "versionRole", versionRole);

    VeniceServerConfig serverConfigMock = mock(VeniceServerConfig.class);
    doReturn(failOnMismatchEnabled).when(serverConfigMock).isBatchPushRecordCountVerificationFailOnMismatchEnabled();
    setField(sit, "serverConfig", serverConfigMock);

    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(storeRepository).getStore(TEST_STORE);
    setField(sit, "storeRepository", storeRepository);

    PubSubTopic vt = mock(PubSubTopic.class);
    doReturn(false).when(vt).isViewTopic();
    setField(sit, "versionTopic", vt);

    doCallRealMethod().when(sit).verifyBatchPushRecordCount(any(), any());
    // verifyBatchPushRecordCount calls this helper on `this`; that self-invocation is intercepted by
    // the mock, so it must also be wired to the real implementation for the per-version gate to run.
    doCallRealMethod().when(sit).isPreExistingMigrationCloneReplay(any());
    return sit;
  }

  private static StoreIngestionTask buildSitOnViewTopic(AggVersionedIngestionStats statsMock) throws Exception {
    StoreIngestionTask sit = buildSit(true, statsMock);
    PubSubTopic vt = mock(PubSubTopic.class);
    doReturn(true).when(vt).isViewTopic();
    setField(sit, "versionTopic", vt);
    return sit;
  }

  private static Version mockVersion(long createdTimeMs) {
    Version version = mock(Version.class);
    doReturn(createdTimeMs).when(version).getCreatedTime();
    return version;
  }

  private static Store storeWithVersionMetadata(
      boolean migrationDuplicateStore,
      long storeCreatedTimeMs,
      long versionCreatedTimeMs) {
    Store store = mock(Store.class);
    doReturn(migrationDuplicateStore).when(store).isMigrationDuplicateStore();
    doReturn(storeCreatedTimeMs).when(store).getCreatedTime();
    doReturn(mockVersion(versionCreatedTimeMs)).when(store).getVersion(TEST_VERSION);
    return store;
  }

  private static Store migrationDuplicateStore(long storeCreatedTimeMs, long versionCreatedTimeMs) {
    return storeWithVersionMetadata(true, storeCreatedTimeMs, versionCreatedTimeMs);
  }

  private static PartitionConsumptionState pcsWithCount(long count) {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(count).when(pcs).getBatchPushRecordCount();
    doReturn("test_replica").when(pcs).getReplicaId();
    return pcs;
  }

  private static PartitionConsumptionState pcsWithCountAndHll(long count, long hllEstimate) {
    PartitionConsumptionState pcs = pcsWithCount(count);
    doReturn(hllEstimate).when(pcs).getEstimatedUniqueIngestedKeyCount();
    return pcs;
  }

  private static PubSubMessageHeaders headersWithPrc(long expectedCount) {
    return new PubSubMessageHeaders().add(
        PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER,
        ByteBuffer.allocate(Long.BYTES).putLong(expectedCount).array());
  }

  @Test
  public void testVerifySkipsOnNullHeaders() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), null);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifySkipsOnMissingPrcHeader() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), new PubSubMessageHeaders());
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifySkipsOnMalformedPrcHeader() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    PubSubMessageHeaders headers = new PubSubMessageHeaders()
        .add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, new byte[] { 1, 2, 3 }); // not 8 bytes
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headers);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifySkipsOnSentinelExpectedCount() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(
        pcsWithCount(100L),
        headersWithPrc(PubSubMessageHeaders.PRC_HEADER_UNAVAILABLE_SENTINEL));
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifySkipsOnViewTopic() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSitOnViewTopic(stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifySkipsOnEmptyPubSubMessageHeadersSingleton() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), EmptyPubSubMessageHeaders.SINGLETON);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * Verification only runs while the push is in progress. Once the version is current (or backup),
   * any re-emit of EOP should not re-fire the check. No metrics, no throw, even on a clear deficit.
   */
  @Test
  public void testVerifySkipsWhenNotFutureVersion() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.CURRENT, // not FUTURE — verification should skip
        /* hllEnabled */ false,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyEmitsMatchSensorOnExactCount() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyEmitsMatchSensorOnSurplus() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(105L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyEmitsMatchSensorOnZeroExpectedAndActual() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(0L), headersWithPrc(0L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /** With server-strict-mode disabled, mismatch records the metric and logs but does NOT throw. */
  @Test
  public void testVerifyEmitsMismatchSensorOnDeficitWhenStrictModeDisabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* failOnMismatchEnabled */ false, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /** With server-strict-mode enabled (default), mismatch records the metric AND throws. */
  @Test
  public void testVerifyThrowsOnDeficitWhenStrictModeEnabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* failOnMismatchEnabled */ true, stats);
    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);

    String msg = ex.getMessage();
    assertTrue(msg.contains("RECORD_COUNT_DEFICIT"), "Tagged error class missing in: " + msg);
    assertTrue(msg.contains("verificationContext=FRESH_PUSH"), "Verification context missing in: " + msg);
    assertTrue(msg.contains("expected=100"), "expected=N missing in: " + msg);
    assertTrue(msg.contains("actual=50"), "actual=M missing in: " + msg);
    assertTrue(msg.contains("replica=test_replica"), "replica id missing in: " + msg);
    assertTrue(msg.contains("topic=" + TEST_TOPIC), "topic missing in: " + msg);
    // Failed-and-throwing mismatches must also increment the dedicated failure sensor — distinct
    // from the informational mismatch sensor, which fires regardless of strict-mode state.
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigrationReplayDoesNotThrowOnCounterDeficitInStrictMode() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, PRE_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));

    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigrationReplayDoesNotThrowOnHllDeficitInStrictMode() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, PRE_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false,
        store);

    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L));

    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigratingSourceStillThrowsOnDeficit() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = mock(Store.class);
    doReturn(true).when(store).isMigrating();
    doReturn(false).when(store).isMigrationDuplicateStore();
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMissingStoreMetadataFailsClosed() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        null);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().startsWith("RECORD_COUNT_DEFICIT"));
    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigrationReplayWarningContainsContextAndReason() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, PRE_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);
    TestLogAppender appender =
        new TestLogAppender("MigrationReplayRecordCountAppender", PatternLayout.createDefaultLayout());
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(StoreIngestionTask.class);
    logger.addAppender(appender);
    try {
      sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));

      String log = appender.getLog();
      assertTrue(log.contains("verificationContext=MIGRATION_REPLAY"), "Missing migration context in: " + log);
      assertTrue(log.contains("reason=PRE_EXISTING_MIGRATION_CLONE"), "Missing migration reason in: " + log);
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test
  public void testVerifyFreshPushDuringMigrationThrowsAndRecordsFailure() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, DURING_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    String message = exception.getMessage();
    assertTrue(message.contains("verificationContext=FRESH_PUSH"), message);
    assertTrue(message.contains("migrationDuplicateStore=true"), message);
    assertTrue(message.contains("storeCreatedTimeMs=" + MIGRATION_STORE_CREATED_TIME_MS), message);
    assertTrue(message.contains("versionCreatedTimeMs=" + DURING_MIGRATION_VERSION_CREATED_TIME_MS), message);
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * Same new-push scoping applies to the HLL leg: an HLL deficit on a push begun during migration is
   * fatal, confirming the per-version gate is independent of which leg detected the deficit.
   */
  @Test
  public void testVerifyFreshPushDuringMigrationHllDeficitThrowsAndRecordsFailure() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, DURING_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false,
        store);

    // counter=100 >= 100 (passes); hll=50, |50-100|=50 > 5 (fails) -> deficit via HLL leg.
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyVersionCreatedAtStoreBoundaryThrows() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, MIGRATION_STORE_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyNonMigrationStoreWithOlderVersionRemainsStrict() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store =
        storeWithVersionMetadata(false, MIGRATION_STORE_CREATED_TIME_MS, PRE_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigrationDuplicateStoreWithUnsetVersionCreatedTimeFailsClosed() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(MIGRATION_STORE_CREATED_TIME_MS, 0L);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  @Test
  public void testVerifyMigrationDuplicateStoreWithUnsetStoreCreatedTimeFailsClosed() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    Store store = migrationDuplicateStore(0L, PRE_MIGRATION_VERSION_CREATED_TIME_MS);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        store);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  /** When server strict-mode is disabled, the dedicated failure sensor must NOT fire. */
  @Test
  public void testVerifyDoesNotEmitFailureSensorWhenStrictModeDisabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* failOnMismatchEnabled */ false, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  /**
   * Dual-check passes when both legs pass: counter ≥ expected AND |hll − expected| ≤ tolerance.
   * With expected=100 and {@code HLL_ERROR_TOLERANCE=0.05}, threshold = ceil(100 * 0.05) = 5, so
   * an HLL estimate of 98 sits |98−100|=2 ≤ 5 → HLL leg passes. Counter at 100 ≥ 100 → counter
   * leg passes. Match.
   */
  @Test
  public void testVerifyDualCheckPassesWhenBothLegsPass() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 98L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  /**
   * Dual-check fails when the HLL leg fails (under-count) even though counter passes. The HLL leg
   * catches duplicate-key inflation that the counter alone would miss. threshold = 5, |50−100|=50
   * > 5 → HLL leg fails → mismatch.
   */
  @Test
  public void testVerifyDualCheckFailsWhenHllUnderCounts() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ false,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * Symmetric: dual-check also fails when the HLL leg over-counts beyond tolerance. counter=120
   * ≥ 100 → counter passes (raw over-count is benign — dup replication / spec-exec). hll=109,
   * |109−100|=9 > 5 → HLL leg fails. Structurally HLL counts unique keys and unique keys cannot
   * exceed raw producer ops, so a >5% over-estimate signals a bug worth flagging.
   */
  @Test
  public void testVerifyDualCheckFailsWhenHllOverCounts() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ false,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(120L, 109L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * Boundary: HLL exactly at the upper edge of the tolerance window still passes. expected=100,
   * threshold=5, hll=105 → |105−100|=5 ≤ 5 → HLL leg passes. Confirms the window is inclusive on
   * both sides.
   */
  @Test
  public void testVerifyDualCheckPassesAtUpperHllToleranceBoundary() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 105L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * Dual-check fails when the counter leg fails even though HLL passes. counter=50 < 100 → counter
   * fails; hll=100 sits |100−100|=0 ≤ 5 → HLL alone would have passed. Confirms that EITHER leg
   * failing is sufficient to trigger mismatch.
   */
  @Test
  public void testVerifyDualCheckFailsWhenOnlyCounterFails() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ false,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(50L, 100L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * DaVinci, server strict-mode enabled, counter-leg deficit: both the failure sensor and the
   * throw are suppressed — DaVinci failure aggregation happens separately via the DaVinci push
   * status store. Only the informational {@code _mismatch} sensor (which fires regardless of
   * strict-mode state) is incremented.
   */
  @Test
  public void testVerifyDaVinciDoesNotThrowOnDeficitWhenStrictModeEnabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    // Should NOT throw — DaVinci skip path. Failure sensor and throw both suppressed; only the
    // informational mismatch sensor fires.
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * DaVinci, server strict-mode disabled, counter-leg deficit: only the informational mismatch
   * sensor fires — no failure sensor, no throw. Mirrors the non-DaVinci strict-mode-disabled case
   * and confirms the DaVinci skip-throw guard does not perturb the metric-only path.
   */
  @Test
  public void testVerifyDaVinciEmitsMismatchSensorOnDeficitWhenStrictModeDisabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ false,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * DaVinci, server strict-mode enabled, HLL-leg failure (counter passes): confirms the DaVinci
   * skip guard is keyed to {@code isDaVinciClient}, not to which leg failed. HLL deviation >
   * tolerance → mismatch detected; failure sensor and throw are both suppressed.
   */
  @Test
  public void testVerifyDaVinciDoesNotThrowOnHllLegFailureWhenStrictModeEnabled() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ true,
        /* isDaVinciClient */ true);
    // counter=100 ≥ 100 (passes); hll=50, |50−100|=50 > 5 (fails) → mismatch.
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
  }

  /**
   * DaVinci, match path: a clean push still records the match sensor and does not trip the
   * mismatch/failure sensors. Sanity check that DaVinci doesn't accidentally skip the match
   * recording.
   */
  @Test
  public void testVerifyDaVinciEmitsMatchSensorOnExactCount() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  /**
   * DaVinci, not-future-version: the future-version gate runs before the DaVinci branch, so an
   * already-current version on DaVinci skips the entire verification — no metrics, no throw.
   */
  @Test
  public void testVerifyDaVinciSkipsWhenNotFutureVersion() throws Exception {
    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.CURRENT,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  private static ZKStore migrationDuplicateZkStore(long createdTimeMs) {
    ZKStore store = new ZKStore(
        TEST_STORE,
        "owner",
        createdTimeMs,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    store.setMigrationDuplicateStore(true);
    return store;
  }

  private static Version versionWithCreatedTime(long createdTimeMs, String pushJobId) {
    return new VersionImpl(TEST_STORE, TEST_VERSION, createdTimeMs, pushJobId, 1, new PartitionerConfigImpl(), null);
  }

  /**
   * Realistic-metadata proof (real {@link ZKStore} / {@link VersionImpl}, no mock store): a
   * pre-existing source version cloned onto the destination keeps its original createdTime via
   * {@link Version#cloneVersion()}, so it precedes the destination store's createdTime and its
   * compacted-replay deficit is nonfatal.
   */
  @Test
  public void testRealMetadataMigrationCloneReplayIsNonfatal() throws Exception {
    long migrationStart = 100_000L;
    Version sourceVersion = versionWithCreatedTime(migrationStart - 50_000L, "push-src");
    Version clonedVersion = sourceVersion.cloneVersion();
    // Sanity: the clone preserves the source's pre-migration createdTime (the invariant this fix relies on).
    assertEquals(clonedVersion.getCreatedTime(), migrationStart - 50_000L);

    ZKStore destinationStore = migrationDuplicateZkStore(migrationStart);
    destinationStore.forceAddVersion(clonedVersion, true);

    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        destinationStore);

    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));

    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, never()).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }

  /**
   * Realistic-metadata proof (real {@link ZKStore} / {@link VersionImpl}, no mock store): a push
   * started while migration is active is a fresh version created AFTER the destination store, so its
   * createdTime does not precede the store's and the deficit stays fatal with the failure sensor.
   */
  @Test
  public void testRealMetadataFreshPushDuringMigrationIsFatal() throws Exception {
    long migrationStart = 100_000L;
    ZKStore destinationStore = migrationDuplicateZkStore(migrationStart);
    Version newPushVersion = versionWithCreatedTime(migrationStart + 50_000L, "push-new");
    destinationStore.forceAddVersion(newPushVersion, false);

    AggVersionedIngestionStats stats = mock(AggVersionedIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* failOnMismatchEnabled */ true,
        stats,
        VersionRole.FUTURE,
        /* hllEnabled */ false,
        /* isDaVinciClient */ false,
        destinationStore);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));

    assertTrue(exception.getMessage().contains("verificationContext=FRESH_PUSH"), exception.getMessage());
    verify(stats, times(1)).recordBatchPushRecordCountMismatch(TEST_STORE, TEST_VERSION);
    verify(stats, times(1)).recordRecordCountMismatchFailure(TEST_STORE, TEST_VERSION);
  }
}
