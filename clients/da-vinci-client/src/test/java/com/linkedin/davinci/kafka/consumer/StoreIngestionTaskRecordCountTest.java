package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.setField;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class StoreIngestionTaskRecordCountTest {
  private static final String TEST_TOPIC = "test_store_v1";
  private static final String TEST_STORE = "test_store";
  // Parsed version from TEST_TOPIC is 1. Future-version means store.getCurrentVersion() < 1, so 0
  // marks "this version is the future version" (verification runs) and 1+ marks "this version is
  // already current-or-older" (verification skips).
  private static final int CURRENT_VERSION_PUSH_IN_PROGRESS = 0;
  private static final int CURRENT_VERSION_PROMOTED = 1;

  private static StoreIngestionTask buildSit(boolean verificationEnabled, HostLevelIngestionStats statsMock)
      throws Exception {
    return buildSit(verificationEnabled, statsMock, CURRENT_VERSION_PUSH_IN_PROGRESS, false, false);
  }

  /**
   * @param verificationEnabled per-store flag {@code batchPushRecordCountVerificationEnabled}
   * @param storeCurrentVersion value returned by {@code store.getCurrentVersion()}. Drives
   *     {@code Utils.isFutureVersion(TEST_TOPIC, repo)}: 0 → push-in-progress (verification runs);
   *     1+ → already current/backup (verification skips).
   * @param hllEnabled toggles {@code uniqueIngestedKeyCountHllEnabled} on the SIT. When false the
   *     HLL leg of the dual check is bypassed (matches the existing-test path).
   * @param isDaVinciClient toggles the DaVinci skip-throw branch on the failure path.
   */
  private static StoreIngestionTask buildSit(
      boolean verificationEnabled,
      HostLevelIngestionStats statsMock,
      int storeCurrentVersion,
      boolean hllEnabled,
      boolean isDaVinciClient) throws Exception {
    StoreIngestionTask sit = mock(StoreIngestionTask.class);
    setField(sit, "hostLevelIngestionStats", statsMock);
    setField(sit, "kafkaVersionTopic", TEST_TOPIC);
    setField(sit, "storeName", TEST_STORE);
    setField(sit, "uniqueIngestedKeyCountHllEnabled", hllEnabled);
    setField(sit, "isDaVinciClient", isDaVinciClient);

    Store storeMock = mock(Store.class);
    doReturn(verificationEnabled).when(storeMock).isBatchPushRecordCountVerificationEnabled();
    doReturn(storeCurrentVersion).when(storeMock).getCurrentVersion();
    ReadOnlyStoreRepository repoMock = mock(ReadOnlyStoreRepository.class);
    doReturn(storeMock).when(repoMock).getStoreOrThrow(TEST_STORE);
    // Utils.isFutureVersion uses getStore (returns null instead of throwing).
    doReturn(storeMock).when(repoMock).getStore(TEST_STORE);
    setField(sit, "storeRepository", repoMock);

    PubSubTopic vt = mock(PubSubTopic.class);
    doReturn(false).when(vt).isViewTopic();
    setField(sit, "versionTopic", vt);

    doCallRealMethod().when(sit).verifyBatchPushRecordCount(any(), any());
    return sit;
  }

  private static StoreIngestionTask buildSitOnViewTopic(HostLevelIngestionStats statsMock) throws Exception {
    StoreIngestionTask sit = buildSit(true, statsMock);
    PubSubTopic vt = mock(PubSubTopic.class);
    doReturn(true).when(vt).isViewTopic();
    setField(sit, "versionTopic", vt);
    return sit;
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
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), null);
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifySkipsOnMissingPrcHeader() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), new PubSubMessageHeaders());
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifySkipsOnMalformedPrcHeader() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    PubSubMessageHeaders headers = new PubSubMessageHeaders()
        .add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, new byte[] { 1, 2, 3 }); // not 8 bytes
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headers);
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifySkipsOnSentinelExpectedCount() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headersWithPrc(-1L));
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifySkipsOnViewTopic() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSitOnViewTopic(stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifySkipsOnEmptyPubSubMessageHeadersSingleton() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), EmptyPubSubMessageHeaders.SINGLETON);
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  /**
   * Verification only runs while the push is in progress. Once the version is current (or backup),
   * any re-emit of EOP should not re-fire the check. No metrics, no throw, even on a clear deficit.
   */
  @Test
  public void testVerifySkipsWhenNotFutureVersion() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PROMOTED, // current >= this version → Utils.isFutureVersion returns false
        /* hllEnabled */ false,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
  }

  @Test
  public void testVerifyEmitsMatchSensorOnExactCount() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(100L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifyEmitsMatchSensorOnSurplus() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(105L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testVerifyEmitsMatchSensorOnZeroExpectedAndActual() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(true, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(0L), headersWithPrc(0L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch();
  }

  /** With store-level flag disabled (Phase 1 default), mismatch records the metric and logs but does NOT throw. */
  @Test
  public void testVerifyEmitsMismatchSensorOnDeficitWhenStoreFlagDisabled() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* verificationEnabled */ false, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /** With store-level flag enabled (per-store opt-in), mismatch records the metric AND throws. */
  @Test
  public void testVerifyThrowsOnDeficitWhenStoreFlagEnabled() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* verificationEnabled */ true, stats);
    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordBatchPushRecordCountMatch();

    String msg = ex.getMessage();
    assertTrue(msg.contains("RECORD_COUNT_DEFICIT"), "Tagged error class missing in: " + msg);
    assertTrue(msg.contains("expected=100"), "expected=N missing in: " + msg);
    assertTrue(msg.contains("actual=50"), "actual=M missing in: " + msg);
    assertTrue(msg.contains("replica=test_replica"), "replica id missing in: " + msg);
    assertTrue(msg.contains("topic=" + TEST_TOPIC), "topic missing in: " + msg);
    // Phase 2: failed-and-throwing mismatches must also increment the dedicated failure sensor —
    // distinct from the informational mismatch sensor, which fires regardless of flag state.
    verify(stats, times(1)).recordRecordCountMismatchFailure();
  }

  /** When the per-store flag is disabled, the dedicated failure sensor must NOT fire. */
  @Test
  public void testVerifyDoesNotEmitFailureSensorWhenStoreFlagDisabled() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(/* verificationEnabled */ false, stats);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
  }

  /**
   * Dual-check passes when both legs pass: counter ≥ expected AND |hll − expected| ≤ tolerance.
   * With expected=100 and {@code HLL_ERROR_TOLERANCE=0.05}, threshold = ceil(100 * 0.05) = 5, so
   * an HLL estimate of 98 sits |98−100|=2 ≤ 5 → HLL leg passes. Counter at 100 ≥ 100 → counter
   * leg passes. Match.
   */
  @Test
  public void testVerifyDualCheckPassesWhenBothLegsPass() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 98L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
  }

  /**
   * Dual-check fails when the HLL leg fails (under-count) even though counter passes. The HLL leg
   * catches duplicate-key inflation that the counter alone would miss. threshold = 5, |50−100|=50
   * > 5 → HLL leg fails → mismatch.
   */
  @Test
  public void testVerifyDualCheckFailsWhenHllUnderCounts() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ false,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /**
   * Symmetric: dual-check also fails when the HLL leg over-counts beyond tolerance. counter=120
   * ≥ 100 → counter passes (raw over-count is benign — dup replication / spec-exec). hll=109,
   * |109−100|=9 > 5 → HLL leg fails. Structurally HLL counts unique keys and unique keys cannot
   * exceed raw producer ops, so a >5% over-estimate signals a bug worth flagging.
   */
  @Test
  public void testVerifyDualCheckFailsWhenHllOverCounts() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ false,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(120L, 109L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /**
   * Boundary: HLL exactly at the upper edge of the tolerance window still passes. expected=100,
   * threshold=5, hll=105 → |105−100|=5 ≤ 5 → HLL leg passes. Confirms the window is inclusive on
   * both sides.
   */
  @Test
  public void testVerifyDualCheckPassesAtUpperHllToleranceBoundary() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 105L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
  }

  /**
   * Dual-check fails when the counter leg fails even though HLL passes. counter=50 < 100 → counter
   * fails; hll=100 sits |100−100|=0 ≤ 5 → HLL alone would have passed. Confirms that EITHER leg
   * failing is sufficient to trigger mismatch.
   */
  @Test
  public void testVerifyDualCheckFailsWhenOnlyCounterFails() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ false,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ false);
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(50L, 100L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /**
   * DaVinci, flag enabled, counter-leg deficit: both the failure sensor and the throw are
   * suppressed — DaVinci failure aggregation happens separately via the DaVinci push status
   * store. Only the informational {@code _mismatch} sensor (which fires regardless of the
   * per-store flag) is incremented.
   */
  @Test
  public void testVerifyDaVinciDoesNotThrowOnDeficitWhenFlagEnabled() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    // Should NOT throw — DaVinci skip path. Failure sensor and throw both suppressed; only the
    // informational mismatch sensor fires.
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /**
   * DaVinci, flag enabled, HLL-leg failure (counter passes): confirms the DaVinci skip guard is
   * keyed to {@code isDaVinciClient}, not to which leg failed. HLL deviation > tolerance →
   * mismatch detected; failure sensor and throw are both suppressed.
   */
  @Test
  public void testVerifyDaVinciDoesNotThrowOnHllLegFailureWhenFlagEnabled() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PUSH_IN_PROGRESS,
        /* hllEnabled */ true,
        /* isDaVinciClient */ true);
    // counter=100 ≥ 100 (passes); hll=50, |50−100|=50 > 5 (fails) → mismatch.
    sit.verifyBatchPushRecordCount(pcsWithCountAndHll(100L, 50L), headersWithPrc(100L));
    verify(stats, times(1)).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
    verify(stats, never()).recordBatchPushRecordCountMatch();
  }

  /**
   * DaVinci, not-future-version: the future-version gate runs before the DaVinci branch, so an
   * already-current version on DaVinci skips the entire verification — no metrics, no throw.
   */
  @Test
  public void testVerifyDaVinciSkipsWhenNotFutureVersion() throws Exception {
    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    StoreIngestionTask sit = buildSit(
        /* verificationEnabled */ true,
        stats,
        CURRENT_VERSION_PROMOTED,
        /* hllEnabled */ false,
        /* isDaVinciClient */ true);
    sit.verifyBatchPushRecordCount(pcsWithCount(50L), headersWithPrc(100L)); // would otherwise fail
    verify(stats, never()).recordBatchPushRecordCountMatch();
    verify(stats, never()).recordBatchPushRecordCountMismatch();
    verify(stats, never()).recordRecordCountMismatchFailure();
  }

}
