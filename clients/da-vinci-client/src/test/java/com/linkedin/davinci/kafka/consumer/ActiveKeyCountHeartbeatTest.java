package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.findMethod;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.setField;
import static com.linkedin.venice.offsets.OffsetRecord.ACTIVE_KEY_COUNT_NOT_TRACKED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for the leader-active-key-count heartbeat header pipeline added to
 * {@link LeaderFollowerStoreIngestionTask} and {@link StoreIngestionTask}.
 *
 * Covers four concerns:
 * <ol>
 *   <li>Encode/decode of the 8-byte {@code lkc} header value.</li>
 *   <li>{@link LeaderFollowerStoreIngestionTask#buildLeaderActiveKeyCountHeader} gating — feature off,
 *       pre-EOP, untracked count, and the happy path that emits the header.</li>
 *   <li>{@link LeaderFollowerStoreIngestionTask#compareLeaderActiveKeyCountOnHeartbeat} branches —
 *       feature off, pre-EOP, follower not tracking, absent header, leader count sentinel, matching counts,
 *       mismatched counts, and a corrupt header length.</li>
 *   <li>{@code sendIngestionHeartbeatToVT} ordering — the lkc header attachment must be chained behind
 *       {@code lastVTProduceCallFuture} so in-flight worker writes have already queued their kcs signals
 *       before the leader reads the count.</li>
 * </ol>
 */
public class ActiveKeyCountHeartbeatTest {
  private static final long SAMPLE_FOLLOWER_COUNT = 12345L;

  /* ---------- 1. encode / decode roundtrip ---------- */

  @DataProvider(name = "headerLongValues")
  public Object[][] headerLongValues() {
    /*
     * decodeLeaderKeyCountHeaderValue now throws on wrong-length payloads rather than returning a numeric
     * sentinel, so every long (including Long.MIN_VALUE and Long.MAX_VALUE) must round-trip cleanly.
     */
    return new Object[][] { { 0L }, { 1L }, { 100L }, { 1_000_000L }, { Long.MAX_VALUE },
        { ACTIVE_KEY_COUNT_NOT_TRACKED }, { Long.MIN_VALUE } };
  }

  @Test(dataProvider = "headerLongValues")
  public void testHeaderEncodeDecodeRoundtrip(long value) {
    byte[] bytes = StoreIngestionTask.encodeLeaderKeyCountHeaderValue(value);
    assertEquals(bytes.length, Long.BYTES, "lkc header must always be 8 bytes");
    PubSubMessageHeader header = new PubSubMessageHeader(PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER, bytes);
    assertEquals(StoreIngestionTask.decodeLeaderKeyCountHeaderValue(header), value);
  }

  @Test
  public void testDecodeNullHeaderReturnsNotTracked() {
    assertEquals(StoreIngestionTask.decodeLeaderKeyCountHeaderValue(null), ACTIVE_KEY_COUNT_NOT_TRACKED);
  }

  @Test
  public void testDecodeHeaderWithNullValueReturnsNotTracked() {
    PubSubMessageHeader header = new PubSubMessageHeader(PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER, null);
    assertEquals(StoreIngestionTask.decodeLeaderKeyCountHeaderValue(header), ACTIVE_KEY_COUNT_NOT_TRACKED);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecodeWrongLengthHeaderThrows() {
    PubSubMessageHeader header =
        new PubSubMessageHeader(PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER, new byte[] { 1, 2, 3 });
    StoreIngestionTask.decodeLeaderKeyCountHeaderValue(header);
  }

  /* ---------- 2. buildLeaderActiveKeyCountHeader gating ---------- */

  @Test
  public void testBuildReturnsNullWhenConsistencyCheckDisabled() throws Exception {
    assertBuildReturnsNull(false, true, 100L, false);
  }

  @Test
  public void testBuildReturnsNullPreEop() throws Exception {
    assertBuildReturnsNull(true, false, 100L, false);
  }

  @Test
  public void testBuildReturnsNullWhenCountUntracked() throws Exception {
    assertBuildReturnsNull(true, true, ACTIVE_KEY_COUNT_NOT_TRACKED, false);
  }

  @Test
  public void testBuildReturnsNullWhenPcsNull() throws Exception {
    assertBuildReturnsNull(true, true, 0L /* unused */, true);
  }

  @Test
  public void testBuildReturnsEncodedHeaderHappyPath() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    PartitionConsumptionState pcs = mockPcs(true, 42L);
    PubSubMessageHeader header = task.buildLeaderActiveKeyCountHeader(pcs);
    assertNotNull(header);
    assertEquals(header.key(), PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER);
    assertEquals(StoreIngestionTask.decodeLeaderKeyCountHeaderValue(header), 42L);
  }

  /* ---------- 3. compareLeaderActiveKeyCountOnHeartbeat branches ---------- */

  @Test
  public void testCompareNoOpWhenConsistencyCheckDisabled() throws Exception {
    // Even with a mismatch in the lkc header, the disabled flag short-circuits before reading the header.
    assertCompareIsNoOp(false, true, SAMPLE_FOLLOWER_COUNT, headersWithLkc(SAMPLE_FOLLOWER_COUNT + 1));
  }

  @Test
  public void testCompareNoOpPreEop() throws Exception {
    assertCompareIsNoOp(true, false, SAMPLE_FOLLOWER_COUNT, headersWithLkc(SAMPLE_FOLLOWER_COUNT + 1));
  }

  @Test
  public void testCompareNoOpWhenFollowerNotTracking() throws Exception {
    assertCompareIsNoOp(true, true, ACTIVE_KEY_COUNT_NOT_TRACKED, headersWithLkc(100L));
  }

  @Test
  public void testCompareNoOpWhenLkcHeaderAbsent() throws Exception {
    // Headers exist but no lkc — legacy / disabled leader scenario.
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader("some-other", new byte[] { 1 }));
    assertCompareIsNoOp(true, true, SAMPLE_FOLLOWER_COUNT, headers);
  }

  @Test
  public void testCompareNoOpWhenLeaderCountSentinel() throws Exception {
    assertCompareIsNoOp(true, true, SAMPLE_FOLLOWER_COUNT, headersWithLkc(ACTIVE_KEY_COUNT_NOT_TRACKED));
  }

  @Test
  public void testCompareMatchingCountsNoInvalidation() throws Exception {
    assertCompareIsNoOp(true, true, SAMPLE_FOLLOWER_COUNT, headersWithLkc(SAMPLE_FOLLOWER_COUNT));
  }

  @Test
  public void testCompareMismatchRecordsMismatchMetricWithoutInvalidating() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    PartitionConsumptionState pcs = mockPcs(true, SAMPLE_FOLLOWER_COUNT);
    long leaderCount = SAMPLE_FOLLOWER_COUNT + 7;
    DefaultPubSubMessage record = mockHeartbeatRecord(headersWithLkc(leaderCount));

    task.compareLeaderActiveKeyCountOnHeartbeat(pcs, record);

    /*
     * Mismatch is diagnostic-only: the dedicated cross-replica mismatch helper fires (which bumps the new
     * counter and emits a WARN log), but no invalidation happens and the follower's count is left untouched.
     */
    verify(task).recordActiveKeyCountMismatchAcrossReplicas(pcs, leaderCount, SAMPLE_FOLLOWER_COUNT);
    verifyNoInvalidation(task);
    verify(pcs, never()).setActiveKeyCount(anyLong());
  }

  @Test
  public void testCompareCorruptHeaderInvalidates() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    PartitionConsumptionState pcs = mockPcs(true, SAMPLE_FOLLOWER_COUNT);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER, new byte[] { 1, 2, 3 }));
    DefaultPubSubMessage record = mockHeartbeatRecord(headers);

    task.compareLeaderActiveKeyCountOnHeartbeat(pcs, record);
    verify(task)
        .invalidateActiveKeyCount(pcs, ActiveKeyCountInvalidationReason.CORRUPT_LEADER_KEY_COUNT_HEADER_LENGTH, 3);
  }

  /* ---------- 3b. Operator override: consistency check disabled while tracking is on ---------- */

  /**
   * Exercises the realistic operator-disable scenario: the underlying active-key-count tracking is on
   * (so the follower has a real count and the leader's count is real too), but the HB consistency check
   * has been turned off via {@code SERVER_ACTIVE_KEY_COUNT_REPLICA_CONSISTENCY_CHECK_ENABLED=false}. Neither side
   * should do anything consistency-check-related — leader skips the header, follower skips comparing — even if a
   * stale {@code lkc} header arrived on the wire from a peer that still has the check on.
   */
  @Test
  public void testLeaderBuildSkippedWhenOperatorDisablesCheckButTrackingIsOn() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true /* tracking */, false /* check off */);
    assertNull(task.buildLeaderActiveKeyCountHeader(mockPcs(true, 100L)));
  }

  @Test
  public void testFollowerCompareSkippedWhenOperatorDisablesCheckButTrackingIsOn() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true /* tracking */, false /* check off */);
    PartitionConsumptionState pcs = mockPcs(true, SAMPLE_FOLLOWER_COUNT);
    // Stale peer that still has the check on attaches a mismatching count header — follower must NOT invalidate.
    DefaultPubSubMessage record = mockHeartbeatRecord(headersWithLkc(SAMPLE_FOLLOWER_COUNT + 99));

    task.compareLeaderActiveKeyCountOnHeartbeat(pcs, record);
    verifyNoInvalidation(task);
  }

  /* ---------- 4. ActiveKeyCountInvalidationReason message formatting ---------- */

  @Test
  public void testCorruptLakcReasonRendersLength() {
    String msg = ActiveKeyCountInvalidationReason.CORRUPT_LEADER_KEY_COUNT_HEADER_LENGTH.getMessage(5);
    assertTrue(msg.contains("length=5"), "Expected 'length=5' in message; got: " + msg);
  }

  /* ---------- Test helpers ---------- */

  /**
   * Mock a {@link LeaderFollowerStoreIngestionTask} with both gating flags reflectively set. Default is to set
   * tracking and consistency-check together (production wires them in lockstep — the check is computed as
   * {@code activeKeyCountForHybridStoreEnabled && serverConfig.isActiveKeyCountReplicaConsistencyCheckEnabled()}).
   * The 2-arg overload exposes them independently for the operator-disable scenario (tracking on, check off).
   *
   * <p>A/A is transitively implied by both flags via the SIT constructor and is therefore not a test parameter.
   * {@code invalidateActiveKeyCount} overloads are left as Mockito-default no-ops so we can
   * {@link org.mockito.Mockito#verify} them.
   */
  private static LeaderFollowerStoreIngestionTask mockedTask(boolean consistencyCheckEnabled) throws Exception {
    return mockedTask(consistencyCheckEnabled, consistencyCheckEnabled);
  }

  private static LeaderFollowerStoreIngestionTask mockedTask(boolean trackingEnabled, boolean consistencyCheckEnabled)
      throws Exception {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).buildLeaderActiveKeyCountHeader(any());
    doCallRealMethod().when(task).compareLeaderActiveKeyCountOnHeartbeat(any(), any());
    setField(task, "activeKeyCountForHybridStoreEnabled", trackingEnabled);
    setField(task, "activeKeyCountReplicaConsistencyCheckEnabled", consistencyCheckEnabled);
    return task;
  }

  private static PartitionConsumptionState mockPcs(boolean postEop, long activeKeyCount) {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(postEop).when(pcs).isEndOfPushReceived();
    doReturn(activeKeyCount).when(pcs).getActiveKeyCount();
    return pcs;
  }

  private static PubSubMessageHeaders headersWithLkc(long encodedCount) {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(
        new PubSubMessageHeader(
            PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER,
            StoreIngestionTask.encodeLeaderKeyCountHeaderValue(encodedCount)));
    return headers;
  }

  private static DefaultPubSubMessage mockHeartbeatRecord(PubSubMessageHeaders headers) {
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    doReturn(headers).when(record).getPubSubMessageHeaders();
    return record;
  }

  /** Reflective handle to the package-private {@code sendIngestionHeartbeatToVT}; identical across ordering tests. */
  private static Method findSendVtMethod() throws Exception {
    Method sendVt = findMethod(
        LeaderFollowerStoreIngestionTask.class,
        "sendIngestionHeartbeatToVT",
        PartitionConsumptionState.class,
        PubSubTopicPartition.class,
        PubSubProducerCallback.class,
        LeaderMetadataWrapper.class,
        LeaderCompleteState.class,
        long.class);
    sendVt.setAccessible(true);
    return sendVt;
  }

  /**
   * Drives a {@code compareLeaderActiveKeyCountOnHeartbeat} call and asserts no invalidation fired.
   * Used by every "skip" branch in section 3 so each test method stays a single-line declaration of its inputs.
   */
  private static void assertCompareIsNoOp(
      boolean consistencyCheckEnabled,
      boolean postEop,
      long followerCount,
      PubSubMessageHeaders headers) throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(consistencyCheckEnabled);
    PartitionConsumptionState pcs = mockPcs(postEop, followerCount);
    DefaultPubSubMessage record = mockHeartbeatRecord(headers);
    task.compareLeaderActiveKeyCountOnHeartbeat(pcs, record);
    verifyNoInvalidation(task);
  }

  /**
   * Drives a {@code buildLeaderActiveKeyCountHeader} call and asserts {@code null} was returned.
   * Used by every "skip" branch in section 2.
   */
  private static void assertBuildReturnsNull(
      boolean consistencyCheckEnabled,
      boolean postEop,
      long activeKeyCount,
      boolean pcsNull) throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(consistencyCheckEnabled);
    PartitionConsumptionState pcs = pcsNull ? null : mockPcs(postEop, activeKeyCount);
    assertNull(task.buildLeaderActiveKeyCountHeader(pcs));
  }

  private static void verifyNoInvalidation(LeaderFollowerStoreIngestionTask task) {
    verify(task, never())
        .invalidateActiveKeyCount(any(PartitionConsumptionState.class), any(ActiveKeyCountInvalidationReason.class));
    verify(task, never()).invalidateActiveKeyCount(
        any(PartitionConsumptionState.class),
        any(ActiveKeyCountInvalidationReason.class),
        anyInt());
    verify(task, never()).invalidateActiveKeyCount(
        any(PartitionConsumptionState.class),
        any(ActiveKeyCountInvalidationReason.class),
        any(Throwable.class));
  }

  /* ---------- 5. sendIngestionHeartbeatToVT ordering w.r.t. lastVTProduceCallFuture ---------- */

  /**
   * Regression test for the chain ordering in {@code sendIngestionHeartbeatToVT}: when
   * {@code activeKeyCountReplicaConsistencyCheckEnabled} is on, the HB SEND must be deferred behind
   * {@code partitionConsumptionState.getLastVTProduceCallFuture()}, but the lkc COUNT must be captured
   * synchronously at swap-time (NOT in the deferred callback). Reading the count from inside the callback
   * would observe SIT-thread increments for records queued AFTER this HB's swap — those records chain
   * behind the HB at the broker but their increments already landed in the AtomicLong by the time the
   * callback fires, so the attached lkc header would reflect state the follower has not yet reached at
   * HB-process time, causing false-positive mismatches.
   *
   * <p>This test installs an incomplete {@code lastVTProduceCallFuture} on the PCS, invokes
   * {@code sendIngestionHeartbeatToVT}, asserts (a) the count IS read synchronously once (swap-time
   * capture) and (b) the writer is NOT called yet (SEND is deferred). Then it completes the future and
   * asserts the writer is called with the swap-time count attached as a header.
   */
  @Test
  public void testSendIngestionHeartbeatToVTOrdersBehindLastVtProduceCallFuture() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    Method sendVt = findSendVtMethod();

    // PCS with an incomplete previous VT produce future — simulates an in-flight worker write.
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(99L).when(pcs).getActiveKeyCount();
    /*
     * The atomic swap returns the previous head and installs the new one in one call. Stub it to return
     * the incomplete previousVtWrite — that's what the chained callback will wait on.
     */
    CompletableFuture<Void> previousVtWrite = new CompletableFuture<>();
    doReturn(previousVtWrite).when(pcs).swapLastVTProduceCallFuture(any());

    // VeniceWriter mock that returns a completed produce result so the inner sendHeartbeat call doesn't trip.
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
    doReturn(CompletableFuture.completedFuture(produceResult)).when(writer)
        .sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), any(PubSubMessageHeader.class));
    doReturn(Lazy.of(() -> writer)).when(pcs).getVeniceWriterLazyRef();

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    sendVt.invoke(
        task,
        pcs,
        topicPartition,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        LeaderCompleteState.LEADER_COMPLETED,
        123L);

    /*
     * Pre-completion: the count IS read synchronously on the SIT thread at swap-time (so it reflects only
     * records queued up to this point — see buildLeaderActiveKeyCountHeader's Javadoc). times(1)
     * locks the swap-time read in: a future regression that re-reads the count inside the chained callback
     * would observe two invocations and fail the test. The writer is NOT called yet because the chained
     * callback waits on previousVtWrite.
     */
    verify(pcs, times(1)).getActiveKeyCount();
    verify(writer, never())
        .sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), any(PubSubMessageHeader.class));

    // PCS must have had its lastVTProduceCallFuture rotated to a fresh future via the atomic swap.
    ArgumentCaptor<CompletableFuture<Void>> futureCap = ArgumentCaptor.forClass(CompletableFuture.class);
    verify(pcs).swapLastVTProduceCallFuture(futureCap.capture());
    CompletableFuture<Void> hbFuture = futureCap.getValue();
    assertNotSame(hbFuture, previousVtWrite, "Swapped-in future must be a fresh instance");
    assertFalse(hbFuture.isDone(), "HB future must remain incomplete until the chained callback fires");

    // Release the previous produce — the chained callback should now call the writer with the captured lkc.
    previousVtWrite.complete(null);
    // The whenCompleteAsync runs on the ForkJoinPool common pool; block on the HB future for determinism.
    hbFuture.get();

    ArgumentCaptor<PubSubMessageHeader> headerCap = ArgumentCaptor.forClass(PubSubMessageHeader.class);
    verify(writer).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), headerCap.capture());
    PubSubMessageHeader lkcHeader = headerCap.getValue();
    assertNotNull(lkcHeader, "Expected lkc header to be attached after ordering completes");
    assertEquals(lkcHeader.key(), PubSubMessageHeaders.VENICE_LEADER_KEY_COUNT_HEADER);
    assertEquals(StoreIngestionTask.decodeLeaderKeyCountHeaderValue(lkcHeader), 99L);
  }

  /**
   * Companion to the ordering test: when the consistency check is disabled the synchronous path must be preserved —
   * we must NOT touch {@code lastVTProduceCallFuture} (rotating it on every HB would introduce a needless
   * dependency for stores that opted out).
   */
  @Test
  public void testSendIngestionHeartbeatToVTDoesNotChainWhenConsistencyCheckDisabled() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(false);
    Method sendVt = findSendVtMethod();

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    doReturn(CompletableFuture.completedFuture(mock(PubSubProduceResult.class))).when(writer)
        .sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), any());
    doReturn(Lazy.of(() -> writer)).when(pcs).getVeniceWriterLazyRef();

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    sendVt.invoke(
        task,
        pcs,
        topicPartition,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        LeaderCompleteState.LEADER_COMPLETED,
        123L);

    // Sync path: writer was invoked with extraHeader=null, lastVTProduceCallFuture was neither read nor swapped.
    verify(writer).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), isNull());
    verify(pcs, never()).getLastVTProduceCallFuture();
    verify(pcs, never()).swapLastVTProduceCallFuture(any());
  }

  /**
   * Regression test for the upstream-exception propagation fix in {@code sendIngestionHeartbeatToVT}'s
   * chained callback. If the previous VT write failed (e.g., close short-circuited the head, or a prior
   * data write blew up), the HB callback must NOT emit a heartbeat — its lkc would reflect records that
   * never reached VT and would trigger false-positive divergence on followers. The chain's failure must
   * propagate to hbProduceFuture instead, so anyone chained behind us fails fast too.
   */
  @Test
  public void testSendIngestionHeartbeatToVTPropagatesPreviousFailureWithoutEmittingHeartbeat() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    Method sendVt = findSendVtMethod();

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(77L).when(pcs).getActiveKeyCount();
    CompletableFuture<Void> previousVtWrite = new CompletableFuture<>();
    doReturn(previousVtWrite).when(pcs).swapLastVTProduceCallFuture(any());

    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    doReturn(Lazy.of(() -> writer)).when(pcs).getVeniceWriterLazyRef();

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    sendVt.invoke(
        task,
        pcs,
        topicPartition,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        LeaderCompleteState.LEADER_COMPLETED,
        123L);

    // Capture the swapped-in future and fail the previous one.
    ArgumentCaptor<CompletableFuture<Void>> futureCap = ArgumentCaptor.forClass(CompletableFuture.class);
    verify(pcs).swapLastVTProduceCallFuture(futureCap.capture());
    CompletableFuture<Void> hbFuture = futureCap.getValue();
    RuntimeException upstream = new RuntimeException("prior write blew up");
    previousVtWrite.completeExceptionally(upstream);

    // Block on the chain future and assert it carries the propagated exception.
    try {
      hbFuture.get(5, TimeUnit.SECONDS);
      throw new AssertionError("hbFuture should have completed exceptionally");
    } catch (java.util.concurrent.ExecutionException expected) {
      assertEquals(expected.getCause(), upstream);
    }

    /*
     * HB must NOT have been emitted — the writer is never called when the prior chain entry failed. The
     * count IS read once at swap-time (synchronous SIT-thread capture for the lkc header), but that read
     * is harmless: nothing was sent.
     */
    verify(writer, never()).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), any());
  }

  /**
   * Regression test for the chained callback in {@link LeaderFollowerStoreIngestionTask#sendIngestionHeartbeatToVT}.
   * When {@code writer.sendHeartbeat} throws synchronously, sendIngestionHeartbeat catches and wraps the cause
   * into the returned future — the chain head must reflect that synchronous-enqueue failure rather than
   * unconditionally completing successfully. Otherwise downstream chained writes proceed under the false
   * assumption that the HB enqueued, defeating the lkc ordering invariant.
   *
   * <p>Stub {@code topicPartition.getPubSubTopic()} with a real PubSubTopic mock so {@code logIngestionHeartbeat}
   * completes normally instead of NPE'ing — the test must exercise the actual failure-propagation path inside
   * the chained callback, not rely on an incidental NPE from an under-stubbed mock to make the assertion fire.
   */
  @Test
  public void testSendIngestionHeartbeatToVTCompletesChainExceptionallyWhenSendHeartbeatThrows() throws Exception {
    LeaderFollowerStoreIngestionTask task = mockedTask(true);
    Method sendVt = findSendVtMethod();

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(99L).when(pcs).getActiveKeyCount();
    CompletableFuture<Void> previousVtWrite = CompletableFuture.completedFuture(null);
    doReturn(previousVtWrite).when(pcs).swapLastVTProduceCallFuture(any());

    // Writer.sendHeartbeat throws synchronously — simulates an in-bad-state VeniceWriter.
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    RuntimeException sendFailure = new RuntimeException("writer is closed");
    org.mockito.Mockito.doThrow(sendFailure)
        .when(writer)
        .sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong(), any(PubSubMessageHeader.class));
    doReturn(Lazy.of(() -> writer)).when(pcs).getVeniceWriterLazyRef();

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    PubSubTopic stubTopic = mock(PubSubTopic.class);
    doReturn("test-store_v1").when(stubTopic).getName();
    doReturn(stubTopic).when(topicPartition).getPubSubTopic();
    doReturn(0).when(topicPartition).getPartitionNumber();
    sendVt.invoke(
        task,
        pcs,
        topicPartition,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        LeaderCompleteState.LEADER_COMPLETED,
        123L);

    ArgumentCaptor<CompletableFuture<Void>> futureCap = ArgumentCaptor.forClass(CompletableFuture.class);
    verify(pcs).swapLastVTProduceCallFuture(futureCap.capture());
    CompletableFuture<Void> hbFuture = futureCap.getValue();
    /*
     * Block on the chain head FIRST — the inner callback runs on the common pool when previousVtWrite is
     * already complete (we stubbed it to completedFuture(null)), so it may not have fired yet when sendVt
     * returns. Then assert the cause is the actual sync writer failure (not an incidental NPE from an
     * under-stubbed mock — that pattern hid this very bug for an iteration).
     */
    try {
      hbFuture.get(5, TimeUnit.SECONDS);
      throw new AssertionError("hbFuture should have completed exceptionally");
    } catch (java.util.concurrent.ExecutionException expected) {
      assertEquals(expected.getCause(), sendFailure, "Chain head must surface the sync writer.sendHeartbeat failure");
    }
    assertTrue(hbFuture.isCompletedExceptionally(), "Chain head must reach a terminal exceptional state");
  }
}
