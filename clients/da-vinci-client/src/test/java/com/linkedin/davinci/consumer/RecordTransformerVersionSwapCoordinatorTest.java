package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.LogContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


public class RecordTransformerVersionSwapCoordinatorTest {
  private static final String STORE = "test-store";
  private static final int OLD_VERSION = 3;
  private static final int NEW_VERSION = 4;
  private static final String OLD_TOPIC = Version.composeKafkaTopic(STORE, OLD_VERSION);
  private static final String NEW_TOPIC = Version.composeKafkaTopic(STORE, NEW_VERSION);
  private static final String CLIENT_REGION = "us-region-1";
  private static final String OTHER_REGION = "eu-region-1";
  private static final String DEST_A = "dc-a";
  private static final String DEST_B = "dc-b";
  private static final int TOTAL_REGIONS = 2;
  private static final long TIMEOUT_MS = 30_000L;

  private VersionSwap newVsm(
      long generationId,
      String sourceRegion,
      String destRegion,
      int oldVersion,
      int newVersion) {
    VersionSwap vs = new VersionSwap();
    vs.oldServingVersionTopic = new Utf8(Version.composeKafkaTopic(STORE, oldVersion));
    vs.newServingVersionTopic = new Utf8(Version.composeKafkaTopic(STORE, newVersion));
    vs.sourceRegion = new Utf8(sourceRegion);
    vs.destinationRegion = new Utf8(destRegion);
    vs.generationId = generationId;
    return vs;
  }

  private VersionSwap newVsm(long generationId, String sourceRegion, String destRegion) {
    return newVsm(generationId, sourceRegion, destRegion, OLD_VERSION, NEW_VERSION);
  }

  private RecordTransformerVersionSwapCoordinator newCoordinator(
      BasicConsumerStats stats,
      Map<Integer, Integer> partitionToVersionToServe,
      Set<Integer> subscribedPartitions,
      Consumer<Exception> failureSurface) {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> mockFuture = (ScheduledFuture<Object>) mock(ScheduledFuture.class);
    when(executor.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(inv -> mockFuture);
    return new RecordTransformerVersionSwapCoordinator(
        STORE,
        CLIENT_REGION,
        TOTAL_REGIONS,
        TIMEOUT_MS,
        stats,
        partitionToVersionToServe,
        subscribedPartitions,
        failureSurface,
        executor);
  }

  @Test
  public void testIsRelevantRejectsForeignRegion() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0, 1)), null);
    VersionSwap foreign = newVsm(100L, OTHER_REGION, DEST_A);
    assertFalse(coordinator.isRelevant(foreign, true, OLD_VERSION));
    assertFalse(coordinator.isRelevant(foreign, false, NEW_VERSION));
  }

  @Test
  public void testIsRelevantRejectsLegacyGenerationId() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0, 1)), null);
    VersionSwap legacy = newVsm(-1L, CLIENT_REGION, DEST_A);
    assertFalse(coordinator.isRelevant(legacy, true, OLD_VERSION));
    assertFalse(coordinator.isRelevant(legacy, false, NEW_VERSION));
  }

  @Test
  public void testIsRelevantRejectsStaleGenerationId() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap firstVsm = newVsm(100L, CLIENT_REGION, DEST_A);
    assertTrue(coordinator.isRelevant(firstVsm, true, OLD_VERSION));
    coordinator.recordCurrentVsm(firstVsm, 0, currentTransformer);
    coordinator.recordFutureVsm(firstVsm, 0, futureTransformer);

    // Different generation ID arrives mid-swap — should be rejected
    VersionSwap staleGen = newVsm(99L, CLIENT_REGION, DEST_B);
    assertFalse(coordinator.isRelevant(staleGen, true, OLD_VERSION));
    assertFalse(coordinator.isRelevant(staleGen, false, NEW_VERSION));
  }

  @Test
  public void testIsRelevantRejectsStaleTopicAfterRestart() {
    // Simulate a consumer that has already promoted partitions to v5; replaying a v3->v4 VSM
    // (e.g. read from EARLIEST after restart) must not trigger a downgrade.
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, 5);
    partitionToVersionToServe.put(1, 5);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, new HashSet<>(Arrays.asList(0, 1)), null);

    VersionSwap stale = newVsm(50L, CLIENT_REGION, DEST_A, 3, 4);
    assertFalse(coordinator.isRelevant(stale, true, 3));
    assertFalse(coordinator.isRelevant(stale, false, 4));
  }

  @Test
  public void testIsRelevantRejectsStaleRollback() {
    // Currently serving v5, rollback re-emits v4->v3 VSMs to backup version. Reject them.
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, 5);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), null);

    VersionSwap rollback = newVsm(75L, CLIENT_REGION, DEST_A, 4, 3);
    assertFalse(coordinator.isRelevant(rollback, true, 4));
    assertFalse(coordinator.isRelevant(rollback, false, 3));
  }

  @Test
  public void testIsRelevantRejectsWrongTopicForSide() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    VersionSwap vsm = newVsm(100L, CLIENT_REGION, DEST_A);

    // current side check uses oldServingVersionTopic; passing transformerVersion=NEW_VERSION fails
    assertFalse(coordinator.isRelevant(vsm, true, NEW_VERSION));
    // future side check uses newServingVersionTopic; passing transformerVersion=OLD_VERSION fails
    assertFalse(coordinator.isRelevant(vsm, false, OLD_VERSION));
    // correct alignment passes
    assertTrue(coordinator.isRelevant(vsm, true, OLD_VERSION));
    assertTrue(coordinator.isRelevant(vsm, false, NEW_VERSION));
  }

  @Test
  public void testRecordCurrentVsmAccumulatesAcrossRegions() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);

    // Two VSMs with the same source (the client's region) but different destination regions —
    // simulating the same swap propagating into each RT-region's VT.
    VersionSwap fromA = newVsm(100L, CLIENT_REGION, DEST_A);
    VersionSwap fromB = newVsm(100L, CLIENT_REGION, DEST_B);

    assertFalse(coordinator.recordCurrentVsm(fromA, 0, currentTransformer));
    assertTrue(coordinator.recordCurrentVsm(fromB, 0, currentTransformer));
    // Duplicate VSM from the same destination region must not double-count
    assertTrue(coordinator.recordCurrentVsm(fromA, 0, currentTransformer));
  }

  @Test
  public void testRecordFutureVsmAccumulatesAcrossRegions() {
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    Set<Integer> subscribedPartitions = new HashSet<>(Arrays.asList(0));
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, subscribedPartitions, null);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap fromA = newVsm(200L, CLIENT_REGION, DEST_A);
    VersionSwap fromB = newVsm(200L, CLIENT_REGION, DEST_B);

    assertFalse(coordinator.recordFutureVsm(fromA, 0, futureTransformer));
    assertTrue(coordinator.recordFutureVsm(fromB, 0, futureTransformer));
  }

  @Test
  public void testCommitSwapRequiresBothSidesAllPartitions() {
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    partitionToVersionToServe.put(1, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, new HashSet<>(Arrays.asList(0, 1)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(300L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(300L, CLIENT_REGION, DEST_B);

    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    // Partition 0 fully done; partition 1 has no observations yet
    assertFalse(coordinator.allPartitionsBothSidesComplete());

    coordinator.recordCurrentVsm(a, 1, currentTransformer);
    coordinator.recordCurrentVsm(b, 1, currentTransformer);
    coordinator.recordFutureVsm(a, 1, futureTransformer);
    // Partition 1 only has 1 future-side region — still incomplete
    assertFalse(coordinator.allPartitionsBothSidesComplete());

    coordinator.recordFutureVsm(b, 1, futureTransformer);
    assertTrue(coordinator.allPartitionsBothSidesComplete());
  }

  @Test
  public void testCommitSwapEmitsSingleSuccessMetric() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(400L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(400L, CLIENT_REGION, DEST_B);

    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    assertTrue(coordinator.allPartitionsBothSidesComplete());
    coordinator.commitSwap();

    assertEquals(partitionToVersionToServe.get(0), Integer.valueOf(NEW_VERSION));
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
    verify(futureTransformer, times(1)).resumePartitionConsumption(0);
    // Current side resume not invoked at commit — torn down by store-version-change machinery
    verify(currentTransformer, never()).resumePartitionConsumption(eq(0));
  }

  @Test
  public void testIdempotentCommitDoesNotDoubleEmit() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(500L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(500L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    coordinator.commitSwap();
    coordinator.commitSwap();
    coordinator.commitSwap();

    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    verify(futureTransformer, times(1)).resumePartitionConsumption(0);
  }

  @Test
  public void testTimeoutEmitsSuccessAndForcesCutover() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    partitionToVersionToServe.put(1, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0, 1)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(600L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(600L, CLIENT_REGION, DEST_B);
    // Partition 0 completes both sides (and gets paused on both); partition 1 has only one region
    // on each side — never completes — when timeout fires this is the realistic mid-flight state.
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    coordinator.recordCurrentVsm(a, 1, currentTransformer);
    coordinator.recordFutureVsm(a, 1, futureTransformer);

    coordinator.timeoutSwap();

    // All assigned partitions' partitionToVersionToServe is force-flipped to NEW_VERSION
    assertEquals(partitionToVersionToServe.get(0), Integer.valueOf(NEW_VERSION));
    assertEquals(partitionToVersionToServe.get(1), Integer.valueOf(NEW_VERSION));
    // Single SUCCESS metric (timeouts count as recoverable success — per plan, no separate timeout counter)
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
    // Only partition 0 was paused on future side, so only it gets resumed
    verify(futureTransformer, times(1)).resumePartitionConsumption(0);
    verify(futureTransformer, never()).resumePartitionConsumption(1);
  }

  @Test
  public void testFailSwapEmitsFailAndSurfacesViaPoll() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    AtomicReference<Exception> raised = new AtomicReference<>();
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), raised::set);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(700L, CLIENT_REGION, DEST_A);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);

    Exception cause = new RuntimeException("boom");
    coordinator.failSwap(cause);

    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
    verify(stats, never()).emitVersionSwapCountMetrics(SUCCESS);
    assertSame(raised.get(), cause);
    // Idempotent
    coordinator.failSwap(cause);
    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testHandleUnsubscribeReleasesBarrier() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    partitionToVersionToServe.put(1, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0, 1)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(800L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(800L, CLIENT_REGION, DEST_B);
    // Complete partition 0 on both sides
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    // Partition 1 still has no observations
    assertFalse(coordinator.allPartitionsBothSidesComplete());

    coordinator.handleUnsubscribe(new HashSet<>(Arrays.asList(1)));
    // After unsubscribing partition 1, the remaining set (just partition 0) is fully complete
    // → coordinator should commit via handleUnsubscribe.
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    assertEquals(partitionToVersionToServe.get(0), Integer.valueOf(NEW_VERSION));
  }

  @Test
  public void testHandleUnsubscribeResumesPausedPartitions() {
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    partitionToVersionToServe.put(1, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, new HashSet<>(Arrays.asList(0, 1)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(900L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(900L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    // Partition 0 is complete on both sides → it is in pausedCurrent / pausedFuture.

    coordinator.handleUnsubscribe(new HashSet<>(Arrays.asList(0)));
    verify(currentTransformer, times(1)).resumePartitionConsumption(0);
    verify(futureTransformer, times(1)).resumePartitionConsumption(0);
  }

  @Test
  public void testCloseCancelsTimeoutWatchdog() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> mockFuture = (ScheduledFuture<Object>) mock(ScheduledFuture.class);
    when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> mockFuture);

    RecordTransformerVersionSwapCoordinator coordinator = new RecordTransformerVersionSwapCoordinator(
        STORE,
        CLIENT_REGION,
        TOTAL_REGIONS,
        TIMEOUT_MS,
        null,
        new ConcurrentHashMap<>(),
        new HashSet<>(Arrays.asList(0)),
        null,
        executor);

    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    coordinator.recordCurrentVsm(newVsm(1000L, CLIENT_REGION, DEST_A), 0, currentTransformer);

    coordinator.shutdown();
    verify(mockFuture, times(1)).cancel(false);
    verify(executor, times(1)).shutdownNow();
  }

  @Test
  public void testMidSwapSubscribeWaitsForNextSwap() {
    // Snapshot is taken at swap-arm: late subscribers don't gate this swap.
    Set<Integer> subscribedPartitions = new HashSet<>(Arrays.asList(0));
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, subscribedPartitions, null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(1100L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(1100L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    // Partition 1 added mid-swap
    subscribedPartitions.add(1);
    partitionToVersionToServe.put(1, OLD_VERSION);
    // VSM observation on partition 1 should be ignored (not in snapshot)
    assertFalse(coordinator.recordCurrentVsm(a, 1, currentTransformer));

    // Original swap can complete with just partition 0
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    assertTrue(coordinator.allPartitionsBothSidesComplete());
    coordinator.commitSwap();
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
  }

  @Test
  public void testStaleObservationsAfterCommit() {
    // After a commit clears state, late VSMs from the same generation should be ignored
    // (because maxServedVersion now tracks the new version).
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(1200L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(1200L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    coordinator.commitSwap();

    // After commit, partitionToVersionToServe[0] = NEW_VERSION; replaying same VSM should be rejected
    assertFalse(coordinator.isRelevant(a, true, OLD_VERSION));
    assertFalse(coordinator.isRelevant(a, false, NEW_VERSION));
  }

  @Test
  public void testIsRelevantNullVsm() {
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    assertFalse(coordinator.isRelevant(null, true, OLD_VERSION));
  }

  @Test
  public void testFailureSurfaceNullSafe() {
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    coordinator.recordCurrentVsm(newVsm(1300L, CLIENT_REGION, DEST_A), 0, currentTransformer);
    // failureSurface is null — should not throw
    coordinator.failSwap(new RuntimeException("boom"));
    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testFailSwapSurfacesEvenWhenNoSwapIsArmed() {
    // A failure encountered before any VSM was accepted (e.g. NPE in isRelevant on a malformed
    // VSM) must still propagate via the failure surface so the consumer's poll() can observe it.
    // No FAIL metric is emitted because there was no swap to fail.
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    AtomicReference<Exception> raised = new AtomicReference<>();
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), raised::set);

    Exception cause = new RuntimeException("pre-arm failure");
    coordinator.failSwap(cause);

    assertSame(raised.get(), cause);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testRecordRejectsNullTopicAndRegionFields() {
    // Defensive: malformed VSMs with missing identity fields should not NPE inside the coordinator.
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, partitionToVersionToServe, new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap noOldTopic = newVsm(1400L, CLIENT_REGION, DEST_A);
    noOldTopic.oldServingVersionTopic = null;
    assertFalse(coordinator.recordCurrentVsm(noOldTopic, 0, currentTransformer));

    VersionSwap noNewTopic = newVsm(1401L, CLIENT_REGION, DEST_A);
    noNewTopic.newServingVersionTopic = null;
    assertFalse(coordinator.recordFutureVsm(noNewTopic, 0, currentTransformer));

    VersionSwap noDestRegion = newVsm(1402L, CLIENT_REGION, DEST_A);
    noDestRegion.destinationRegion = null;
    assertFalse(coordinator.recordCurrentVsm(noDestRegion, 0, currentTransformer));

    // isRelevant must also tolerate missing topics
    VersionSwap noOldTopic2 = newVsm(1403L, CLIENT_REGION, DEST_A);
    noOldTopic2.oldServingVersionTopic = null;
    assertFalse(coordinator.isRelevant(noOldTopic2, true, OLD_VERSION));
  }

  @Test
  public void testRecordRejectsUnsubscribedPartitionBeforeArming() {
    // A VSM for a partition not in subscribedPartitions must not arm the swap — otherwise an
    // early/spurious VSM would start the timeout watchdog for a doomed swap whose snapshot
    // wouldn't include the partition anyway.
    Set<Integer> subscribedPartitions = new HashSet<>(Arrays.asList(0));
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, subscribedPartitions, null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);

    // Partition 99 is not subscribed; this should be a clean no-op.
    assertFalse(coordinator.recordCurrentVsm(newVsm(1500L, CLIENT_REGION, DEST_A), 99, currentTransformer));

    // A subsequent legitimate VSM for partition 0 should still proceed as the first arming event.
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);
    VersionSwap a = newVsm(1501L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(1501L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    coordinator.commitSwap();

    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    assertEquals(partitionToVersionToServe.get(0), Integer.valueOf(NEW_VERSION));
  }

  @Test
  public void testCommitFlipsLateSubscribers() {
    // A partition subscribed after the swap is armed is not counted against the barrier (its
    // snapshot membership check fails), but flipServingVersion must still flip it to NEW_VERSION
    // at commit so future-version records on that partition aren't silently filtered.
    Set<Integer> subscribedPartitions = new HashSet<>(Arrays.asList(0));
    Map<Integer, Integer> partitionToVersionToServe = new ConcurrentHashMap<>();
    partitionToVersionToServe.put(0, OLD_VERSION);
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, partitionToVersionToServe, subscribedPartitions, null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(1600L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(1600L, CLIENT_REGION, DEST_B);

    // Arm with subscribed = {0}
    coordinator.recordCurrentVsm(a, 0, currentTransformer);

    // Late subscriber: partition 1 joins after arm
    subscribedPartitions.add(1);

    // Complete the barrier for partition 0
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);
    coordinator.commitSwap();

    // Both partitions — the snapshotted one AND the late subscriber — are flipped
    assertEquals(partitionToVersionToServe.get(0), Integer.valueOf(NEW_VERSION));
    assertEquals(partitionToVersionToServe.get(1), Integer.valueOf(NEW_VERSION));
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
  }

  @Test
  public void testTerminalFailureResumesBothSides() {
    // When flipServingVersion throws after both sides paused, the terminal-failure path must
    // resume Kafka prefetch on every paused partition — otherwise SIT continues running but
    // those partitions stay paused forever.
    BasicConsumerStats stats = mock(BasicConsumerStats.class);
    Map<Integer, Integer> throwingMap = new ConcurrentHashMap<Integer, Integer>() {
      @Override
      public Integer put(Integer key, Integer value) {
        throw new IllegalStateException("induced flip failure");
      }
    };
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(stats, throwingMap, new HashSet<>(Arrays.asList(0)), null);
    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer = mock(InternalDaVinciRecordTransformer.class);

    VersionSwap a = newVsm(1700L, CLIENT_REGION, DEST_A);
    VersionSwap b = newVsm(1700L, CLIENT_REGION, DEST_B);
    coordinator.recordCurrentVsm(a, 0, currentTransformer);
    coordinator.recordCurrentVsm(b, 0, currentTransformer);
    coordinator.recordFutureVsm(a, 0, futureTransformer);
    coordinator.recordFutureVsm(b, 0, futureTransformer);

    // commitSwap absorbs the flip failure via handleTerminalFailure and does not rethrow.
    coordinator.commitSwap();
    verify(currentTransformer, times(1)).resumePartitionConsumption(0);
    verify(futureTransformer, times(1)).resumePartitionConsumption(0);
    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testIsRelevantRejectsNonExistingTargetVersion() {
    // A malformed VSM targeting Store.NON_EXISTING_VERSION (v0) must not pass — even on an empty
    // partitionToVersionToServe (where computeMaxServedVersion() returns -1).
    RecordTransformerVersionSwapCoordinator coordinator =
        newCoordinator(null, new ConcurrentHashMap<>(), new HashSet<>(Arrays.asList(0)), null);
    // newV=0 → newTopic = "test-store_v0"; canRecord must reject.
    VersionSwap toVersionZero = newVsm(1234L, CLIENT_REGION, DEST_A, 1, 0);
    assertFalse(coordinator.isRelevant(toVersionZero, true, 1));
    assertFalse(coordinator.isRelevant(toVersionZero, false, 0));

    InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer = mock(InternalDaVinciRecordTransformer.class);
    assertFalse(coordinator.recordCurrentVsm(toVersionZero, 0, currentTransformer));
  }

  @Test
  public void testConstructorRejectsNonPositiveTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RecordTransformerVersionSwapCoordinator(
            STORE,
            CLIENT_REGION,
            TOTAL_REGIONS,
            0L,
            null,
            new ConcurrentHashMap<>(),
            new HashSet<>(),
            null,
            (LogContext) null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new RecordTransformerVersionSwapCoordinator(
            STORE,
            CLIENT_REGION,
            TOTAL_REGIONS,
            -1L,
            null,
            new ConcurrentHashMap<>(),
            new HashSet<>(),
            null,
            (LogContext) null));
  }
}
