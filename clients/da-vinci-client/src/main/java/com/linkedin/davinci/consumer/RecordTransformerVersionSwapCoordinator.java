package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Coordinates active-active aware version swaps for the DaVinciRecordTransformer-based CDC consumer.
 *
 * <p>One coordinator instance is owned by each {@link VeniceChangelogConsumerDaVinciRecordTransformerImpl}
 * when {@link ChangelogClientConfig#isVersionSwapByControlMessageEnabled()} is true. It enforces a
 * cross-partition, cross-region barrier so that a swap is only committed once every assigned partition
 * has observed the version swap message (VSM) from every region on both the old (current-serving) and
 * new (future) version topics.
 *
 * <p>Lifecycle of one swap:
 * <pre>
 *   IDLE -- first relevant VSM --&gt; IN_PROGRESS -- barrier release ---&gt; COMMITTED
 *                                              -- timeout watchdog --&gt; TIMED_OUT
 *                                              -- exception thrown --&gt; FAILED
 * </pre>
 * After any terminal state the coordinator returns to IDLE so the next swap can be armed.
 *
 * <p>Thread safety: every state-mutating method is {@code synchronized} on this instance. The
 * watchdog timer fires on a daemon executor and contends for the same lock.
 *
 * <h3>Cutover semantics and same-poll-batch records</h3>
 *
 * <p>Both sides pause Kafka prefetch at their own VSM once recordCurrent/FutureVsm reports the
 * partition is fully observed across regions. Kafka's {@code consumer.pause()} stops <em>future</em>
 * polls but does <em>not</em> truncate the batch already returned by the current poll, so any records
 * past the VSM in the same poll-batch flow through {@code processPut}:
 * <ul>
 *   <li><b>Current side</b>: gate passes pre-commit, records surface (this is the AA design's
 *       intended path for these records).</li>
 *   <li><b>Future side</b>: gate fails pre-commit, records dropped silently. Once dropped, Kafka
 *       does not redeliver them — they are gone from the future-version transformer's stream.</li>
 * </ul>
 *
 * <p>The plan accepts this asymmetry under the assumption that current and future leaders process
 * the same RT events in approximately aligned poll-batches, so any record dropped by the future side
 * is delivered by the current side. This assumption holds in production but can break under
 * artificial stress (e.g., heavy parallel test load), where the leaders' batching diverges. In
 * those cases an individual record produced inside the cutover window may be lost from the
 * consumer's stream. Records produced sufficiently after the swap commits — once
 * {@code partitionToVersionToServe} has flipped and the future-side prefetch has resumed — flow
 * deterministically through the future-version transformer.
 *
 * <p>Operationally, the consumer's at-least-once guarantee holds for steady-state traffic and for
 * records produced before the swap is triggered or after it has fully committed. The window of
 * vulnerability is bounded by the watchdog timeout configured via
 * {@link ChangelogClientConfig#setVersionSwapTimeoutInMs(long)}.
 */
public class RecordTransformerVersionSwapCoordinator {
  private static final Logger LOGGER = LogManager.getLogger(RecordTransformerVersionSwapCoordinator.class);

  enum State {
    IDLE, IN_PROGRESS, COMMITTED, TIMED_OUT, FAILED
  }

  private final String storeName;
  private final String clientRegionName;
  private final int totalRegionCount;
  private final long versionSwapTimeoutInMs;
  private final BasicConsumerStats changeCaptureStats;
  private final Map<Integer, Integer> partitionToVersionToServe;
  private final Set<Integer> subscribedPartitions;
  private final Consumer<Exception> failureSurface;
  private final ScheduledExecutorService timeoutExecutor;

  // All reads/writes occur under {@code synchronized(this)} — plain field suffices.
  private State state = State.IDLE;

  // Per-swap state below — reset whenever a swap is armed and after any terminal state.
  private long activeGenerationId = -1;
  private String activeOldVersionTopic;
  private String activeNewVersionTopic;
  private int activeNewVersion = -1;
  private final Map<Integer, Set<String>> currentVersionRegionsConsumed = new HashMap<>();
  private final Map<Integer, Set<String>> futureVersionRegionsConsumed = new HashMap<>();
  private final Set<Integer> pausedCurrentPartitions = new HashSet<>();
  private final Set<Integer> pausedFuturePartitions = new HashSet<>();
  private final Set<Integer> assignedPartitionsSnapshot = new HashSet<>();
  private InternalDaVinciRecordTransformer<?, ?, ?> currentTransformerRef;
  private InternalDaVinciRecordTransformer<?, ?, ?> futureTransformerRef;
  private ScheduledFuture<?> timeoutWatchdog;

  public RecordTransformerVersionSwapCoordinator(
      String storeName,
      String clientRegionName,
      int totalRegionCount,
      long versionSwapTimeoutInMs,
      BasicConsumerStats changeCaptureStats,
      Map<Integer, Integer> partitionToVersionToServe,
      Set<Integer> subscribedPartitions,
      Consumer<Exception> failureSurface) {
    if (versionSwapTimeoutInMs <= 0) {
      throw new IllegalArgumentException("versionSwapTimeoutInMs must be positive but was " + versionSwapTimeoutInMs);
    }
    this.storeName = storeName;
    this.clientRegionName = clientRegionName;
    this.totalRegionCount = totalRegionCount;
    this.versionSwapTimeoutInMs = versionSwapTimeoutInMs;
    this.changeCaptureStats = changeCaptureStats;
    this.partitionToVersionToServe = partitionToVersionToServe;
    this.subscribedPartitions = subscribedPartitions;
    this.failureSurface = failureSurface;
    this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
        new DaemonThreadFactory("RecordTransformerVersionSwapCoordinator-" + storeName));
  }

  @VisibleForTesting
  RecordTransformerVersionSwapCoordinator(
      String storeName,
      String clientRegionName,
      int totalRegionCount,
      long versionSwapTimeoutInMs,
      BasicConsumerStats changeCaptureStats,
      Map<Integer, Integer> partitionToVersionToServe,
      Set<Integer> subscribedPartitions,
      Consumer<Exception> failureSurface,
      ScheduledExecutorService timeoutExecutor) {
    this.storeName = storeName;
    this.clientRegionName = clientRegionName;
    this.totalRegionCount = totalRegionCount;
    this.versionSwapTimeoutInMs = versionSwapTimeoutInMs;
    this.changeCaptureStats = changeCaptureStats;
    this.partitionToVersionToServe = partitionToVersionToServe;
    this.subscribedPartitions = subscribedPartitions;
    this.failureSurface = failureSurface;
    this.timeoutExecutor = timeoutExecutor;
  }

  /**
   * Returns whether the given VSM is relevant for this consumer:
   * <ul>
   *   <li>has a non-default {@code generationId} (legacy/old VSMs use -1)</li>
   *   <li>was sourced from this client's region</li>
   *   <li>was consumed on the topic matching this transformer's side (old topic for current side,
   *       new topic for future side)</li>
   *   <li>targets a version greater than any version already promoted to serving (defends against
   *       historical VSMs replayed after restart from EARLIEST or rollback to a lower version)</li>
   *   <li>matches the in-progress generationId when a swap is already armed</li>
   * </ul>
   */
  public synchronized boolean isRelevant(VersionSwap vsm, boolean isCurrentSide, int transformerVersion) {
    if (vsm == null || vsm.getGenerationId() == -1) {
      return false;
    }
    if (vsm.getSourceRegion() == null || !clientRegionName.equals(vsm.getSourceRegion().toString())) {
      return false;
    }
    if (vsm.getOldServingVersionTopic() == null || vsm.getNewServingVersionTopic() == null) {
      return false;
    }
    String oldTopic = vsm.getOldServingVersionTopic().toString();
    String newTopic = vsm.getNewServingVersionTopic().toString();
    if (!Version.isVersionTopic(newTopic)) {
      return false;
    }
    String transformerTopic = Version.composeKafkaTopic(storeName, transformerVersion);
    if (isCurrentSide) {
      if (!transformerTopic.equals(oldTopic)) {
        return false;
      }
    } else {
      if (!transformerTopic.equals(newTopic)) {
        return false;
      }
    }
    int newVersion = Version.parseVersionFromVersionTopicName(newTopic);
    if (newVersion <= computeMaxServedVersion()) {
      return false;
    }
    if (state == State.IN_PROGRESS) {
      if (vsm.getGenerationId() != activeGenerationId) {
        return false;
      }
      if (!oldTopic.equals(activeOldVersionTopic) || !newTopic.equals(activeNewVersionTopic)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Records a current-side (old-version) VSM observation for {@code partition}. Returns true when
   * this partition has now observed one VSM from every destination-region on the current side.
   *
   * <p>Mirrors the accumulation key in {@link VersionSwapMessageState#handleVersionSwap}: the
   * {@code destinationRegion} field identifies which region's RT this VSM was written to. A given
   * swap is replicated into every RT, so a partition has caught up to the swap point on its current
   * side once we have seen one VSM per destination region.
   */
  public synchronized boolean recordCurrentVsm(
      VersionSwap vsm,
      int partition,
      InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer) {
    if (!canRecord(vsm, partition)) {
      return false;
    }
    armIfNeeded(vsm);
    // Re-check identity post-arm: defends against the race where another VSM armed first between
    // the caller's {@link #isRelevant} check and this call.
    if (vsm.getGenerationId() != activeGenerationId) {
      return false;
    }
    currentTransformerRef = currentTransformer;
    if (!assignedPartitionsSnapshot.contains(partition)) {
      // Partition was added to {@link #subscribedPartitions} after this swap was armed; it is not
      // counted against this swap's barrier (which would require waiting for VSMs that the prior
      // arming moment couldn't have known to expect). At commit time, {@link #flipServingVersion}
      // still flips this partition to the new version along with the snapshotted ones.
      return false;
    }
    Set<String> regions = currentVersionRegionsConsumed.computeIfAbsent(partition, p -> new HashSet<>());
    regions.add(vsm.getDestinationRegion().toString());
    boolean complete = regions.size() >= totalRegionCount;
    if (complete) {
      pausedCurrentPartitions.add(partition);
    }
    return complete;
  }

  /**
   * Records a future-side (new-version) VSM observation for {@code partition}. Returns true when
   * this partition has now observed one VSM from every destination-region on the future side.
   */
  public synchronized boolean recordFutureVsm(
      VersionSwap vsm,
      int partition,
      InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer) {
    if (!canRecord(vsm, partition)) {
      return false;
    }
    armIfNeeded(vsm);
    if (vsm.getGenerationId() != activeGenerationId) {
      return false;
    }
    futureTransformerRef = futureTransformer;
    if (!assignedPartitionsSnapshot.contains(partition)) {
      return false;
    }
    Set<String> regions = futureVersionRegionsConsumed.computeIfAbsent(partition, p -> new HashSet<>());
    regions.add(vsm.getDestinationRegion().toString());
    boolean complete = regions.size() >= totalRegionCount;
    if (complete) {
      pausedFuturePartitions.add(partition);
    }
    return complete;
  }

  /**
   * Common pre-conditions for {@link #recordCurrentVsm} / {@link #recordFutureVsm}:
   * <ul>
   *   <li>VSM is non-null and its identity fields are populated.</li>
   *   <li>The {@code partition} is in {@link #subscribedPartitions} <em>before</em> arming a new
   *       swap — defends against an early/spurious VSM arming a doomed swap whose snapshot
   *       wouldn't include the partition anyway.</li>
   *   <li>The VSM's target version has not already been promoted to serving on any partition
   *       (re-checked under the monitor so a swap that committed between the caller's
   *       {@link #isRelevant} check and this call rejects this stale VSM).</li>
   * </ul>
   */
  private boolean canRecord(VersionSwap vsm, int partition) {
    if (vsm == null || vsm.getOldServingVersionTopic() == null || vsm.getNewServingVersionTopic() == null
        || vsm.getDestinationRegion() == null) {
      return false;
    }
    if (!subscribedPartitions.contains(partition)) {
      return false;
    }
    int newVersion = Version.parseVersionFromVersionTopicName(vsm.getNewServingVersionTopic().toString());
    if (newVersion <= computeMaxServedVersion()) {
      return false;
    }
    return true;
  }

  /**
   * @return true when every assigned partition has received VSMs from every region on both sides.
   */
  public synchronized boolean allPartitionsBothSidesComplete() {
    if (assignedPartitionsSnapshot.isEmpty()) {
      return false;
    }
    for (int partition: assignedPartitionsSnapshot) {
      Set<String> currentRegions = currentVersionRegionsConsumed.get(partition);
      if (currentRegions == null || currentRegions.size() < totalRegionCount) {
        return false;
      }
      Set<String> futureRegions = futureVersionRegionsConsumed.get(partition);
      if (futureRegions == null || futureRegions.size() < totalRegionCount) {
        return false;
      }
    }
    return true;
  }

  /**
   * Atomically flips {@code partitionToVersionToServe} to the future version for every assigned
   * partition, resumes the future side's paused partitions, emits a single SUCCESS metric, and
   * returns to {@link State#IDLE}. Idempotent — concurrent callers lose the CAS and become no-ops.
   * If the flip itself throws, the exception is treated as a swap failure (FAIL metric, surfaced)
   * and rethrown.
   */
  public synchronized void commitSwap() {
    if (state != State.IN_PROGRESS) {
      return;
    }
    state = State.COMMITTED;
    cancelWatchdog();
    try {
      flipServingVersion();
      resumeFutureSide();
    } catch (Exception flipFailure) {
      handleTerminalFailure(flipFailure);
      throw flipFailure;
    }
    if (changeCaptureStats != null) {
      changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
    }
    LOGGER.info(
        "Version swap committed for store: {}, swap: {} -> {}, partitions: {}",
        storeName,
        activeOldVersionTopic,
        activeNewVersionTopic,
        assignedPartitionsSnapshot);
    clearSwapState();
  }

  /**
   * Forces the cutover after the configured timeout has elapsed without all VSMs being observed.
   * Counts as a recoverable success — emits a single SUCCESS metric and a WARN log; no separate
   * timeout counter exists by design.
   */
  public synchronized void timeoutSwap() {
    if (state != State.IN_PROGRESS) {
      return;
    }
    state = State.TIMED_OUT;
    cancelWatchdog();
    try {
      flipServingVersion();
      resumeFutureSide();
    } catch (Exception flipFailure) {
      handleTerminalFailure(flipFailure);
      return;
    }
    if (changeCaptureStats != null) {
      changeCaptureStats.emitVersionSwapCountMetrics(SUCCESS);
    }
    LOGGER.warn(
        "Version swap timed out (forced cutover) for store: {}, swap: {} -> {}, partitions: {}",
        storeName,
        activeOldVersionTopic,
        activeNewVersionTopic,
        assignedPartitionsSnapshot);
    clearSwapState();
  }

  private void handleTerminalFailure(Exception error) {
    if (changeCaptureStats != null) {
      changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
    }
    if (failureSurface != null) {
      failureSurface.accept(error);
    }
    LOGGER.error(
        "Version swap commit failed for store: {}, swap: {} -> {}",
        storeName,
        activeOldVersionTopic,
        activeNewVersionTopic,
        error);
    // The flip failed mid-commit, so partitions that were paused waiting for the barrier must be
    // resumed on both sides — otherwise their Kafka prefetch stays paused even though the swap
    // will never proceed.
    resumeCurrentSide();
    resumeFutureSide();
    clearSwapState();
  }

  /**
   * Surfaces an exception from the AA version-swap code path so the next {@code poll()} call
   * throws. Safe to call regardless of whether a swap is currently armed:
   * <ul>
   *   <li>If a swap is in progress, transitions to FAILED, cancels the watchdog, emits a single
   *       FAIL metric, and clears per-swap state.</li>
   *   <li>If no swap is armed (e.g. the exception happened during {@code isRelevant} before any
   *       VSM was accepted), the failure is still surfaced so the consumer's {@code poll()} sees
   *       it; no FAIL metric is emitted (there was no swap to fail).</li>
   * </ul>
   */
  public synchronized void failSwap(Exception error) {
    // Surface unconditionally so pre-arm exceptions are not silently swallowed.
    if (failureSurface != null) {
      failureSurface.accept(error);
    }
    if (state != State.IN_PROGRESS) {
      return;
    }
    state = State.FAILED;
    cancelWatchdog();
    if (changeCaptureStats != null) {
      changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
    }
    LOGGER.error(
        "Version swap failed for store: {}, swap: {} -> {}",
        storeName,
        activeOldVersionTopic,
        activeNewVersionTopic,
        error);
    clearSwapState();
  }

  /**
   * Removes unsubscribed partitions from in-flight swap state, releases any per-partition pauses,
   * then re-evaluates the barrier — partitions leaving the assignment may complete the barrier for
   * the remaining set.
   */
  public synchronized void handleUnsubscribe(Set<Integer> partitions) {
    for (int partition: partitions) {
      assignedPartitionsSnapshot.remove(partition);
      currentVersionRegionsConsumed.remove(partition);
      futureVersionRegionsConsumed.remove(partition);
      if (pausedCurrentPartitions.remove(partition) && currentTransformerRef != null) {
        currentTransformerRef.resumePartitionConsumption(partition);
      }
      if (pausedFuturePartitions.remove(partition) && futureTransformerRef != null) {
        futureTransformerRef.resumePartitionConsumption(partition);
      }
    }
    if (state == State.IN_PROGRESS && !assignedPartitionsSnapshot.isEmpty() && allPartitionsBothSidesComplete()) {
      commitSwap();
    }
  }

  /**
   * Cancels the timeout watchdog and shuts down the executor. Called from the consumer's close().
   * The {@code shutdownNow()} call is issued <em>outside</em> the monitor so that, if the watchdog
   * task is mid-{@link #timeoutSwap} (also synchronized) and blocked on the monitor, we don't
   * compound the wait by holding it.
   */
  public void shutdown() {
    synchronized (this) {
      cancelWatchdog();
    }
    timeoutExecutor.shutdownNow();
  }

  /**
   * Arms a fresh swap from the given VSM. Caller must have validated {@code vsm}'s topic fields
   * (non-null) and confirmed the swap should proceed (region, version, generation gates passed).
   * No-op if a swap is already in progress, or if {@code state} sits in a terminal-but-not-yet-
   * cleared state (defensive — terminal states normally chain into {@link #clearSwapState()}
   * before the monitor is released, so the field never rests there for observers).
   */
  private void armIfNeeded(VersionSwap vsm) {
    if (state != State.IDLE) {
      return;
    }
    activeGenerationId = vsm.getGenerationId();
    activeOldVersionTopic = vsm.getOldServingVersionTopic().toString();
    activeNewVersionTopic = vsm.getNewServingVersionTopic().toString();
    activeNewVersion = Version.parseVersionFromVersionTopicName(activeNewVersionTopic);
    currentVersionRegionsConsumed.clear();
    futureVersionRegionsConsumed.clear();
    pausedCurrentPartitions.clear();
    pausedFuturePartitions.clear();
    assignedPartitionsSnapshot.clear();
    assignedPartitionsSnapshot.addAll(subscribedPartitions);
    state = State.IN_PROGRESS;
    scheduleTimeoutWatchdog();
  }

  private void scheduleTimeoutWatchdog() {
    timeoutWatchdog = timeoutExecutor.schedule(this::timeoutSwap, versionSwapTimeoutInMs, TimeUnit.MILLISECONDS);
  }

  private void cancelWatchdog() {
    if (timeoutWatchdog != null) {
      timeoutWatchdog.cancel(false);
      timeoutWatchdog = null;
    }
  }

  private void flipServingVersion() {
    if (activeNewVersion <= 0) {
      // Invariant violation: armIfNeeded always derives activeNewVersion from a parsed version
      // topic (> 0) before state transitions to IN_PROGRESS, and commit/timeout/fail paths only
      // call flipServingVersion under that state. Bail out instead of poisoning every assigned
      // partition with version -1.
      LOGGER.error(
          "Refusing to flip serving version for store: {} because activeNewVersion is not set; " + "swap: {} -> {}",
          storeName,
          activeOldVersionTopic,
          activeNewVersionTopic);
      return;
    }
    for (int partition: assignedPartitionsSnapshot) {
      partitionToVersionToServe.put(partition, activeNewVersion);
    }
    // Mid-swap subscribers: partitions that joined {@link #subscribedPartitions} after the snapshot
    // was taken would otherwise remain pinned to the OLD version, silently filtering future-version
    // records until the next swap. Flip them here so the cutover is complete for the full
    // assignment set as observed at commit time.
    for (int partition: subscribedPartitions) {
      if (!assignedPartitionsSnapshot.contains(partition)) {
        partitionToVersionToServe.put(partition, activeNewVersion);
      }
    }
  }

  private void resumeFutureSide() {
    if (futureTransformerRef == null) {
      return;
    }
    for (int partition: pausedFuturePartitions) {
      futureTransformerRef.resumePartitionConsumption(partition);
    }
  }

  /**
   * Resume the current side's paused partitions. Only invoked from the terminal-failure path —
   * a successful commit relies on the store-version-change machinery to tear down the current
   * version (and with it, its Kafka prefetch), so resume there would be wasted work.
   */
  private void resumeCurrentSide() {
    if (currentTransformerRef == null) {
      return;
    }
    for (int partition: pausedCurrentPartitions) {
      currentTransformerRef.resumePartitionConsumption(partition);
    }
  }

  private void clearSwapState() {
    activeGenerationId = -1;
    activeOldVersionTopic = null;
    activeNewVersionTopic = null;
    activeNewVersion = -1;
    currentVersionRegionsConsumed.clear();
    futureVersionRegionsConsumed.clear();
    pausedCurrentPartitions.clear();
    pausedFuturePartitions.clear();
    assignedPartitionsSnapshot.clear();
    state = State.IDLE;
  }

  private int computeMaxServedVersion() {
    int max = -1;
    for (int version: partitionToVersionToServe.values()) {
      if (version > max) {
        max = version;
      }
    }
    return max;
  }
}
