package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
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
 * Owned by {@link VeniceChangelogConsumerDaVinciRecordTransformerImpl} when
 * {@link ChangelogClientConfig#isVersionSwapByControlMessageEnabled()} is true. Enforces a
 * cross-partition, cross-region barrier so a swap commits only after every assigned partition has
 * observed VSMs from every region on both the current and future version topics.
 *
 * Lifecycle: IDLE → (first relevant VSM) → IN_PROGRESS → COMMITTED / TIMED_OUT / FAILED → IDLE.
 *
 * Thread safety: every state-mutating method is {@code synchronized} on this instance; the watchdog
 * timer fires on a daemon executor and contends for the same lock.
 *
 * Cutover semantics: both sides pause Kafka prefetch once a partition's per-side barrier closes.
 * Kafka's {@code consumer.pause()} does not truncate the in-flight batch, so records past the VSM in
 * the same poll-batch flow through {@code processPut}: surfaced on the current side, dropped on the
 * future side. The design accepts this under the assumption that current and future leaders process
 * RT events in approximately aligned poll-batches; the watchdog (configured via
 * {@link ChangelogClientConfig#setVersionSwapTimeoutInMs(long)}) bounds the vulnerable window.
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

  // All state reads/writes occur under {@code synchronized(this)}.
  private State state = State.IDLE;

  // Per-swap state — reset on arm and after any terminal state.
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
      Consumer<Exception> failureSurface,
      LogContext logContext) {
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
        new DaemonThreadFactory("RecordTransformerVersionSwapCoordinator-" + storeName, logContext));
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
   * Returns true when the VSM should be considered for this consumer: non-default
   * {@code generationId}, sourced from this client's region, on the topic matching this side,
   * targets a version greater than any version already promoted to serving, and matches the
   * active swap's generationId when one is in progress.
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
   * Records a current-side (old-version) VSM observation. Returns true when the partition has now
   * observed one VSM per destination-region on the current side (accumulation key is
   * {@code destinationRegion}, one VSM per region completes the per-partition barrier).
   */
  public synchronized boolean recordCurrentVsm(
      VersionSwap vsm,
      int partition,
      InternalDaVinciRecordTransformer<?, ?, ?> currentTransformer) {
    return recordVsm(vsm, partition, currentTransformer, true);
  }

  /** Records a future-side (new-version) VSM observation. Mirror of {@link #recordCurrentVsm}. */
  public synchronized boolean recordFutureVsm(
      VersionSwap vsm,
      int partition,
      InternalDaVinciRecordTransformer<?, ?, ?> futureTransformer) {
    return recordVsm(vsm, partition, futureTransformer, false);
  }

  private boolean recordVsm(
      VersionSwap vsm,
      int partition,
      InternalDaVinciRecordTransformer<?, ?, ?> transformer,
      boolean isCurrentSide) {
    if (!canRecord(vsm, partition)) {
      return false;
    }
    armIfNeeded(vsm);
    // Re-check identity post-arm in case another VSM armed first between the caller's isRelevant
    // check and this call.
    if (vsm.getGenerationId() != activeGenerationId) {
      return false;
    }
    if (isCurrentSide) {
      currentTransformerRef = transformer;
    } else {
      futureTransformerRef = transformer;
    }
    if (!assignedPartitionsSnapshot.contains(partition)) {
      // Late subscriber — not counted against this swap, but flipServingVersion still flips it.
      return false;
    }
    Map<Integer, Set<String>> regions = isCurrentSide ? currentVersionRegionsConsumed : futureVersionRegionsConsumed;
    Set<String> seen = regions.computeIfAbsent(partition, p -> new HashSet<>());
    seen.add(vsm.getDestinationRegion().toString());
    if (seen.size() < totalRegionCount) {
      return false;
    }
    (isCurrentSide ? pausedCurrentPartitions : pausedFuturePartitions).add(partition);
    return true;
  }

  /**
   * Pre-conditions for {@link #recordCurrentVsm} / {@link #recordFutureVsm}: VSM identity fields
   * populated, partition currently subscribed, and VSM's target version not already promoted to
   * serving (re-checked under the monitor to reject stale VSMs after an intervening commit).
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
   * partition, resumes the future side, emits a single SUCCESS metric, and returns to IDLE.
   * Idempotent. A flip failure is fully handled internally (FAIL metric, failure surface, both
   * sides resumed, state cleared) — the failure is not rethrown to keep this method safe to call
   * from {@code onVersionSwap}, which would otherwise propagate the exception out of
   * {@code StoreIngestionTask.processControlMessage} and kill per-partition ingestion.
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
      return;
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
   * Forces the cutover after the configured timeout elapses. Counts as a recoverable success —
   * emits a single SUCCESS metric and a WARN log.
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
    // Flip failed mid-commit — resume both sides so partitions don't stay stuck paused.
    resumeCurrentSide();
    resumeFutureSide();
    clearSwapState();
  }

  /**
   * Surfaces an exception so the next {@code poll()} throws. Safe to call regardless of state:
   * always sets the failure surface; the FAIL metric and per-swap cleanup are gated on
   * {@code state == IN_PROGRESS} (no metric for pre-arm exceptions where no swap was armed).
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
   * Removes unsubscribed partitions from in-flight swap state, releases per-partition pauses, and
   * re-evaluates the barrier — partitions leaving may complete it for the remaining set.
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
   * Cancels the watchdog and shuts down the executor. {@code shutdownNow()} runs outside the
   * monitor so it can't compound the wait if a watchdog task is mid-{@link #timeoutSwap}.
   */
  public void shutdown() {
    synchronized (this) {
      cancelWatchdog();
    }
    timeoutExecutor.shutdownNow();
  }

  /**
   * Arms a fresh swap from the given VSM. Caller must have validated topic fields and gates.
   * No-op unless {@code state == IDLE}; terminal states normally chain into
   * {@link #clearSwapState()} before the monitor is released.
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
    // armIfNeeded sets activeNewVersion > 0 before state transitions to IN_PROGRESS, and
    // commitSwap/timeoutSwap only call this from IN_PROGRESS via state guard. So activeNewVersion
    // is always > 0 here by construction.
    for (int partition: assignedPartitionsSnapshot) {
      partitionToVersionToServe.put(partition, activeNewVersion);
    }
    // Late subscribers (joined after the snapshot) get flipped too so future-version records aren't
    // silently filtered until the next swap.
    for (int partition: subscribedPartitions) {
      if (!assignedPartitionsSnapshot.contains(partition)) {
        partitionToVersionToServe.put(partition, activeNewVersion);
      }
    }
  }

  private void resumeFutureSide() {
    resumeSide(futureTransformerRef, pausedFuturePartitions);
  }

  /**
   * Resume current-side paused partitions. Only called from the terminal-failure path — a
   * successful commit relies on store-version-change teardown to drop the current version's prefetch.
   */
  private void resumeCurrentSide() {
    resumeSide(currentTransformerRef, pausedCurrentPartitions);
  }

  private static void resumeSide(InternalDaVinciRecordTransformer<?, ?, ?> transformer, Set<Integer> paused) {
    if (transformer == null) {
      return;
    }
    for (int partition: paused) {
      transformer.resumePartitionConsumption(partition);
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
    return partitionToVersionToServe.values().stream().mapToInt(Integer::intValue).max().orElse(-1);
  }
}
