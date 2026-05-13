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
import java.util.concurrent.atomic.AtomicReference;
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

  private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);

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
    if (state.get() == State.IN_PROGRESS) {
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
    armIfNeeded(vsm);
    currentTransformerRef = currentTransformer;
    if (!assignedPartitionsSnapshot.contains(partition)) {
      // Partition was added mid-swap; it will be considered on the next swap.
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
    armIfNeeded(vsm);
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
    if (!state.compareAndSet(State.IN_PROGRESS, State.COMMITTED)) {
      return;
    }
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
    if (!state.compareAndSet(State.IN_PROGRESS, State.TIMED_OUT)) {
      return;
    }
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
    clearSwapState();
  }

  /**
   * Marks the current swap as failed, emits a single FAIL metric, and surfaces the exception via
   * the failure surface so the next {@code poll()} call throws.
   */
  public synchronized void failSwap(Exception error) {
    if (!state.compareAndSet(State.IN_PROGRESS, State.FAILED)) {
      return;
    }
    cancelWatchdog();
    if (changeCaptureStats != null) {
      changeCaptureStats.emitVersionSwapCountMetrics(FAIL);
    }
    if (failureSurface != null) {
      failureSurface.accept(error);
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
    if (state.get() == State.IN_PROGRESS && !assignedPartitionsSnapshot.isEmpty() && allPartitionsBothSidesComplete()) {
      commitSwap();
    }
  }

  /**
   * Cancels the timeout watchdog and shuts down the executor. Called from the consumer's close().
   */
  public synchronized void shutdown() {
    cancelWatchdog();
    timeoutExecutor.shutdownNow();
  }

  private void armIfNeeded(VersionSwap vsm) {
    State current = state.get();
    if (current == State.IN_PROGRESS) {
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
    state.set(State.IN_PROGRESS);
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
    for (int partition: assignedPartitionsSnapshot) {
      partitionToVersionToServe.put(partition, activeNewVersion);
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
    state.set(State.IDLE);
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
