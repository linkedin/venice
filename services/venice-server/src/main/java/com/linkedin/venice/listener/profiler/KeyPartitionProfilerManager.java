package com.linkedin.venice.listener.profiler;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Tracks active {@link KeyPartitionProfiler} sessions and exposes a near-zero-cost guard for the
 * read hot path.
 *
 * <p>When a session's window expires, the result is emitted to the server log,
 * The session is then removed and the fast-path boolean is cleared once the last session is gone.
 */
public final class KeyPartitionProfilerManager {
  private static final Logger LOGGER = LogManager.getLogger(KeyPartitionProfilerManager.class);
  public static final int DEFAULT_MAX_CONCURRENT_SESSIONS = 5;
  public static final int DEFAULT_TOP_K = 100;
  /** Upper bound on a single session's profiling window. Prevents accidental long-running sessions. */
  public static final long MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(30);
  /** Upper bound on per-partition top-K size. Bounds per-session memory growth. */
  public static final int MAX_TOP_K = 10_000;
  public static final String LOG_PREFIX = "HOT_PARTITION_PROFILE";

  private final Map<String, ProfilerSession> activeProfilers = new VeniceConcurrentHashMap<>();
  private final ScheduledExecutorService expirationScheduler;
  private final int maxConcurrentSessions;
  private final ToIntFunction<String> partitionCountResolver;

  // Fast-path guard: writes are sequenced after map insertion / before map clear; reads on the
  // hot path are a single volatile load.
  private volatile boolean anyProfilingActive = false;

  /** Test-only convenience: default session cap, no LogContext. */
  KeyPartitionProfilerManager(ToIntFunction<String> partitionCountResolver) {
    this(partitionCountResolver, DEFAULT_MAX_CONCURRENT_SESSIONS, LogContext.EMPTY);
  }

  public KeyPartitionProfilerManager(
      ToIntFunction<String> partitionCountResolver,
      int maxConcurrentSessions,
      LogContext logContext) {
    this.partitionCountResolver = partitionCountResolver;
    this.maxConcurrentSessions = maxConcurrentSessions;
    this.expirationScheduler = Executors
        .newSingleThreadScheduledExecutor(new DaemonThreadFactory("KeyPartitionProfiler-Expiration", logContext));
  }

  /**
   * Hot-path entry point. Returns {@code true} when at least one profiling session is currently
   * running and the read path should record key/partition activity.
   */
  public boolean isAnyProfilingActive() {
    return anyProfilingActive;
  }

  /**
   * Returns the active profiler for {@code storeName} or {@code null} if no session is running
   * for that store. Should only be called when {@link #isAnyProfilingActive()} returned true.
   */
  public KeyPartitionProfiler getProfiler(String storeName) {
    ProfilerSession session = activeProfilers.get(storeName);
    return session == null ? null : session.profiler;
  }

  /**
   * Starts a profiling session for {@code storeName}. Returns a description of the action taken
   * (always-on capability; no config flag).
   */
  public synchronized StartResult startProfiling(String storeName, String storeVersion, long durationMs, int topK) {
    if (durationMs <= 0) {
      return StartResult.invalid("durationMs must be positive");
    }
    if (durationMs > MAX_DURATION_MS) {
      return StartResult.invalid("durationMs exceeds maximum (" + MAX_DURATION_MS + " ms)");
    }
    if (topK <= 0) {
      return StartResult.invalid("topK must be positive");
    }
    if (topK > MAX_TOP_K) {
      return StartResult.invalid("topK exceeds maximum (" + MAX_TOP_K + ")");
    }
    if (activeProfilers.containsKey(storeName)) {
      return StartResult.alreadyRunning();
    }
    if (activeProfilers.size() >= maxConcurrentSessions) {
      return StartResult.capacityExceeded(maxConcurrentSessions);
    }
    int partitionCount;
    try {
      partitionCount = partitionCountResolver.applyAsInt(storeVersion);
    } catch (RuntimeException e) {
      return StartResult.invalid("Could not resolve partition count: " + e.getMessage());
    }
    if (partitionCount <= 0) {
      return StartResult.invalid("partition count for " + storeVersion + " is non-positive: " + partitionCount);
    }
    long startTimeMs = System.currentTimeMillis();
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(storeName, storeVersion, startTimeMs, durationMs, partitionCount, topK);
    ProfilerSession session = new ProfilerSession(profiler);
    activeProfilers.put(storeName, session);
    anyProfilingActive = true;
    session.expirationFuture =
        expirationScheduler.schedule(() -> finishSession(storeName, "expired"), durationMs, TimeUnit.MILLISECONDS);
    LOGGER.info(
        "{}: started storeVersion={} durationMs={} topK={} partitionCount={}",
        LOG_PREFIX,
        storeVersion,
        durationMs,
        topK,
        partitionCount);
    return StartResult.started();
  }

  /**
   * Stops the profiling session for {@code storeName} early and emits its result. Returns the
   * stopped {@link KeyPartitionProfiler} (carrying the actual {@code storeVersion} of the
   * session) so callers can build accurate audit messages, or {@link Optional#empty()} if no
   * session was running for the store.
   */
  public Optional<KeyPartitionProfiler> stopProfiling(String storeName) {
    return finishSession(storeName, "stopped");
  }

  /**
   * Removes the session, emits its JSON result, and clears the fast-path flag if no sessions
   * remain. State mutation (map remove, future cancel, fast-path clear) happens under the
   * monitor; the JSON emit runs unlocked so a multi-MB serialization doesn't block concurrent
   * start/stop admin calls.
   */
  private Optional<KeyPartitionProfiler> finishSession(String storeName, String reason) {
    KeyPartitionProfiler profiler;
    synchronized (this) {
      ProfilerSession session = activeProfilers.remove(storeName);
      if (session == null) {
        return Optional.empty();
      }
      if (session.expirationFuture != null) {
        session.expirationFuture.cancel(false);
      }
      if (activeProfilers.isEmpty()) {
        anyProfilingActive = false;
      }
      profiler = session.profiler;
    }
    try {
      profiler.emitJsonTo(LOGGER, LOG_PREFIX, reason);
    } catch (Throwable t) {
      LOGGER.error("{}: failed to emit profile for store={} reason={}", LOG_PREFIX, storeName, reason, t);
    }
    return Optional.of(profiler);
  }

  @VisibleForTesting
  public void shutdown() {
    expirationScheduler.shutdownNow();
  }

  @VisibleForTesting
  int activeSessionCount() {
    return activeProfilers.size();
  }

  private static final class ProfilerSession {
    final KeyPartitionProfiler profiler;
    /**
     * Assigned by {@link #startProfiling} immediately after the session is registered in the
     * active-profiler map. Written under the manager's monitor and only read while the same
     * monitor is held, so no further publication semantics are required.
     */
    ScheduledFuture<?> expirationFuture;

    ProfilerSession(KeyPartitionProfiler profiler) {
      this.profiler = profiler;
    }
  }

  /** Outcome of a {@link #startProfiling} call. */
  public static final class StartResult {
    public enum Status {
      STARTED, ALREADY_RUNNING, CAPACITY_EXCEEDED, INVALID
    }

    public final Status status;
    public final String message;

    private StartResult(Status status, String message) {
      this.status = status;
      this.message = message;
    }

    static StartResult started() {
      return new StartResult(Status.STARTED, "started");
    }

    static StartResult alreadyRunning() {
      return new StartResult(Status.ALREADY_RUNNING, "session already running for store");
    }

    static StartResult capacityExceeded(int limit) {
      return new StartResult(Status.CAPACITY_EXCEEDED, "max concurrent profiling sessions reached: " + limit);
    }

    static StartResult invalid(String reason) {
      return new StartResult(Status.INVALID, reason);
    }
  }
}
