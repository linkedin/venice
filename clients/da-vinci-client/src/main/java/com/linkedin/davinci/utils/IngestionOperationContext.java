package com.linkedin.davinci.utils;

import java.util.concurrent.TimeUnit;


/**
 * Thread-confined breadcrumbs for a monitored drainer. Readers only inspect immutable, volatile-published snapshots;
 * they never acquire a storage lock or call the storage engine. No context is installed on unmonitored threads.
 */
public final class IngestionOperationContext {
  private static final ThreadLocal<IngestionOperationContext> CURRENT = new ThreadLocal<>();

  public enum Phase {
    PROCESS_RECORD, SYNC_OFFSET, SYNC_VT_DIV, SYNC_GLOBAL_RT_DIV, STORAGE_PUT, STORAGE_PUT_WITH_RMD, STORAGE_PUT_RMD,
    STORAGE_DELETE, STORAGE_DELETE_WITH_RMD, STORAGE_SYNC, ROCKSDB_PUT, ROCKSDB_WRITE, ROCKSDB_DELETE, ROCKSDB_FLUSH
  }

  @FunctionalInterface
  public interface Action<T, E extends Exception> {
    T execute() throws E;
  }

  public static final class Operation {
    public final Phase phase;
    public final String storeVersion;
    public final int partition;
    public final long startedAtMs;
    private final long startedAtNs = System.nanoTime();

    private Operation(Phase phase, String storeVersion, int partition) {
      this(phase, storeVersion, partition, System.currentTimeMillis());
    }

    private Operation(Phase phase, String storeVersion, int partition, long startedAtMs) {
      this.phase = phase;
      this.storeVersion = storeVersion;
      this.partition = partition;
      this.startedAtMs = startedAtMs;
    }

    public long ageMs() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNs);
    }

    @Override
    public String toString() {
      return phase + "(" + storeVersion + ", partition=" + partition + ", startedAtMs=" + startedAtMs + ")";
    }
  }

  public static final class Snapshot {
    public final Operation root;
    public final Operation operation;
    public final Operation lastCompleted;
    public final long lastCompletedAtMs;
    public final boolean lastSucceeded;

    private Snapshot(
        Operation root,
        Operation operation,
        Operation lastCompleted,
        long lastCompletedAtMs,
        boolean lastSucceeded) {
      this.root = root;
      this.operation = operation;
      this.lastCompleted = lastCompleted;
      this.lastCompletedAtMs = lastCompletedAtMs;
      this.lastSucceeded = lastSucceeded;
    }
  }

  private volatile Snapshot snapshot = new Snapshot(null, null, null, 0, false);

  public Snapshot snapshot() {
    return snapshot;
  }

  public static boolean isTracking() {
    return CURRENT.get() != null;
  }

  public void start(Phase phase, String storeVersion, int partition) {
    start(phase, storeVersion, partition, System.currentTimeMillis());
  }

  public void start(Phase phase, String storeVersion, int partition, long startedAtMs) {
    Operation root = new Operation(phase, storeVersion, partition, startedAtMs);
    Snapshot previous = snapshot;
    snapshot = new Snapshot(root, root, previous.lastCompleted, previous.lastCompletedAtMs, previous.lastSucceeded);
    CURRENT.set(this);
  }

  public void finish(boolean succeeded) {
    Snapshot previous = snapshot;
    snapshot = new Snapshot(null, null, previous.root, System.currentTimeMillis(), succeeded);
    CURRENT.remove();
  }

  /**
   * STORAGE phases include dispatch, lifecycle guards, synchronization waits and Java preparation. Only the
   * ROCKSDB phases immediately surrounding the corresponding JNI call identify a native write/flush boundary.
   */
  public static <T, E extends Exception> T track(Phase phase, String storeVersion, int partition, Action<T, E> action)
      throws E {
    IngestionOperationContext context = CURRENT.get();
    if (context == null) {
      return action.execute();
    }
    Snapshot previous = context.snapshot;
    Operation operation = new Operation(phase, storeVersion, partition);
    context.snapshot = new Snapshot(
        previous.root,
        operation,
        previous.lastCompleted,
        previous.lastCompletedAtMs,
        previous.lastSucceeded);
    boolean succeeded = false;
    try {
      T result = action.execute();
      succeeded = true;
      return result;
    } finally {
      context.snapshot =
          new Snapshot(previous.root, previous.operation, operation, System.currentTimeMillis(), succeeded);
    }
  }
}
