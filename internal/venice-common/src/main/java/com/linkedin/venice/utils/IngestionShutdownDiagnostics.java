package com.linkedin.venice.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;


/**
 * Opt-in, shutdown-only timing shared by a SIT and its writer. Phase transitions have a single owner (the SIT
 * thread); checkpoint workers only increment their completion counter. Readers never acquire the SIT/writer locks.
 * Snapshots are best effort across counters, but the current phase/partition/start are published together.
 */
public final class IngestionShutdownDiagnostics {
  public enum Phase {
    CLOSE_REQUEST_TO_LOOP_EXIT, WRITER_CLOSE, WRITER_LOCK, EOS_SUBMIT, EOS_FUTURE, BATCH_UNSUBSCRIBE, CHECKPOINT_SWEEP,
    INTERNAL_CLOSE, FINISHED
  }

  private static final class Operation {
    final Phase phase;
    final int partition;
    final long start;

    Operation(Phase phase, int partition, long start) {
      this.phase = phase;
      this.partition = partition;
      this.start = start;
    }
  }

  private final LongSupplier nanoTime;
  private final long requestedAt;
  private final AtomicLongArray totals = new AtomicLongArray(Phase.values().length);
  private final AtomicLongArray maxima = new AtomicLongArray(Phase.values().length);
  private final AtomicLongArray counts = new AtomicLongArray(Phase.values().length);
  private final AtomicInteger checkpointsFinished = new AtomicInteger();
  private volatile Operation current;
  private volatile int partitions;
  private volatile int writerClosesFinished;
  private volatile int writerClosesFailed;
  private volatile int writerSweepPartitionsFinished;
  private volatile long writerCloseTotalNs;
  private volatile long writerCloseMaxNs;
  private int slowWriterLogs;

  public IngestionShutdownDiagnostics() {
    this(System::nanoTime);
  }

  IngestionShutdownDiagnostics(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
    requestedAt = nanoTime.getAsLong();
    current = new Operation(Phase.CLOSE_REQUEST_TO_LOOP_EXIT, -1, requestedAt);
  }

  public void transition(Phase phase, int partition) {
    long now = nanoTime.getAsLong();
    Operation previous = current;
    long elapsed = now - previous.start;
    int index = previous.phase.ordinal();
    totals.addAndGet(index, elapsed);
    maxima.accumulateAndGet(index, elapsed, Math::max);
    counts.incrementAndGet(index);
    current = new Operation(phase, partition, now);
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public void checkpointFinished() {
    checkpointsFinished.incrementAndGet();
  }

  /** Inclusive writer close duration: overlaps WRITER_LOCK/EOS_SUBMIT/EOS_FUTURE, not an additional phase. */
  public boolean writerCloseFinished(long elapsedNs, boolean failed) {
    writerClosesFinished++;
    if (failed) {
      writerClosesFailed++;
    }
    writerCloseTotalNs += elapsedNs;
    writerCloseMaxNs = Math.max(writerCloseMaxNs, elapsedNs);
    return elapsedNs >= TimeUnit.SECONDS.toNanos(1) && slowWriterLogs++ < 5;
  }

  public void writerSweepPartitionFinished() {
    writerSweepPartitionsFinished++;
  }

  public boolean isFinished() {
    return current.phase == Phase.FINISHED;
  }

  /** Formatting happens only on the shutdown coordinator/SIT, never inside a writer partition monitor. */
  public String snapshot() {
    Operation operation = current;
    long now = operation.phase == Phase.FINISHED ? operation.start : nanoTime.getAsLong();
    StringBuilder result = new StringBuilder();
    result.append("phase=")
        .append(operation.phase)
        .append(", partition=")
        .append(operation.partition)
        .append(", phaseElapsedMs=")
        .append(TimeUnit.NANOSECONDS.toMillis(now - operation.start))
        .append(", sinceCloseRequestMs=")
        .append(TimeUnit.NANOSECONDS.toMillis(now - requestedAt))
        .append(", partitions=")
        .append(partitions)
        .append(", writerClosesFinished=")
        .append(writerClosesFinished)
        .append(", writerClosesFailed=")
        .append(writerClosesFailed)
        .append(", writerSweepPartitionsFinished=")
        .append(writerSweepPartitionsFinished)
        .append(", writerSweepPartitionsRemaining=")
        .append(Math.max(0, partitions - writerSweepPartitionsFinished))
        .append(", writerCloseInclusiveTotalMs=")
        .append(TimeUnit.NANOSECONDS.toMillis(writerCloseTotalNs))
        .append(", writerCloseInclusiveMaxMs=")
        .append(TimeUnit.NANOSECONDS.toMillis(writerCloseMaxNs))
        .append(", checkpointsFinished=")
        .append(checkpointsFinished.get())
        .append(", checkpointsRemaining=")
        .append(Math.max(0, partitions - checkpointsFinished.get()));
    for (Phase phase: Phase.values()) {
      int index = phase.ordinal();
      result.append(", ")
          .append(phase)
          .append("[finishedVisits=")
          .append(counts.get(index))
          .append(", totalMs=")
          .append(TimeUnit.NANOSECONDS.toMillis(totals.get(index)))
          .append(", maxMs=")
          .append(TimeUnit.NANOSECONDS.toMillis(maxima.get(index)))
          .append(']');
    }
    return result.toString();
  }
}
