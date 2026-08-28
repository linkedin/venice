package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.ObjIntConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A minimal, producer-agnostic, bounded partition-striped executor.
 *
 * <p>The executor owns a fixed number of <em>stripes</em>. Each stripe is exactly one FIFO worker
 * thread backed by a bounded queue, so tasks routed to the same stripe run in submission order while
 * different stripes run in parallel. A caller routes work by partition; the deterministic mapping
 * {@link #stripeFor(int)} guarantees that a given partition always lands on the same stripe. During
 * normal dispatch a partition blocked on its stripe therefore does not stall partitions mapped to
 * <em>other</em> stripes; partitions that share a stripe are serialized and can wait on one another.</p>
 *
 * <p>Admission is bounded and blocking: when a stripe queue is full the submitting thread blocks until
 * space frees up. It never runs the task on the caller thread and never silently drops it. Shutdown or
 * interruption of a blocked submitter surfaces as a {@link RejectedExecutionException}; callers decide
 * how to react (fall back, fail a write, etc.).</p>
 *
 * <p>This class is intentionally free of any producer concepts. It knows nothing about Venice writers,
 * callbacks, futures, sticky errors, flush policy, metrics, retries, or inline execution; it only moves
 * opaque {@link Runnable}s onto per-partition worker threads. Callers layer those concerns on top.</p>
 *
 * <p>Metrics integrations that need the underlying {@link ThreadPoolExecutor} (for example to wrap each
 * stripe in a stats gauge) can supply a construction-time {@code stripeObserver}; the executor exposes
 * no mutable post-construction accessor for its internals.</p>
 */
public final class PartitionStripedExecutor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionStripedExecutor.class);

  /** How often a blocked submitter re-checks for shutdown while waiting for queue space. */
  private static final long OFFER_POLL_MS = 100;

  private final ThreadPoolExecutor[] stripes;

  public PartitionStripedExecutor(int stripeCount, int queueCapacity, String threadNamePrefix) {
    this(stripeCount, queueCapacity, threadNamePrefix, null);
  }

  /**
   * @param stripeCount number of stripes (worker threads); must be positive
   * @param queueCapacity bounded queue capacity per stripe; must be positive
   * @param threadNamePrefix per-stripe thread names are {@code <threadNamePrefix>-<stripe>-t<n>}
   * @param stripeObserver optional construction-time observer invoked once per stripe with the stripe's
   *                       {@link ThreadPoolExecutor} and index (e.g. to register metrics); may be null
   */
  public PartitionStripedExecutor(
      int stripeCount,
      int queueCapacity,
      String threadNamePrefix,
      ObjIntConsumer<ThreadPoolExecutor> stripeObserver) {
    if (stripeCount <= 0) {
      throw new IllegalArgumentException("stripeCount must be positive, got " + stripeCount);
    }
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be positive, got " + queueCapacity);
    }
    this.stripes = new ThreadPoolExecutor[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      String stripeName = threadNamePrefix + "-" + i;
      ThreadPoolExecutor stripe = new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(queueCapacity),
          new DaemonThreadFactory(stripeName),
          new BlockingAdmissionHandler(stripeName));
      this.stripes[i] = stripe;
      if (stripeObserver != null) {
        stripeObserver.accept(stripe, i);
      }
    }
  }

  /** @return the number of stripes (worker threads). */
  public int getStripeCount() {
    return stripes.length;
  }

  /**
   * Deterministically maps a partition to a stripe index in {@code [0, stripeCount)}.
   *
   * <p>Uses a bitwise mask rather than {@link Math#abs(int)} because {@code Math.abs(Integer.MIN_VALUE)}
   * is still negative; the mask keeps the index non-negative for every input, including
   * {@link Integer#MIN_VALUE}.</p>
   */
  public int stripeFor(int partition) {
    return (partition & Integer.MAX_VALUE) % stripes.length;
  }

  /**
   * Routes a task to the stripe owning {@code partition}, blocking if that stripe's queue is full.
   *
   * @throws RejectedExecutionException if the executor is shut down or the caller is interrupted while
   *         waiting for queue space
   */
  public void submit(int partition, Runnable task) {
    stripes[stripeFor(partition)].execute(task);
  }

  /**
   * Routes a task to an exact stripe (used for per-stripe fence markers), blocking if its queue is full.
   *
   * @throws RejectedExecutionException if the executor is shut down or the caller is interrupted while
   *         waiting for queue space
   */
  public void executeOnStripe(int stripe, Runnable task) {
    stripes[stripe].execute(task);
  }

  /** @return the current queued (not-yet-running) task count for a stripe. */
  public int getStripeQueueSize(int stripe) {
    return stripes[stripe].getQueue().size();
  }

  /** @return the summed queued task count across all stripes. */
  public int getTotalQueueSize() {
    int total = 0;
    for (ThreadPoolExecutor stripe: stripes) {
      total += stripe.getQueue().size();
    }
    return total;
  }

  /** Graceful shutdown: stops accepting new tasks; already-queued tasks still run. */
  public void shutdown() {
    for (ThreadPoolExecutor stripe: stripes) {
      stripe.shutdown();
    }
  }

  /**
   * Forced shutdown: attempts to stop running tasks and returns the tasks that were still queued, so the
   * caller owns their disposition.
   */
  public List<Runnable> shutdownNow() {
    List<Runnable> pending = new ArrayList<>();
    for (ThreadPoolExecutor stripe: stripes) {
      pending.addAll(stripe.shutdownNow());
    }
    return pending;
  }

  /**
   * Awaits termination of every stripe against a single shared deadline derived from {@code timeout}.
   *
   * @return true if all stripes terminated within the deadline
   */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    boolean terminated = true;
    for (ThreadPoolExecutor stripe: stripes) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      terminated &= stripe.awaitTermination(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS);
    }
    return terminated;
  }

  /**
   * Blocks the submitting thread until queue space is available instead of running the task inline or
   * dropping it. Wakes periodically to observe shutdown, and translates interruption into a rejection.
   *
   * <p>When a stripe queue is full the handler logs a warning naming the stripe so the mechanism layer
   * keeps the operational visibility callers previously relied on, then blocks (it never runs the task on
   * the caller thread and never drops it).</p>
   */
  private static class BlockingAdmissionHandler implements RejectedExecutionHandler {
    private final String stripeName;

    BlockingAdmissionHandler(String stripeName) {
      this.stripeName = stripeName;
    }

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        throw new RejectedExecutionException("Stripe executor has been shut down");
      }
      BlockingQueue<Runnable> queue = executor.getQueue();
      LOGGER.warn("Queue full for stripe {}, blocking submitter. Queue size: {}", stripeName, queue.size());
      try {
        while (!queue.offer(task, OFFER_POLL_MS, TimeUnit.MILLISECONDS)) {
          if (executor.isShutdown()) {
            throw new RejectedExecutionException("Stripe executor has been shut down");
          }
        }
        // The offer won a race, but a concurrent shutdownNow() may have already drained the queue and
        // returned its snapshot without this task. If we are now shut down and the task is still queued
        // (not yet taken by a graceful-draining worker), pull it back out and reject it so it is never
        // silently stranded. If a draining worker already claimed it, remove() fails and we let it run.
        if (executor.isShutdown() && queue.remove(task)) {
          throw new RejectedExecutionException("Stripe executor has been shut down");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("Interrupted while waiting for stripe queue space", e);
      }
    }
  }
}
