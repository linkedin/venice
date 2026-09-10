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

  private final ThreadPoolExecutor[] stripes;

  /**
   * One private lifecycle monitor per stripe, shared only with that stripe's {@link BlockingAdmissionHandler}
   * and its {@link SignalingBlockingQueue}. A blocked admission's shutdown-state check and nonblocking
   * {@code offer} happen atomically under this monitor (see {@link StripeLifecycleMonitor#admit}), and
   * {@link #shutdown()}/{@link #shutdownNow()} take the same monitor around the stripe's state transition
   * (and, for {@code shutdownNow()}, its drain) before signaling it. This linearizes a racing admission
   * against a forced shutdown: a successful offer either happens strictly before the drain snapshot (and is
   * returned as pending by {@code shutdownNow()}) or is rejected because the stripe is already shut down; it
   * can never be offered after the drain and silently stranded. The same monitor also carries the
   * capacity-available signal a worker raises when it dequeues a task, so a blocked admission is woken the
   * instant a slot frees rather than after a fixed polling interval.
   */
  private final StripeLifecycleMonitor[] lifecycleMonitors;

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
    this.lifecycleMonitors = new StripeLifecycleMonitor[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      String stripeName = threadNamePrefix + "-" + i;
      StripeLifecycleMonitor monitor = new StripeLifecycleMonitor();
      this.lifecycleMonitors[i] = monitor;
      ThreadPoolExecutor stripe = new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new SignalingBlockingQueue(queueCapacity, monitor),
          new DaemonThreadFactory(stripeName),
          new BlockingAdmissionHandler(stripeName, monitor));
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
    for (int i = 0; i < stripes.length; i++) {
      // Take the stripe lifecycle monitor around the state transition so a concurrent blocked admission cannot
      // offer into a stripe that has just been shut down: it will observe the shutdown under the same monitor.
      // Signal under the monitor after the transition so an admission parked waiting for capacity wakes,
      // re-checks the shutdown state, and rejects rather than hanging.
      synchronized (lifecycleMonitors[i]) {
        stripes[i].shutdown();
        lifecycleMonitors[i].signalAll();
      }
    }
  }

  /**
   * Forced shutdown: attempts to stop running tasks and returns the tasks that were still queued, so the
   * caller owns their disposition.
   */
  public List<Runnable> shutdownNow() {
    List<Runnable> pending = new ArrayList<>();
    for (int i = 0; i < stripes.length; i++) {
      // Transition and drain under the stripe lifecycle monitor so the drain snapshot linearizes against any
      // racing blocked admission: a task the admission offered before this point is captured here and returned
      // as pending; an admission that has not offered yet will observe the shutdown and be rejected instead.
      // Signal under the monitor after the drain so a parked admission wakes and rejects.
      synchronized (lifecycleMonitors[i]) {
        pending.addAll(stripes[i].shutdownNow());
        lifecycleMonitors[i].signalAll();
      }
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
   * dropping it, and translates interruption into a rejection.
   *
   * <p>When a stripe queue is full the handler logs a warning naming the stripe so the mechanism layer
   * keeps the operational visibility callers previously relied on, then blocks (it never runs the task on
   * the caller thread and never drops it).</p>
   *
   * <p>The blocking loop lives in {@link StripeLifecycleMonitor#admit}: the shutdown-state check and the
   * nonblocking {@code queue.offer(task)} are performed together under the stripe's lifecycle monitor, the
   * same monitor {@link #shutdown()} and {@link #shutdownNow()} hold while they transition (and drain). This
   * makes a racing admission and a forced shutdown linearize: either this admission wins the monitor first and
   * its offer lands strictly before {@code shutdownNow()}'s drain snapshot (so the task is returned to that
   * caller as pending), or {@code shutdownNow()} wins first and this admission then observes the shutdown and
   * rejects. The task can therefore never be offered <em>after</em> the drain and be silently stranded. When
   * the queue is full the admission parks on the monitor (releasing it), so a concurrent shutdown can still
   * transition or drain; a worker dequeue or a shutdown signals the monitor to wake it immediately.</p>
   */
  private static class BlockingAdmissionHandler implements RejectedExecutionHandler {
    private final String stripeName;
    private final StripeLifecycleMonitor monitor;

    BlockingAdmissionHandler(String stripeName, StripeLifecycleMonitor monitor) {
      this.stripeName = stripeName;
      this.monitor = monitor;
    }

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
      BlockingQueue<Runnable> queue = executor.getQueue();
      LOGGER.warn("Queue full for stripe {}, blocking submitter. Queue size: {}", stripeName, queue.size());
      try {
        monitor.admit(executor, queue, task);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("Interrupted while waiting for stripe queue space", e);
      }
    }
  }

  /**
   * Per-stripe lifecycle monitor shared by the stripe's {@link BlockingAdmissionHandler}, its
   * {@link SignalingBlockingQueue}, and {@link #shutdown()}/{@link #shutdownNow()}. It coordinates blocked
   * admissions with capacity-freeing dequeues and lifecycle transitions using a single guarded condition.
   *
   * <p>{@code signalGeneration} is the guarded state: every event that a blocked admission might care about
   * (a dequeue freeing a slot, or a shutdown transition) advances it under this monitor and wakes all
   * waiters via {@link #signalAll()}. A blocked admission captures the generation while it still holds the
   * monitor and its {@code offer} has just failed, then parks until the generation advances. Because the
   * failed offer and the ensuing wait are atomic under this monitor, and every signal also holds it, a
   * freed-slot or shutdown signal between them can never be lost.</p>
   */
  private static final class StripeLifecycleMonitor {
    /** Advanced under this monitor on every dequeue that frees a slot and on every shutdown transition. */
    private long signalGeneration;

    /**
     * Blocks until {@code task} is admitted to {@code queue} or {@code executor} is shut down. The whole
     * check-offer-park loop runs under this monitor so it is atomic with {@link #signalAll()} and with the
     * shutdown transition/drain guarded by the same monitor.
     *
     * @throws RejectedExecutionException if the stripe is (or becomes) shut down before admission
     * @throws InterruptedException if the caller is interrupted while parked waiting for capacity
     */
    synchronized void admit(ThreadPoolExecutor executor, BlockingQueue<Runnable> queue, Runnable task)
        throws InterruptedException {
      while (true) {
        if (executor.isShutdown()) {
          throw new RejectedExecutionException("Stripe executor has been shut down");
        }
        if (queue.offer(task)) {
          return;
        }
        // Queue full: park until a dequeue frees a slot or a shutdown transitions the stripe, both of which
        // advance the generation under this same monitor. Wait until the generation moves past the value we
        // captured while holding the monitor; the guarded loop tolerates spurious wakeups and, together with
        // the atomic offer-then-capture above, prevents a lost wakeup.
        long awaitedGeneration = signalGeneration + 1;
        while (signalGeneration < awaitedGeneration) {
          wait();
        }
      }
    }

    /** Advances the signal and wakes every blocked admission. Callers may already hold this monitor. */
    synchronized void signalAll() {
      signalGeneration++;
      notifyAll();
    }
  }

  /**
   * A bounded {@link LinkedBlockingQueue} that signals its stripe's {@link StripeLifecycleMonitor} whenever a
   * worker dequeues a task, so a blocked admission is woken the instant a slot frees instead of after a fixed
   * polling interval. It overrides only the worker's dequeue paths ({@code take}, timed and no-arg
   * {@code poll}); {@code shutdownNow()}'s drain does its own signaling after transitioning under the monitor.
   *
   * <p>The signal is raised only <em>after</em> the {@code super} dequeue returns, i.e. after the queue's
   * internal take lock has been released, so this queue never acquires the lifecycle monitor while still
   * holding an internal queue lock. That keeps the global lock order (lifecycle monitor before any internal
   * queue lock) acyclic and avoids deadlock against a {@code shutdownNow()} that drains while holding the
   * monitor.</p>
   */
  private static final class SignalingBlockingQueue extends LinkedBlockingQueue<Runnable> {
    private static final long serialVersionUID = 1L;

    /** Transient: never serialized in practice, and the monitor is not itself {@link java.io.Serializable}. */
    private final transient StripeLifecycleMonitor monitor;

    SignalingBlockingQueue(int capacity, StripeLifecycleMonitor monitor) {
      super(capacity);
      this.monitor = monitor;
    }

    @Override
    public Runnable take() throws InterruptedException {
      Runnable task = super.take();
      // A slot just freed. Signal only now that super.take() has released the internal take lock.
      monitor.signalAll();
      return task;
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
      Runnable task = super.poll(timeout, unit);
      if (task != null) {
        monitor.signalAll();
      }
      return task;
    }

    @Override
    public Runnable poll() {
      Runnable task = super.poll();
      if (task != null) {
        monitor.signalAll();
      }
      return task;
    }
  }
}
