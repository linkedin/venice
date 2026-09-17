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
 * A minimal, producer-agnostic, bounded partition-striped executor. Each stripe is one FIFO worker thread with a
 * bounded queue; a partition is mapped by {@link #stripeFor(int)} and always lands on the same stripe, so same-stripe
 * tasks run in submission order while different stripes run in parallel and never stall one another. Admission is
 * bounded and blocking (never caller-runs, never drops); it carries no producer concepts (writers, callbacks,
 * futures, flush, retries), only opaque {@link Runnable}s.
 */
public final class PartitionStripedExecutor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionStripedExecutor.class);

  private final ThreadPoolExecutor[] stripes;

  private final StripeLifecycleMonitor[] lifecycleMonitors;

  public PartitionStripedExecutor(int stripeCount, int queueCapacity, String threadNamePrefix) {
    this(stripeCount, queueCapacity, threadNamePrefix, null);
  }

  /**
   * @param threadNamePrefix per-stripe thread names are {@code <threadNamePrefix>-<stripe>-t<n>}
   * @param stripeObserver optional construction-time observer invoked once per stripe with its
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

  public int getStripeCount() {
    return stripes.length;
  }

  /** Maps a partition to a stripe in {@code [0, stripeCount)}; masks the sign bit so {@link Integer#MIN_VALUE} stays in range. */
  public int stripeFor(int partition) {
    return (partition & Integer.MAX_VALUE) % stripes.length;
  }

  /**
   * Routes a task to the stripe owning {@code partition}, blocking if that stripe's queue is full.
   * @throws RejectedExecutionException if the executor is shut down or the caller is interrupted while waiting
   */
  public void submit(int partition, Runnable task) {
    stripes[stripeFor(partition)].execute(task);
  }

  /** Routes a task to an exact stripe (used for per-stripe fence markers), blocking if its queue is full. */
  public void executeOnStripe(int stripe, Runnable task) {
    stripes[stripe].execute(task);
  }

  public int getStripeQueueSize(int stripe) {
    return stripes[stripe].getQueue().size();
  }

  public int getTotalQueueSize() {
    int total = 0;
    for (ThreadPoolExecutor stripe: stripes) {
      total += stripe.getQueue().size();
    }
    return total;
  }

  public void shutdown() {
    for (int i = 0; i < stripes.length; i++) {
      // Transition and signal under the monitor so a blocked admission rejects instead of hanging; shutdownNow's
      // drain takes the same monitor, so admission, shutdown, and drain all linearize against each other.
      synchronized (lifecycleMonitors[i]) {
        stripes[i].shutdown();
        lifecycleMonitors[i].signalAll();
      }
    }
  }

  public List<Runnable> shutdownNow() {
    List<Runnable> pending = new ArrayList<>();
    for (int i = 0; i < stripes.length; i++) {
      synchronized (lifecycleMonitors[i]) {
        pending.addAll(stripes[i].shutdownNow());
        lifecycleMonitors[i].signalAll();
      }
    }
    return pending;
  }

  /** Awaits termination of every stripe against a single shared deadline derived from {@code timeout}. */
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
   * Blocks the submitting thread until queue space frees instead of caller-running or dropping the task, mapping
   * interruption to {@link RejectedExecutionException}. The wait/shutdown loop lives in {@link StripeLifecycleMonitor#admit}.
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
   * Per-stripe monitor: admissions block here for capacity, and every dequeue and lifecycle transition advances
   * {@code signalGeneration} under the monitor before waking waiters. Capturing the generation after a failed
   * offer, under the monitor, is what rules out a lost wakeup.
   */
  private static final class StripeLifecycleMonitor {
    private long signalGeneration;

    synchronized void admit(ThreadPoolExecutor executor, BlockingQueue<Runnable> queue, Runnable task)
        throws InterruptedException {
      while (true) {
        if (executor.isShutdown()) {
          throw new RejectedExecutionException("Stripe executor has been shut down");
        }
        if (queue.offer(task)) {
          return;
        }
        long awaitedGeneration = signalGeneration + 1;
        while (signalGeneration < awaitedGeneration) {
          wait();
        }
      }
    }

    synchronized void signalAll() {
      signalGeneration++;
      notifyAll();
    }
  }

  /**
   * Bounded queue that signals its {@link StripeLifecycleMonitor} after {@code super} dequeues a task. Signalling
   * only once the internal take lock is released keeps the lock order (monitor before internal queue lock) acyclic
   * against a draining {@code shutdownNow()}.
   */
  private static final class SignalingBlockingQueue extends LinkedBlockingQueue<Runnable> {
    private static final long serialVersionUID = 1L;

    private final transient StripeLifecycleMonitor monitor;

    SignalingBlockingQueue(int capacity, StripeLifecycleMonitor monitor) {
      super(capacity);
      this.monitor = monitor;
    }

    @Override
    public Runnable take() throws InterruptedException {
      Runnable task = super.take();
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
