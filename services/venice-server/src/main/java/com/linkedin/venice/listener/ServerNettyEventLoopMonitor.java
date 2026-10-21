package com.linkedin.venice.listener;

import com.linkedin.venice.stats.ServerNettyEventLoopStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Time;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Samples outside the worker group so a blocked loop cannot stop backlog collection or accumulate diagnostic probes.
 */
public class ServerNettyEventLoopMonitor implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(ServerNettyEventLoopMonitor.class);

  private final List<EventLoopProbe> probes = new ArrayList<>();
  private final ServerNettyEventLoopStats stats;
  private final ScheduledExecutorService sampler;
  private final LongSupplier nanoTime;
  private ScheduledFuture<?> samplingTask;
  private volatile boolean closed;

  public ServerNettyEventLoopMonitor(
      EventLoopGroup workerGroup,
      MetricsRepository metricsRepository,
      LogContext logContext) {
    this(
        workerGroup,
        metricsRepository,
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("NettyEventLoopMonitor", logContext)),
        System::nanoTime);
  }

  ServerNettyEventLoopMonitor(
      EventLoopGroup workerGroup,
      MetricsRepository metricsRepository,
      ScheduledExecutorService sampler,
      LongSupplier nanoTime) {
    this.sampler = sampler;
    this.nanoTime = nanoTime;
    for (EventExecutor executor: workerGroup) {
      if (!(executor instanceof SingleThreadEventExecutor)) {
        throw new IllegalArgumentException("Netty event-loop monitoring requires single-threaded event executors");
      }
      probes.add(new EventLoopProbe((SingleThreadEventExecutor) executor));
    }
    stats = new ServerNettyEventLoopStats(metricsRepository, probes.size());
  }

  public synchronized void start() {
    if (closed) {
      throw new IllegalStateException("Cannot start a closed Netty event-loop monitor");
    }
    if (samplingTask == null) {
      samplingTask = sampler.scheduleWithFixedDelay(this::sample, 0, 1, TimeUnit.SECONDS);
    }
  }

  synchronized void sample() {
    if (closed) {
      return;
    }
    long total = 0;
    int max = 0;
    for (EventLoopProbe probe: probes) {
      int pending = probe.eventLoop.pendingTasks();
      total += pending;
      max = Math.max(max, pending);
    }
    stats.recordPendingTasks(total, max);
    for (EventLoopProbe probe: probes) {
      probe.submit();
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (samplingTask != null) {
      samplingTask.cancel(false);
    }
    sampler.shutdownNow();
  }

  private final class EventLoopProbe implements Runnable {
    private final SingleThreadEventExecutor eventLoop;
    private final AtomicBoolean outstanding = new AtomicBoolean();
    private long submittedAtNs;

    private EventLoopProbe(SingleThreadEventExecutor eventLoop) {
      this.eventLoop = eventLoop;
    }

    private void submit() {
      if (eventLoop.isShuttingDown() || !outstanding.compareAndSet(false, true)) {
        return;
      }
      submittedAtNs = nanoTime.getAsLong();
      try {
        eventLoop.execute(this);
      } catch (RejectedExecutionException e) {
        outstanding.set(false);
        if (!eventLoop.isShuttingDown()) {
          LOGGER.warn("Netty event loop rejected a scheduling-delay probe", e);
        }
      }
    }

    @Override
    public void run() {
      try {
        if (!closed) {
          stats.recordSchedulingDelay((nanoTime.getAsLong() - submittedAtNs) / (double) Time.NS_PER_MS);
        }
      } finally {
        outstanding.set(false);
      }
    }
  }
}
