package com.linkedin.alpini.base.registry.impl;

import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.misc.ScheduledFuture;
import com.linkedin.alpini.base.registry.ShutdownableScheduledExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class ShutdownableScheduledExecutorServiceImpl<E extends ScheduledExecutorService>
    extends ShutdownableExecutorServiceImpl<E> implements ShutdownableScheduledExecutorService {
  public ShutdownableScheduledExecutorServiceImpl(@Nonnull E executor) {
    super(executor);
  }

  @Override
  public void shutdown() {
    for (Runnable task: _e.shutdownNow()) {
      if (task instanceof ScheduledFuture) {
        ((ScheduledFuture) task).cancel(false);
      }
    }
  }

  /**
   * @see ScheduledExecutorService#schedule(Runnable, long, java.util.concurrent.TimeUnit)
   */
  @Override
  @Nonnull
  public final ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
    return _e.schedule(command, delay, unit);
  }

  /**
   * @see ScheduledExecutorService#schedule(java.util.concurrent.Callable, long, java.util.concurrent.TimeUnit)
   */
  @Override
  @Nonnull
  public final <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
    return _e.schedule(callable, delay, unit);
  }

  /**
   * @see ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, java.util.concurrent.TimeUnit)
   */
  @Override
  @Nonnull
  public final ScheduledFuture<?> scheduleAtFixedRate(
      @Nonnull Runnable command,
      long initialDelay,
      long period,
      @Nonnull TimeUnit unit) {
    return _e.scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  /**
   * @see ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, java.util.concurrent.TimeUnit)
   */
  @Override
  @Nonnull
  public final ScheduledFuture<?> scheduleWithFixedDelay(
      @Nonnull Runnable command,
      long initialDelay,
      long delay,
      @Nonnull TimeUnit unit) {
    return _e.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }
}
