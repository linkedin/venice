package com.linkedin.alpini.base.registry.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.registry.ShutdownableExecutorService;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class ShutdownableExecutorServiceImpl<E extends ExecutorService> extends AbstractExecutorService
    implements ShutdownableExecutorService {
  protected final @Nonnull E _e;

  public ShutdownableExecutorServiceImpl(@Nonnull E executor) {
    _e = Objects.requireNonNull(executor);
  }

  /**
   * @see ExecutorService#execute(Runnable)
   */
  @Override
  public final void execute(@Nonnull Runnable command) {
    _e.execute(command);
  }

  /**
   * @see ExecutorService#shutdown()
   */
  @Override
  public void shutdown() {
    _e.shutdown();
  }

  /**
   * @see ExecutorService#shutdownNow()
   */
  @Override
  @Nonnull
  public final List<Runnable> shutdownNow() {
    return _e.shutdownNow();
  }

  /**
   * @see ExecutorService#isShutdown()
   */
  @Override
  public final boolean isShutdown() {
    return _e.isShutdown();
  }

  /**
   * @see ExecutorService#isTerminated()
   */
  @Override
  public final boolean isTerminated() {
    return _e.isTerminated();
  }

  /**
   * @see com.linkedin.alpini.base.registry.Shutdownable#waitForShutdown()
   */
  @Override
  public final void waitForShutdown() throws InterruptedException, IllegalStateException {
    while (!awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS)) {
      Thread.yield();
    }
  }

  /**
   * @see com.linkedin.alpini.base.registry.Shutdownable#waitForShutdown(long)
   */
  @Override
  public final void waitForShutdown(long timeoutInMs)
      throws InterruptedException, IllegalStateException, TimeoutException {
    if (!awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException();
    }
  }

  /**
   * @see ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public final boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return _e.awaitTermination(timeout, unit);
  }

  /**
   * @see ExecutorService#submit(Runnable)
   */
  @Override
  @Nonnull
  public final AsyncFuture<?> submit(@Nonnull Runnable task) {
    return _e.submit(task);
  }

  /**
   * @see ExecutorService#submit(Callable)
   */
  @Override
  @Nonnull
  public final <T> AsyncFuture<T> submit(@Nonnull Callable<T> task) {
    return _e.submit(task);
  }

  /**
   * @see ExecutorService#submit(Runnable, Object)
   */
  @Override
  @Nonnull
  public final <T> AsyncFuture<T> submit(@Nonnull Runnable task, T result) {
    return _e.submit(task, result);
  }

  /**
   * @see ExecutorService#invokeAll(Collection)
   */
  @Override
  @Nonnull
  public final <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _e.invokeAll(tasks);
  }

  /**
   * @see ExecutorService#invokeAll(Collection, long, TimeUnit)
   */
  @Override
  @Nonnull
  public final <T> List<Future<T>> invokeAll(
      @Nonnull Collection<? extends Callable<T>> tasks,
      long timeout,
      @Nonnull TimeUnit unit) throws InterruptedException {
    return _e.invokeAll(tasks, timeout, unit);
  }

  /**
   * @see ExecutorService#invokeAny(Collection)
   */
  @Override
  public final <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return _e.invokeAny(tasks);
  }

  /**
   * @see ExecutorService#invokeAny(Collection, long, TimeUnit)
   */
  @Override
  public final <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _e.invokeAny(tasks, timeout, unit);
  }

  /**
   * @see ShutdownableExecutorService#unwrap(Class)
   */
  @Override
  public final <T extends ExecutorService> T unwrap(Class<T> clazz) {
    if (clazz.isAssignableFrom(_e.getClass())) {
      return clazz.cast(_e);
    }
    throw new ClassCastException(
        String.format("_e.class %s is not clazz: %s", _e.getClass().getCanonicalName(), clazz));
  }
}
