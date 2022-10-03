package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.impl.DefaultAsyncFutureTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * An {@link ExecutorService} that executes each submitted task using
 * one of possibly several pooled threads, normally configured
 * using {@link Executors} factory methods.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ThreadPoolExecutor extends java.util.concurrent.ThreadPoolExecutor implements ExecutorService {
  /**
   * Creates a new {@code ThreadPoolExecutor} with the given initial
   * parameters and default thread factory and rejected execution handler.
   * It may be more convenient to use one of the {@link Executors} factory
   * methods instead of this general purpose constructor.
   *
   * @param corePoolSize    the number of threads to keep in the pool, even
   *                        if they are idle, unless {@code allowCoreThreadTimeOut} is set
   * @param maximumPoolSize the maximum number of threads to allow in the
   *                        pool
   * @param keepAliveTime   when the number of threads is greater than
   *                        the core, this is the maximum time that excess idle threads
   *                        will wait for new tasks before terminating.
   * @param unit            the time unit for the {@code keepAliveTime} argument
   * @param workQueue       the queue to use for holding tasks before they are
   *                        executed.  This queue will hold only the {@code Runnable}
   *                        tasks submitted by the {@code execute} method.
   * @throws IllegalArgumentException if one of the following holds:<br>
   *                                  {@code corePoolSize < 0}<br>
   *                                  {@code keepAliveTime < 0}<br>
   *                                  {@code maximumPoolSize <= 0}<br>
   *                                  {@code maximumPoolSize < corePoolSize}
   * @throws NullPointerException     if {@code workQueue} is null
   */
  public ThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  /**
   * Creates a new {@code ThreadPoolExecutor} with the given initial
   * parameters and default rejected execution handler.
   *
   * @param corePoolSize    the number of threads to keep in the pool, even
   *                        if they are idle, unless {@code allowCoreThreadTimeOut} is set
   * @param maximumPoolSize the maximum number of threads to allow in the
   *                        pool
   * @param keepAliveTime   when the number of threads is greater than
   *                        the core, this is the maximum time that excess idle threads
   *                        will wait for new tasks before terminating.
   * @param unit            the time unit for the {@code keepAliveTime} argument
   * @param workQueue       the queue to use for holding tasks before they are
   *                        executed.  This queue will hold only the {@code Runnable}
   *                        tasks submitted by the {@code execute} method.
   * @param threadFactory   the factory to use when the executor
   *                        creates a new thread
   * @throws IllegalArgumentException if one of the following holds:<br>
   *                                  {@code corePoolSize < 0}<br>
   *                                  {@code keepAliveTime < 0}<br>
   *                                  {@code maximumPoolSize <= 0}<br>
   *                                  {@code maximumPoolSize < corePoolSize}
   * @throws NullPointerException     if {@code workQueue}
   *                                  or {@code threadFactory} is null
   */
  public ThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  /**
   * Creates a new {@code ThreadPoolExecutor} with the given initial
   * parameters and default thread factory.
   *
   * @param corePoolSize    the number of threads to keep in the pool, even
   *                        if they are idle, unless {@code allowCoreThreadTimeOut} is set
   * @param maximumPoolSize the maximum number of threads to allow in the
   *                        pool
   * @param keepAliveTime   when the number of threads is greater than
   *                        the core, this is the maximum time that excess idle threads
   *                        will wait for new tasks before terminating.
   * @param unit            the time unit for the {@code keepAliveTime} argument
   * @param workQueue       the queue to use for holding tasks before they are
   *                        executed.  This queue will hold only the {@code Runnable}
   *                        tasks submitted by the {@code execute} method.
   * @param handler         the handler to use when execution is blocked
   *                        because the thread bounds and queue capacities are reached
   * @throws IllegalArgumentException if one of the following holds:<br>
   *                                  {@code corePoolSize < 0}<br>
   *                                  {@code keepAliveTime < 0}<br>
   *                                  {@code maximumPoolSize <= 0}<br>
   *                                  {@code maximumPoolSize < corePoolSize}
   * @throws NullPointerException     if {@code workQueue}
   *                                  or {@code handler} is null
   */
  public ThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
  }

  /**
   * Creates a new {@code ThreadPoolExecutor} with the given initial
   * parameters.
   *
   * @param corePoolSize    the number of threads to keep in the pool, even
   *                        if they are idle, unless {@code allowCoreThreadTimeOut} is set
   * @param maximumPoolSize the maximum number of threads to allow in the
   *                        pool
   * @param keepAliveTime   when the number of threads is greater than
   *                        the core, this is the maximum time that excess idle threads
   *                        will wait for new tasks before terminating.
   * @param unit            the time unit for the {@code keepAliveTime} argument
   * @param workQueue       the queue to use for holding tasks before they are
   *                        executed.  This queue will hold only the {@code Runnable}
   *                        tasks submitted by the {@code execute} method.
   * @param threadFactory   the factory to use when the executor
   *                        creates a new thread
   * @param handler         the handler to use when execution is blocked
   *                        because the thread bounds and queue capacities are reached
   * @throws IllegalArgumentException if one of the following holds:<br>
   *                                  {@code corePoolSize < 0}<br>
   *                                  {@code keepAliveTime < 0}<br>
   *                                  {@code maximumPoolSize <= 0}<br>
   *                                  {@code maximumPoolSize < corePoolSize}
   * @throws NullPointerException     if {@code workQueue}
   *                                  or {@code threadFactory} or {@code handler} is null
   */
  public ThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  /**
   * Returns a {@code RunnableFuture} for the given runnable and default
   * value.
   *
   * @param runnable the runnable task being wrapped
   * @param value    the default value for the returned future
   * @return a {@code RunnableFuture} which, when run, will run the
   * underlying runnable and which, as a {@code Future}, will yield
   * the given value as its result and provide for cancellation of
   * the underlying task
   * @since 1.6
   */
  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new DefaultAsyncFutureTask<>(Executors.callable(runnable, value), true);
  }

  /**
   * Returns a {@code RunnableFuture} for the given callable task.
   *
   * @param callable the callable task being wrapped
   * @return a {@code RunnableFuture} which, when run, will call the
   * underlying callable and which, as a {@code Future}, will yield
   * the callable's result as its result and provide for
   * cancellation of the underlying task
   * @since 1.6
   */
  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new DefaultAsyncFutureTask<>(callable, true);
  }

  /**
   * @param task {@inheritDoc}
   * @throws java.util.concurrent.RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   */
  @Override
  @Nonnull
  public AsyncFuture<?> submit(@Nonnull Runnable task) {
    return (AsyncFuture<?>) super.submit(task);
  }

  /**
   * @param task {@inheritDoc}
   * @param result {@inheritDoc}
   * @throws java.util.concurrent.RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   */
  @Override
  @Nonnull
  public <T> AsyncFuture<T> submit(@Nonnull Runnable task, T result) {
    return (AsyncFuture<T>) super.submit(task, result);
  }

  /**
   * @param task {@inheritDoc}
   * @throws java.util.concurrent.RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   */
  @Override
  @Nonnull
  public <T> AsyncFuture<T> submit(@Nonnull Callable<T> task) {
    return (AsyncFuture<T>) super.submit(task);
  }
}
