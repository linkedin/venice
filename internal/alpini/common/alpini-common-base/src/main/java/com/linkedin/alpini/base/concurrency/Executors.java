package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.misc.ScheduledFuture;
import com.linkedin.alpini.base.misc.ThreadPoolExecutor;
import com.linkedin.alpini.base.misc.TimeScheduledThreadPoolExecutor;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;


/**
 * Factory and utility methods for {@link java.util.concurrent.Executor}, {@link ExecutorService},
 * {@link ScheduledExecutorService}, {@link ThreadFactory}, and {@link Callable}
 * classes defined in this package.
 *
 * The scheduled executors constructed respect {@link com.linkedin.alpini.base.misc.Time}
 *
 * This utility class is analogious to {@link java.util.concurrent.Executors}.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum Executors {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  /**
   * Creates a thread pool that reuses a fixed number of threads
   * operating off a shared unbounded queue.  At any point, at most
   * {@code nThreads} threads will be active processing tasks.
   * If additional tasks are submitted when all threads are active,
   * they will wait in the queue until a thread is available.
   * If any thread terminates due to a failure during execution
   * prior to shutdown, a new one will take its place if needed to
   * execute subsequent tasks.  The threads in the pool will exist
   * until it is explicitly {@link ExecutorService#shutdown shutdown}.
   *
   * @param nThreads the number of threads in the pool
   * @return the newly created thread pool
   * @throws IllegalArgumentException if {@code nThreads <= 0}
   */
  public static ExecutorService newFixedThreadPool(int nThreads) {
    return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  /**
   * Creates a thread pool that reuses a fixed number of threads
   * operating off a shared unbounded queue, using the provided
   * ThreadFactory to create new threads when needed.  At any point,
   * at most {@code nThreads} threads will be active processing
   * tasks.  If additional tasks are submitted when all threads are
   * active, they will wait in the queue until a thread is
   * available.  If any thread terminates due to a failure during
   * execution prior to shutdown, a new one will take its place if
   * needed to execute subsequent tasks.  The threads in the pool will
   * exist until it is explicitly {@link ExecutorService#shutdown
   * shutdown}.
   *
   * @param nThreads the number of threads in the pool
   * @param threadFactory the factory to use when creating new threads
   * @return the newly created thread pool
   * @throws NullPointerException if threadFactory is null
   * @throws IllegalArgumentException if {@code nThreads <= 0}
   */
  public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
    return new ThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        threadFactory);
  }

  /**
   * Creates an Executor that uses a single worker thread operating
   * off an unbounded queue. (Note however that if this single
   * thread terminates due to a failure during execution prior to
   * shutdown, a new one will take its place if needed to execute
   * subsequent tasks.)  Tasks are guaranteed to execute
   * sequentially, and no more than one task will be active at any
   * given time. Unlike the otherwise equivalent
   * {@code newFixedThreadPool(1)} the returned executor is
   * guaranteed not to be reconfigurable to use additional threads.
   *
   * @return the newly created single-threaded Executor
   */
  public static ExecutorService newSingleThreadExecutor() {
    return new FinalizableDelegatedExecutorService(
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()));
  }

  /**
   * Creates an Executor that uses a single worker thread operating
   * off an unbounded queue, and uses the provided ThreadFactory to
   * create a new thread when needed. Unlike the otherwise
   * equivalent {@code newFixedThreadPool(1, threadFactory)} the
   * returned executor is guaranteed not to be reconfigurable to use
   * additional threads.
   *
   * @param threadFactory the factory to use when creating new
   * threads
   *
   * @return the newly created single-threaded Executor
   * @throws NullPointerException if threadFactory is null
   */
  public static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory) {
    return new FinalizableDelegatedExecutorService(
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory));
  }

  /**
   * Creates a thread pool that creates new threads as needed, but
   * will reuse previously constructed threads when they are
   * available.  These pools will typically improve the performance
   * of programs that execute many short-lived asynchronous tasks.
   * Calls to {@code execute} will reuse previously constructed
   * threads if available. If no existing thread is available, a new
   * thread will be created and added to the pool. Threads that have
   * not been used for sixty seconds are terminated and removed from
   * the cache. Thus, a pool that remains idle for long enough will
   * not consume any resources. Note that pools with similar
   * properties but different details (for example, timeout parameters)
   * may be created using {@link java.util.concurrent.ThreadPoolExecutor} constructors.
   *
   * @return the newly created thread pool
   */
  public static ExecutorService newCachedThreadPool() {
    return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
  }

  /**
   * Creates a thread pool that creates new threads as needed, but
   * will reuse previously constructed threads when they are
   * available, and uses the provided
   * ThreadFactory to create new threads when needed.
   * @param threadFactory the factory to use when creating new threads
   * @return the newly created thread pool
   * @throws NullPointerException if threadFactory is null
   */
  public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
    return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
  }

  /**
   * Creates a single-threaded executor that can schedule commands
   * to run after a given delay, or to execute periodically.
   * (Note however that if this single
   * thread terminates due to a failure during execution prior to
   * shutdown, a new one will take its place if needed to execute
   * subsequent tasks.)  Tasks are guaranteed to execute
   * sequentially, and no more than one task will be active at any
   * given time. Unlike the otherwise equivalent
   * {@code newScheduledThreadPool(1)} the returned executor is
   * guaranteed not to be reconfigurable to use additional threads.
   * @return the newly created scheduled executor
   */
  public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
    return unconfigurableScheduledExecutorService(new TimeScheduledThreadPoolExecutor(1));
  }

  /**
   * Creates a single-threaded executor that can schedule commands
   * to run after a given delay, or to execute periodically.  (Note
   * however that if this single thread terminates due to a failure
   * during execution prior to shutdown, a new one will take its
   * place if needed to execute subsequent tasks.)  Tasks are
   * guaranteed to execute sequentially, and no more than one task
   * will be active at any given time. Unlike the otherwise
   * equivalent {@code newScheduledThreadPool(1, threadFactory)}
   * the returned executor is guaranteed not to be reconfigurable to
   * use additional threads.
   * @param threadFactory the factory to use when creating new
   * threads
   * @return a newly created scheduled executor
   * @throws NullPointerException if threadFactory is null
   */
  public static ScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
    return unconfigurableScheduledExecutorService(new TimeScheduledThreadPoolExecutor(1, threadFactory));
  }

  /**
   * Creates a thread pool that can schedule commands to run after a
   * given delay, or to execute periodically.
   * @param corePoolSize the number of threads to keep in the pool,
   * even if they are idle
   * @return a newly created scheduled thread pool
   * @throws IllegalArgumentException if {@code corePoolSize < 0}
   */
  public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize) {
    return new TimeScheduledThreadPoolExecutor(corePoolSize);
  }

  /**
   * Creates a thread pool that can schedule commands to run after a
   * given delay, or to execute periodically.
   * @param corePoolSize the number of threads to keep in the pool,
   * even if they are idle
   * @param threadFactory the factory to use when the executor
   * creates a new thread
   * @return a newly created scheduled thread pool
   * @throws IllegalArgumentException if {@code corePoolSize < 0}
   * @throws NullPointerException if threadFactory is null
   */
  public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
    return new TimeScheduledThreadPoolExecutor(corePoolSize, threadFactory);
  }

  /**
   * Returns an object that delegates all defined {@link
   * ExecutorService} methods to the given executor, but not any
   * other methods that might otherwise be accessible using
   * casts. This provides a way to safely "freeze" configuration and
   * disallow tuning of a given concrete implementation.
   * @param executor the underlying implementation
   * @return an {@code ExecutorService} instance
   * @throws NullPointerException if executor null
   */
  public static ExecutorService unconfigurableExecutorService(ExecutorService executor) {
    return new DelegatedExecutorService(Objects.requireNonNull(executor));
  }

  /**
   * Returns an object that delegates all defined {@link
   * ScheduledExecutorService} methods to the given executor, but
   * not any other methods that might otherwise be accessible using
   * casts. This provides a way to safely "freeze" configuration and
   * disallow tuning of a given concrete implementation.
   * @param executor the underlying implementation
   * @return a {@code ScheduledExecutorService} instance
   * @throws NullPointerException if executor null
   */
  public static ScheduledExecutorService unconfigurableScheduledExecutorService(ScheduledExecutorService executor) {
    return new DelegatedScheduledExecutorService(Objects.requireNonNull(executor));
  }

  /**
   * Returns a default thread factory used to create new threads.
   * This factory creates all new threads used by an Executor in the
   * same {@link ThreadGroup}. If there is a {@link
   * java.lang.SecurityManager}, it uses the group of {@link
   * System#getSecurityManager}, else the group of the thread
   * invoking this {@code defaultThreadFactory} method. Each new
   * thread is created as a non-daemon thread with priority set to
   * the smaller of {@code Thread.NORM_PRIORITY} and the maximum
   * priority permitted in the thread group.  New threads have names
   * accessible via {@link Thread#getName} of
   * <em>pool-N-thread-M</em>, where <em>N</em> is the sequence
   * number of this factory, and <em>M</em> is the sequence number
   * of the thread created by this factory.
   * @return a thread factory
   */
  public static ThreadFactory defaultThreadFactory() {
    return java.util.concurrent.Executors.defaultThreadFactory();
  }

  /**
   * Returns a thread factory used to create new threads that
   * have the same permissions as the current thread.
   * This factory creates threads with the same settings as {@link
   * Executors#defaultThreadFactory}, additionally setting the
   * AccessControlContext and contextClassLoader of new threads to
   * be the same as the thread invoking this
   * {@code privilegedThreadFactory} method.  A new
   * {@code privilegedThreadFactory} can be created within an
   * {@link java.security.AccessController#doPrivileged AccessController.doPrivileged}
   * action setting the current thread's access control context to
   * create threads with the selected permission settings holding
   * within that action.
   *
   * <p>Note that while tasks running within such threads will have
   * the same access control and class loader settings as the
   * current thread, they need not have the same {@link
   * java.lang.ThreadLocal} or {@link
   * java.lang.InheritableThreadLocal} values. If necessary,
   * particular values of thread locals can be set or reset before
   * any task runs in {@link java.util.concurrent.ThreadPoolExecutor} subclasses using
   * {@link java.util.concurrent.ThreadPoolExecutor#beforeExecute(Thread, Runnable)}.
   * Also, if it is necessary to initialize worker threads to have
   * the same InheritableThreadLocal settings as some other
   * designated thread, you can create a custom ThreadFactory in
   * which that thread waits for and services requests to create
   * others that will inherit its values.
   *
   * @return a thread factory
   * @throws java.security.AccessControlException if the current access control
   * context does not have permission to both get and set context
   * class loader
   */
  public static ThreadFactory privilegedThreadFactory() {
    return java.util.concurrent.Executors.privilegedThreadFactory();
  }

  /**
   * Returns a {@link Callable} object that, when
   * called, runs the given task and returns the given result.  This
   * can be useful when applying methods requiring a
   * {@code Callable} to an otherwise resultless action.
   * @param task the task to run
   * @param result the result to return
   * @param <T> the type of the result
   * @return a callable object
   * @throws NullPointerException if task null
   */
  public static <T> Callable<T> callable(Runnable task, T result) {
    return java.util.concurrent.Executors.callable(task, result);
  }

  /**
   * Returns a {@link Callable} object that, when
   * called, runs the given task and returns {@code null}.
   * @param task the task to run
   * @return a callable object
   * @throws NullPointerException if task null
   */
  public static Callable<Object> callable(Runnable task) {
    return java.util.concurrent.Executors.callable(task);
  }

  /**
   * Returns a {@link Callable} object that, when
   * called, runs the given privileged action and returns its result.
   * @param action the privileged action to run
   * @return a callable object
   * @throws NullPointerException if action null
   */
  public static Callable<Object> callable(final PrivilegedAction<?> action) {
    return java.util.concurrent.Executors.callable(action);
  }

  /**
   * Returns a {@link Callable} object that, when
   * called, runs the given privileged exception action and returns
   * its result.
   * @param action the privileged exception action to run
   * @return a callable object
   * @throws NullPointerException if action null
   */
  public static Callable<Object> callable(final PrivilegedExceptionAction<?> action) {
    return java.util.concurrent.Executors.callable(action);
  }

  /**
   * Returns a {@link Callable} object that will, when called,
   * execute the given {@code callable} under the current access
   * control context. This method should normally be invoked within
   * an {@link java.security.AccessController#doPrivileged AccessController.doPrivileged}
   * action to create callables that will, if possible, execute
   * under the selected permission settings holding within that
   * action; or if not possible, throw an associated {@link
   * java.security.AccessControlException}.
   * @param callable the underlying task
   * @param <T> the type of the callable's result
   * @return a callable object
   * @throws NullPointerException if callable null
   */
  public static <T> Callable<T> privilegedCallable(Callable<T> callable) {
    return java.util.concurrent.Executors.privilegedCallable(callable);
  }

  /**
   * Returns a {@link Callable} object that will, when called,
   * execute the given {@code callable} under the current access
   * control context, with the current context class loader as the
   * context class loader. This method should normally be invoked
   * within an
   * {@link java.security.AccessController#doPrivileged AccessController.doPrivileged}
   * action to create callables that will, if possible, execute
   * under the selected permission settings holding within that
   * action; or if not possible, throw an associated {@link
   * java.security.AccessControlException}.
   *
   * @param callable the underlying task
   * @param <T> the type of the callable's result
   * @return a callable object
   * @throws NullPointerException if callable null
   * @throws java.security.AccessControlException if the current access control
   * context does not have permission to both set and get context
   * class loader
   */
  public static <T> Callable<T> privilegedCallableUsingCurrentClassLoader(Callable<T> callable) {
    return java.util.concurrent.Executors.privilegedCallableUsingCurrentClassLoader(callable);
  }

  public static ThreadFactory daemonThreadFactory() {
    ThreadFactory defaultFactory = defaultThreadFactory();
    return r -> {
      Thread thread = defaultFactory.newThread(r);
      thread.setDaemon(true);
      thread.setName("FrameTooLongCloser");
      return thread;
    };
  }

  /**
   * A wrapper class that exposes only the ExecutorService methods
   * of an ExecutorService implementation.
   */
  static class DelegatedExecutorService extends AbstractExecutorService implements ExecutorService {
    private final ExecutorService e;

    DelegatedExecutorService(ExecutorService executor) {
      e = executor;
    }

    public void execute(@Nonnull Runnable command) {
      e.execute(command);
    }

    public void shutdown() {
      e.shutdown();
    }

    public @Nonnull List<Runnable> shutdownNow() {
      return e.shutdownNow();
    }

    public boolean isShutdown() {
      return e.isShutdown();
    }

    public boolean isTerminated() {
      return e.isTerminated();
    }

    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
      return e.awaitTermination(timeout, unit);
    }

    public @Nonnull AsyncFuture<?> submit(Runnable task) {
      return e.submit(task);
    }

    public @Nonnull <T> AsyncFuture<T> submit(Callable<T> task) {
      return e.submit(task);
    }

    public @Nonnull <T> AsyncFuture<T> submit(Runnable task, T result) {
      return e.submit(task, result);
    }

    public @Nonnull <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      return e.invokeAll(tasks);
    }

    public @Nonnull <T> List<Future<T>> invokeAll(
        @Nonnull Collection<? extends Callable<T>> tasks,
        long timeout,
        @Nonnull TimeUnit unit) throws InterruptedException {
      return e.invokeAll(tasks, timeout, unit);
    }

    public @Nonnull <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      return e.invokeAny(tasks);
    }

    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return e.invokeAny(tasks, timeout, unit);
    }

    public ExecutorService executorService() {
      return e;
    }
  }

  static class FinalizableDelegatedExecutorService extends DelegatedExecutorService {
    FinalizableDelegatedExecutorService(ExecutorService executor) {
      super(executor);
    }

    protected void finalize() {
      super.shutdown();
    }
  }

  /**
   * A wrapper class that exposes only the ScheduledExecutorService
   * methods of a ScheduledExecutorService implementation.
   */
  static class DelegatedScheduledExecutorService extends DelegatedExecutorService implements ScheduledExecutorService {
    private final ScheduledExecutorService e;

    DelegatedScheduledExecutorService(ScheduledExecutorService executor) {
      super(executor);
      e = executor;
    }

    public @Nonnull ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
      return e.schedule(command, delay, unit);
    }

    public @Nonnull <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
      return e.schedule(callable, delay, unit);
    }

    public @Nonnull ScheduledFuture<?> scheduleAtFixedRate(
        @Nonnull Runnable command,
        long initialDelay,
        long period,
        @Nonnull TimeUnit unit) {
      return e.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public @Nonnull ScheduledFuture<?> scheduleWithFixedDelay(
        @Nonnull Runnable command,
        long initialDelay,
        long delay,
        @Nonnull TimeUnit unit) {
      return e.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
  }
}
