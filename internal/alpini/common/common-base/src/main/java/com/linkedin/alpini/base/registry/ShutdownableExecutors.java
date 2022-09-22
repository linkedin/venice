package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.registry.impl.ShutdownableExecutorServiceImpl;
import com.linkedin.alpini.base.registry.impl.ShutdownableScheduledExecutorServiceImpl;
import java.util.concurrent.ThreadFactory;


/**
 * An Executor factory to be used with {@link ResourceRegistry} instead of using {@link Executors}
 *
 * <br>Example:
 * <pre>
 *   ExecutorService executor = resourceRegistry.factory(ShutdownableExecutors.class).newFixedThreadPool(5);
 * </pre>
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public interface ShutdownableExecutors extends ResourceRegistry.Factory<ShutdownableExecutorService> {
  /**
   * Creates a thread pool that reuses a fixed number of threads
   * operating off a shared unbounded queue.  At any point, at most
   * <tt>nThreads</tt> threads will be active processing tasks.
   * If additional tasks are submitted when all threads are active,
   * they will wait in the queue until a thread is available.
   * If any thread terminates due to a failure during execution
   * prior to shutdown, a new one will take its place if needed to
   * execute subsequent tasks.  The threads in the pool will exist
   * until it is explicitly {@link ShutdownableExecutorService#shutdown shutdown}.
   *
   * @param nThreads the number of threads in the pool
   * @return the newly created thread pool
   * @throws IllegalArgumentException if <tt>nThreads &lt;= 0</tt>
   */
  ShutdownableExecutorService newFixedThreadPool(int nThreads);

  /**
   * Creates a thread pool that reuses a fixed number of threads
   * operating off a shared unbounded queue, using the provided
   * ThreadFactory to create new threads when needed.  At any point,
   * at most <tt>nThreads</tt> threads will be active processing
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
   * @throws IllegalArgumentException if <tt>nThreads &lt;= 0</tt>
   */
  ShutdownableExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory);

  /**
   * Creates an Executor that uses a single worker thread operating
   * off an unbounded queue. (Note however that if this single
   * thread terminates due to a failure during execution prior to
   * shutdown, a new one will take its place if needed to execute
   * subsequent tasks.)  Tasks are guaranteed to execute
   * sequentially, and no more than one task will be active at any
   * given time. Unlike the otherwise equivalent
   * <tt>newFixedThreadPool(1)</tt> the returned executor is
   * guaranteed not to be reconfigurable to use additional threads.
   *
   * @return the newly created single-threaded Executor
   */
  ShutdownableExecutorService newSingleThreadExecutor();

  /**
   * Creates an Executor that uses a single worker thread operating
   * off an unbounded queue, and uses the provided ThreadFactory to
   * create a new thread when needed. Unlike the otherwise
   * equivalent <tt>newFixedThreadPool(1, threadFactory)</tt> the
   * returned executor is guaranteed not to be reconfigurable to use
   * additional threads.
   *
   * @param threadFactory the factory to use when creating new
   * threads
   *
   * @return the newly created single-threaded Executor
   * @throws NullPointerException if threadFactory is null
   */
  ShutdownableExecutorService newSingleThreadExecutor(ThreadFactory threadFactory);

  /**
   * Creates a thread pool that creates new threads as needed, but
   * will reuse previously constructed threads when they are
   * available.  These pools will typically improve the performance
   * of programs that execute many short-lived asynchronous tasks.
   * Calls to <tt>execute</tt> will reuse previously constructed
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
  ShutdownableExecutorService newCachedThreadPool();

  /**
   * Creates a thread pool that creates new threads as needed, but
   * will reuse previously constructed threads when they are
   * available, and uses the provided
   * ThreadFactory to create new threads when needed.
   * @param threadFactory the factory to use when creating new threads
   * @return the newly created thread pool
   * @throws NullPointerException if threadFactory is null
   */
  ShutdownableExecutorService newCachedThreadPool(ThreadFactory threadFactory);

  /**
   * Creates a single-threaded executor that can schedule commands
   * to run after a given delay, or to execute periodically.
   * (Note however that if this single
   * thread terminates due to a failure during execution prior to
   * shutdown, a new one will take its place if needed to execute
   * subsequent tasks.)  Tasks are guaranteed to execute
   * sequentially, and no more than one task will be active at any
   * given time. Unlike the otherwise equivalent
   * <tt>newScheduledThreadPool(1)</tt> the returned executor is
   * guaranteed not to be reconfigurable to use additional threads.
   * @return the newly created scheduled executor
   */
  ShutdownableScheduledExecutorService newSingleThreadScheduledExecutor();

  /**
   * Creates a single-threaded executor that can schedule commands
   * to run after a given delay, or to execute periodically.  (Note
   * however that if this single thread terminates due to a failure
   * during execution prior to shutdown, a new one will take its
   * place if needed to execute subsequent tasks.)  Tasks are
   * guaranteed to execute sequentially, and no more than one task
   * will be active at any given time. Unlike the otherwise
   * equivalent <tt>newScheduledThreadPool(1, threadFactory)</tt>
   * the returned executor is guaranteed not to be reconfigurable to
   * use additional threads.
   * @param threadFactory the factory to use when creating new
   * threads
   * @return a newly created scheduled executor
   * @throws NullPointerException if threadFactory is null
   */
  ShutdownableScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory);

  /**
   * Creates a thread pool that can schedule commands to run after a
   * given delay, or to execute periodically.
   * @param corePoolSize the number of threads to keep in the pool,
   * even if they are idle.
   * @return a newly created scheduled thread pool
   * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
   */
  ShutdownableScheduledExecutorService newScheduledThreadPool(int corePoolSize);

  /**
   * Creates a thread pool that can schedule commands to run after a
   * given delay, or to execute periodically.
   * @param corePoolSize the number of threads to keep in the pool,
   * even if they are idle.
   * @param threadFactory the factory to use when the executor
   * creates a new thread.
   * @return a newly created scheduled thread pool
   * @throws IllegalArgumentException if <tt>corePoolSize &lt; 0</tt>
   * @throws NullPointerException if threadFactory is null
   */
  ShutdownableScheduledExecutorService newScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory);

  static final ResourceRegistry.Factory<ShutdownableExecutorService> SERVICE_FACTORY =
      ResourceRegistry.registerFactory(ShutdownableExecutors.class, new ShutdownableExecutors() {
        @Override
        public ShutdownableExecutorService newFixedThreadPool(int nThreads) {
          return wrap(Executors.newFixedThreadPool(nThreads));
        }

        @Override
        public ShutdownableExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
          return wrap(Executors.newFixedThreadPool(nThreads, threadFactory));
        }

        @Override
        public ShutdownableExecutorService newSingleThreadExecutor() {
          return wrap(Executors.newSingleThreadExecutor());
        }

        @Override
        public ShutdownableExecutorService newSingleThreadExecutor(ThreadFactory threadFactory) {
          return wrap(Executors.newSingleThreadExecutor(threadFactory));
        }

        @Override
        public ShutdownableExecutorService newCachedThreadPool() {
          return wrap(Executors.newCachedThreadPool());
        }

        @Override
        public ShutdownableExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
          return wrap(Executors.newCachedThreadPool(threadFactory));
        }

        @Override
        public ShutdownableScheduledExecutorService newSingleThreadScheduledExecutor() {
          return wrap(Executors.newSingleThreadScheduledExecutor());
        }

        @Override
        public ShutdownableScheduledExecutorService newSingleThreadScheduledExecutor(ThreadFactory threadFactory) {
          return wrap(Executors.newSingleThreadScheduledExecutor(threadFactory));
        }

        @Override
        public ShutdownableScheduledExecutorService newScheduledThreadPool(int corePoolSize) {
          return wrap(Executors.newScheduledThreadPool(corePoolSize));
        }

        @Override
        public ShutdownableScheduledExecutorService newScheduledThreadPool(
            int corePoolSize,
            ThreadFactory threadFactory) {
          return wrap(Executors.newScheduledThreadPool(corePoolSize, threadFactory));
        }

        private ShutdownableExecutorService wrap(ExecutorService executorService) {
          return new ShutdownableExecutorServiceImpl<>(executorService);
        }

        private ShutdownableScheduledExecutorService wrap(ScheduledExecutorService executorService) {
          return new ShutdownableScheduledExecutorServiceImpl<>(executorService);
        }
      });
}
