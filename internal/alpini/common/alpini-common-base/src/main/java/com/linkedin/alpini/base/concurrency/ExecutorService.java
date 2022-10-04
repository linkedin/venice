package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.Callable;


/**
 * An analog to {@link java.util.concurrent.ExecutorService} except that the futures
 * returned are {@link AsyncFuture}s.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ExecutorService extends java.util.concurrent.ExecutorService {
  /**
   * Submits a value-returning task for execution and returns a
   * Future representing the pending results of the task. The
   * Future's {@code get} method will return the task's result upon
   * successful completion.
   *
   * <p>
   * If you would like to immediately block waiting
   * for a task, you can use constructions of the form
   * {@code result = exec.submit(aCallable).get();}
   *
   * <p>Note: The {@link java.util.concurrent.Executors} class includes a set of methods
   * that can convert some other common closure-like objects,
   * for example, {@link java.security.PrivilegedAction} to
   * {@link Callable} form so they can be submitted.
   *
   * @param task the task to submit
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> AsyncFuture<T> submit(Callable<T> task);

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return the given result upon successful completion.
   *
   * @param task the task to submit
   * @param result the result to return
   * @param <T> the type of the result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  <T> AsyncFuture<T> submit(Runnable task, T result);

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task. The Future's {@code get} method will
   * return {@code null} upon <em>successful</em> completion.
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be
   *         scheduled for execution
   * @throws NullPointerException if the task is null
   */
  AsyncFuture<?> submit(Runnable task);
}
