package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.misc.ScheduledFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;


/**
 * An analog to {@link java.util.concurrent.ScheduledExecutorService} which extends {@link ExecutorService}.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ScheduledExecutorService extends java.util.concurrent.ScheduledExecutorService, ExecutorService {
  /**
   * Creates and executes a one-shot action that becomes enabled
   * after the given delay.
   *
   * @param command the task to execute
   * @param delay   the time from now to delay execution
   * @param unit    the time unit of the delay parameter
   * @return a ScheduledFuture representing pending completion of
   * the task and whose {@code get()} method will return
   * {@code null} upon completion
   * @throws RejectedExecutionException if the task cannot be
   *                                    scheduled for execution
   * @throws NullPointerException       if command is null
   */
  @Nonnull
  ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit);

  /**
   * Creates and executes a ScheduledFuture that becomes enabled after the
   * given delay.
   *
   * @param callable the function to execute
   * @param delay    the time from now to delay execution
   * @param unit     the time unit of the delay parameter
   * @param <V>      the type of the callable's result
   * @return a ScheduledFuture that can be used to extract result or cancel
   * @throws RejectedExecutionException if the task cannot be
   *                                    scheduled for execution
   * @throws NullPointerException       if callable is null
   */
  @Nonnull
  <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit);

  /**
   * Creates and executes a periodic action that becomes enabled first
   * after the given initial delay, and subsequently with the given
   * period; that is executions will commence after
   * {@code initialDelay} then {@code initialDelay+period}, then
   * {@code initialDelay + 2 * period}, and so on.
   * If any execution of the task
   * encounters an exception, subsequent executions are suppressed.
   * Otherwise, the task will only terminate via cancellation or
   * termination of the executor.  If any execution of this task
   * takes longer than its period, then subsequent executions
   * may start late, but will not concurrently execute.
   *
   * @param command      the task to execute
   * @param initialDelay the time to delay first execution
   * @param period       the period between successive executions
   * @param unit         the time unit of the initialDelay and period parameters
   * @return a ScheduledFuture representing pending completion of
   * the task, and whose {@code get()} method will throw an
   * exception upon cancellation
   * @throws RejectedExecutionException if the task cannot be
   *                                    scheduled for execution
   * @throws NullPointerException       if command is null
   * @throws IllegalArgumentException   if period less than or equal to zero
   */
  @Nonnull
  ScheduledFuture<?> scheduleAtFixedRate(
      @Nonnull Runnable command,
      long initialDelay,
      long period,
      @Nonnull TimeUnit unit);

  /**
   * Creates and executes a periodic action that becomes enabled first
   * after the given initial delay, and subsequently with the
   * given delay between the termination of one execution and the
   * commencement of the next.  If any execution of the task
   * encounters an exception, subsequent executions are suppressed.
   * Otherwise, the task will only terminate via cancellation or
   * termination of the executor.
   *
   * @param command      the task to execute
   * @param initialDelay the time to delay first execution
   * @param delay        the delay between the termination of one
   *                     execution and the commencement of the next
   * @param unit         the time unit of the initialDelay and delay parameters
   * @return a ScheduledFuture representing pending completion of
   * the task, and whose {@code get()} method will throw an
   * exception upon cancellation
   * @throws RejectedExecutionException if the task cannot be
   *                                    scheduled for execution
   * @throws NullPointerException       if command is null
   * @throws IllegalArgumentException   if delay less than or equal to zero
   */
  @Nonnull
  ScheduledFuture<?> scheduleWithFixedDelay(
      @Nonnull Runnable command,
      long initialDelay,
      long delay,
      @Nonnull TimeUnit unit);
}
