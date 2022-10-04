package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.concurrency.Executors;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.AbstractEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;


public class InstrumentibleEventExecutor extends AbstractEventExecutorGroup {
  private final EventExecutorGroup _eventExecutorGroup;
  private final Map<EventExecutor, InstrumentibleExecutor> _map;

  public InstrumentibleEventExecutor(EventExecutorGroup eventExecutorGroup) {
    _eventExecutorGroup = eventExecutorGroup;
    IdentityHashMap<EventExecutor, InstrumentibleExecutor> map = new IdentityHashMap<>();
    StreamSupport.stream(eventExecutorGroup.spliterator(), false)
        .map(InstrumentibleExecutor::new)
        .forEach(executor -> map.put(executor.unwrap(), executor));
    _map = Collections.unmodifiableMap(map);
  }

  @Override
  public boolean isShuttingDown() {
    return _eventExecutorGroup.isShuttingDown();
  }

  @Override
  public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
    return _eventExecutorGroup.shutdownGracefully(quietPeriod, timeout, unit);
  }

  @Override
  public Future<?> terminationFuture() {
    return _eventExecutorGroup.terminationFuture();
  }

  @Override
  public void shutdown() {
    _eventExecutorGroup.shutdown();
  }

  @Override
  public boolean isShutdown() {
    return _eventExecutorGroup.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _eventExecutorGroup.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
    return _eventExecutorGroup.awaitTermination(timeout, unit);
  }

  @Override
  public EventExecutor next() {
    return _map.get(_eventExecutorGroup.next());
  }

  @Nonnull
  @Override
  public Iterator<EventExecutor> iterator() {
    return _map.values().stream().map(Function.<EventExecutor>identity()).iterator();
  }

  private class InstrumentibleExecutor extends AbstractEventExecutor {
    private final EventExecutor _executor;

    private InstrumentibleExecutor(EventExecutor executor) {
      _executor = executor;
    }

    private EventExecutor unwrap() {
      return _executor;
    }

    @Override
    public boolean isShuttingDown() {
      return _executor.isShuttingDown();
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
      return _executor.shutdownGracefully(quietPeriod, timeout, unit);
    }

    @Override
    public Future<?> terminationFuture() {
      return _executor.terminationFuture();
    }

    @Override
    public void shutdown() {
      _executor.shutdown();
    }

    @Nonnull
    @Override
    public Future<?> submit(Runnable task) {
      Completion completion = newCompletion();
      return completion.submit(_executor.submit(completion.wrap(task)));
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return submit(Executors.callable(task, result));
    }

    @Nonnull
    @Override
    public <T> Future<T> submit(Callable<T> task) {
      Completion completion = newCompletion();
      return completion.submit(_executor.submit(completion.wrap(task)));
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      Completion completion = newCompletion();
      return completion.schedule(_executor.schedule(completion.wrap(command), delay, unit));
    }

    @Nonnull
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      Completion completion = newCompletion();
      return completion.schedule(_executor.schedule(completion.wrap(callable), delay, unit));
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      Completion completion = newCompletion();
      return completion.schedule(_executor.scheduleAtFixedRate(completion.wrap(command), initialDelay, period, unit));
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      Completion completion = newCompletion();
      return completion.schedule(_executor.scheduleWithFixedDelay(completion.wrap(command), initialDelay, delay, unit));
    }

    @Override
    public boolean isShutdown() {
      return _executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return _executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
      return _executor.awaitTermination(timeout, unit);
    }

    @Override
    public boolean inEventLoop(Thread thread) {
      return _executor.inEventLoop(thread);
    }

    @Override
    public void execute(@Nonnull Runnable command) {
      submit(command);
    }
  }

  /**
   * Invoked when the task is scheduled for immediate execution
   * @param completion
   */
  protected void onSubmit(Completion completion) {
  }

  /**
   * Invoked when the task is scheduled for later execution.
   * @param completion
   */
  protected void onSchedule(Completion completion) {
  }

  /**
   * Invoked on the executing thread before execution of the task
   * @param completion
   */
  protected void onExec(Completion completion) {
  }

  /**
   * Invoked after completion of the task
   * @param completion
   * @param isSuccess
   */
  protected void onComplete(Completion completion, boolean isSuccess) {
  }

  protected Completion newCompletion() {
    return new Completion();
  }

  protected class Completion {
    private <F extends Future<V>, V> F submit(F future) {
      onSubmit(this);
      return future;
    }

    private <F extends ScheduledFuture<V>, V> F schedule(F future) {
      onSchedule(this);
      return future;
    }

    private Runnable wrap(Runnable command) {
      return () -> {
        onExec(this);
        boolean success = false;
        try {
          command.run();
          success = true;
        } finally {
          onComplete(this, success);
        }
      };
    }

    private <V> Callable<V> wrap(Callable<V> task) {
      return () -> {
        onExec(this);
        boolean success = false;
        try {
          V result = task.call();
          success = true;
          return result;
        } finally {
          onComplete(this, success);
        }
      };
    }
  }
}
