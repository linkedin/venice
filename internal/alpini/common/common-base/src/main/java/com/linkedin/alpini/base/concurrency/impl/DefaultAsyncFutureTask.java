package com.linkedin.alpini.base.concurrency.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.function.BooleanSupplier;


/**
 * Wraps a {@link Callable} in a {@link FutureTask} and make it notify a {@link DefaultAsyncFuture}.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class DefaultAsyncFutureTask<T> extends DefaultAsyncFuture<T> implements RunnableFuture<T> {
  private final Future<?> _future;
  private BooleanSupplier _runAndReset;

  /**
   * Wrap a callable in an AsyncFuture. This constructor does not execute the callable so
   * {@linkplain #run()} will need to be executed.
   *
   * @param callable
   * @param cancellable
   */
  public DefaultAsyncFutureTask(Callable<T> callable, boolean cancellable) {
    super(cancellable);

    _future = newFutureTask(callable);
  }

  /**
   * Wrap a callable in an AsyncFuture and then submit it to be executed on the provided ExecutorService.
   *
   * @param executor
   * @param callable
   * @param cancellable
   */
  public DefaultAsyncFutureTask(ExecutorService executor, Callable<T> callable, boolean cancellable) {
    super(cancellable);
    _future = executor.submit(newFutureTask(callable));
  }

  public DefaultAsyncFutureTask(Executor executor, Callable<T> callable, boolean cancellable) {
    super(cancellable);
    if (executor instanceof ExecutorService) {
      _future = ((ExecutorService) executor).submit(newFutureTask(callable));
    } else {
      _future = newFutureTask(callable);
      executor.execute(this);
    }
  }

  protected boolean runAndReset() {
    return _runAndReset.getAsBoolean();
  }

  private FutureTask<T> newFutureTask(Callable<T> callable) {
    return new FutureTask<T>(callable) {
      {
        _runAndReset = this::runAndReset;
      }

      @Override
      protected void done() {
        if (isCancelled()) {
          setFailure(new CancellationException());
        } else {
          try {
            setSuccess(get());
          } catch (InterruptedException e) {
            setFailure(e);
          } catch (ExecutionException e) {
            setFailure(e.getCause());
          }
        }
      }
    };
  }

  /**
   * Run the Callable. If the Callable has already been executed or submitted to an Executor, this may cause
   * an IllegalStateException to be thrown.
   */
  @Override
  public void run() {
    if (_future instanceof Runnable) {
      ((Runnable) _future).run();
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * @see Future#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return super.cancel(mayInterruptIfRunning) && _future.cancel(mayInterruptIfRunning);
  }
}
