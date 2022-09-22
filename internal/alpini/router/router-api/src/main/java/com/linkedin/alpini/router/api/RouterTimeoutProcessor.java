package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


public interface RouterTimeoutProcessor extends Executor {
  @Nonnull
  TimeoutFuture schedule(@Nonnull Runnable task, @Nonnegative long scheduleDelay, @Nonnull TimeUnit unit);

  @Nonnull
  <T> T unwrap(@Nonnull Class<T> clazz);

  interface TimeoutFuture {
    boolean cancel();
  }

  static RouterTimeoutProcessor adapt(final TimeoutProcessor timeoutProcessor) {
    return timeoutProcessor != null ? new RouterTimeoutProcessor() {
      @Nonnull
      @Override
      public TimeoutFuture schedule(@Nonnull Runnable task, long scheduleDelay, @Nonnull TimeUnit unit) {
        return new TimeoutFuture() {
          final TimeoutProcessor.TimeoutFuture _future = timeoutProcessor.schedule(task, scheduleDelay, unit);

          @Override
          public boolean cancel() {
            return _future.cancel();
          }
        };
      }

      @Override
      public void execute(@Nonnull Runnable task) {
        timeoutProcessor.execute(task);
      }

      @Nonnull
      @Override
      public <T> T unwrap(@Nonnull Class<T> clazz) {
        return clazz.cast(timeoutProcessor);
      }
    } : null;
  }
}
