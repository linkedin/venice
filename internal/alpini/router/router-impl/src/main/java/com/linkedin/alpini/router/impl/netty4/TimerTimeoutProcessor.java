package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


public class TimerTimeoutProcessor implements RouterTimeoutProcessor {
  private final Timer _timer;

  public TimerTimeoutProcessor(@Nonnull Timer timer) {
    _timer = timer;
  }

  @Nonnull
  @Override
  public TimeoutFuture schedule(@Nonnull Runnable task, @Nonnegative long scheduleDelay, @Nonnull TimeUnit unit) {
    return new TimeoutTask(task, _timer, scheduleDelay, unit);
  }

  @Nonnull
  @Override
  public <T> T unwrap(@Nonnull Class<T> clazz) {
    return clazz.cast(_timer);
  }

  @Override
  public void execute(@Nonnull Runnable command) {
    schedule(command, 0, TimeUnit.NANOSECONDS);
  }

  private static class TimeoutTask implements TimeoutFuture, TimerTask {
    private final Runnable _task;
    private final Timeout _timeout;

    private TimeoutTask(Runnable task, Timer timer, long scheduleDelay, @Nonnull TimeUnit unit) {
      _task = task;
      _timeout = timer.newTimeout(this, scheduleDelay, unit);
    }

    @Override
    public boolean cancel() {
      return _timeout.cancel();
    }

    @Override
    public void run(Timeout timeout) throws Exception {
      if (timeout.isExpired()) {
        _task.run();
      }
    }
  }
}
