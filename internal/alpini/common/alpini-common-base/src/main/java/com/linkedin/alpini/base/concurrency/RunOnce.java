package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import javax.annotation.Nonnull;


/**
 * A simple filter {@link Runnable} which will only invoke the contained runnable no more than once.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public final class RunOnce implements Runnable {
  private static final AtomicReferenceFieldUpdater<RunOnce, Runnable> ATOMIC_TASK =
      AtomicReferenceFieldUpdater.newUpdater(RunOnce.class, Runnable.class, "_task");

  private volatile transient Runnable _task;

  public RunOnce(@Nonnull Runnable task) {
    _task = task;
  }

  public static Runnable make(@Nonnull Runnable task) {
    return new RunOnce(task);
  }

  public static <T> Runnable make(T value, @Nonnull Consumer<T> task) {
    return new RunOnce(() -> task.accept(value));
  }

  @Override
  public void run() {
    Runnable task = _task;
    if (task != null && ATOMIC_TASK.compareAndSet(this, task, null)) {
      task.run();
    }
  }
}
