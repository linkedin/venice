package com.linkedin.venice.utils;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Process-wide executor for handing user-visible completions off Venice worker and PubSub callback threads.
 *
 * <p>The pool expands like a cached pool because arbitrary user continuations may block. A fixed or bounded pool could
 * otherwise strand unrelated producer completions behind blocked continuations. All idle workers expire after one
 * minute.</p>
 */
public final class VeniceCompletionExecutor {
  private static final DaemonThreadFactory THREAD_FACTORY = new DaemonThreadFactory("venice-completion-handoff");
  private static final ThreadPoolExecutor EXECUTOR = createExecutor();

  private VeniceCompletionExecutor() {
  }

  public static void execute(Runnable completion) {
    Objects.requireNonNull(completion);
    try {
      EXECUTOR.execute(completion);
    } catch (RejectedExecutionException unexpectedRejection) {
      // This executor is never shut down and has no bounded queue. Preserve off-thread progress if it still rejects.
      THREAD_FACTORY.newThread(completion).start();
    }
  }

  private static ThreadPoolExecutor createExecutor() {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<>(), THREAD_FACTORY);
    return executor;
  }
}
