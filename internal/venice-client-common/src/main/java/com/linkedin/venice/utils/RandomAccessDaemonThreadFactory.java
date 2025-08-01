package com.linkedin.venice.utils;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import javax.annotation.Nullable;


public class RandomAccessDaemonThreadFactory extends DaemonThreadFactory {
  /**
   * A map holding all previously created threads which are still active.
   *
   * The threads can be either short-lived or long-lived. In the case of short-lived threads, the map will get cleaned
   * up via {@link TerminationAwareRunnable}, in order to avoid memory leaks.
   */
  private final Map<Integer, Thread> activeThreads = new VeniceConcurrentHashMap<>();

  public RandomAccessDaemonThreadFactory(String threadNamePrefix, LogContext logContext) {
    super(threadNamePrefix, logContext);
  }

  @Override
  public Thread newThread(Runnable r) {
    int threadNumber;
    Thread newThread;
    synchronized (this) {
      /**
       * Synchronization is required to ensure that the thread number does not get re-incremented again by another
       * caller. This is probably fine performance-wise as thread creation should not be on the hot path, but if for
       * some reason it becomes a concern, then a slightly bigger refactoring of {@link DaemonThreadFactory} could be
       * done to make the same logic lock-free...
       */
      threadNumber = super.threadNumber.get();
      TerminationAwareRunnable terminationAwareRunnable =
          new TerminationAwareRunnable(r, () -> activeThreads.remove(threadNumber));
      newThread = super.newThread(terminationAwareRunnable);
    }
    activeThreads.put(threadNumber, newThread);
    return newThread;
  }

  @Nullable
  public Thread getThread(int index) {
    return activeThreads.get(index);
  }

  private static class TerminationAwareRunnable implements Runnable {
    private final Runnable r;
    private final Runnable cleaner;

    TerminationAwareRunnable(Runnable r, Runnable cleaner) {
      this.r = r;
      this.cleaner = cleaner;
    }

    @Override
    public void run() {
      try {
        r.run();
      } finally {
        cleaner.run();
      }
    }
  }
}
