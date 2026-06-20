package com.linkedin.venice.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;


/**
 * A thread factory that sets the threads to run as daemons. (Otherwise things
 * that embed the threadpool can't shut themselves down).
 */
public class DaemonThreadFactory implements ThreadFactory {
  // Sentinel meaning "leave the thread's priority at the JVM default (inherited from the creating thread)".
  public static final int UNSPECIFIED_PRIORITY = -1;
  protected final AtomicInteger threadNumber;
  private final String namePrefix;
  private final LogContext logContext;
  private final int priority;

  public DaemonThreadFactory(String threadNamePrefix, @Nullable LogContext logContext) {
    this(threadNamePrefix, UNSPECIFIED_PRIORITY, logContext);
  }

  public DaemonThreadFactory(String threadNamePrefix) {
    this(threadNamePrefix, null);
  }

  /**
   * @param priority the {@link Thread#setPriority(int) priority} for created threads, or {@link #UNSPECIFIED_PRIORITY}
   *                 to leave it at the JVM default.
   */
  public DaemonThreadFactory(String threadNamePrefix, int priority, @Nullable LogContext logContext) {
    this.threadNumber = new AtomicInteger(0);
    this.namePrefix = threadNamePrefix;
    this.logContext = logContext;
    this.priority = priority;
  }

  @Override
  public Thread newThread(Runnable r) {
    Runnable wrapped = () -> {
      LogContext.setLogContext(logContext);
      try {
        r.run();
      } finally {
        LogContext.clearLogContext();
      }
    };

    Thread t = new Thread(wrapped, namePrefix + "-t" + threadNumber.getAndIncrement());
    t.setDaemon(true);
    if (priority != UNSPECIFIED_PRIORITY) {
      t.setPriority(priority);
    }
    return t;
  }
}
