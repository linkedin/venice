package com.linkedin.venice.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;


/**
 * A thread factory that sets the threads to run as daemons. (Otherwise things
 * that embed the threadpool can't shut themselves down).
 */
public class DaemonThreadFactory implements ThreadFactory {
  protected final AtomicInteger threadNumber;
  private final String namePrefix;
  private final LogContext logContext;

  public DaemonThreadFactory(String threadNamePrefix, @Nullable LogContext logContext) {
    this.threadNumber = new AtomicInteger(0);
    this.namePrefix = threadNamePrefix;
    this.logContext = logContext;
  }

  public DaemonThreadFactory(String threadNamePrefix) {
    this(threadNamePrefix, null);
  }

  @Override
  public Thread newThread(Runnable r) {
    Runnable wrapped = () -> {
      if (logContext != null) {
        LogContext.setLogContext(logContext);
      } else {
        // Clear any inherited context (e.g., from InheritableThreadLocal with
        // log4j2.isThreadContextMapInheritable=true) to avoid misattribution.
        LogContext.clearLogContext();
      }
      try {
        r.run();
      } finally {
        LogContext.clearLogContext();
      }
    };

    Thread t = new Thread(wrapped, namePrefix + "-t" + threadNumber.getAndIncrement());
    t.setDaemon(true);
    return t;
  }
}
