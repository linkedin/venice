package com.linkedin.venice.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.logging.log4j.ThreadContext;


/**
 * A thread factory that sets the threads to run as daemons. (Otherwise things
 * that embed the threadpool can't shut themselves down).
 */
public class DaemonThreadFactory implements ThreadFactory {
  protected final AtomicInteger threadNumber;
  private final String namePrefix;
  private final Object logContext;

  public DaemonThreadFactory(String threadNamePrefix, @Nullable Object logContext) {
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
      LogContext.setLogContext(logContext);
      try {
        r.run();
      } finally {
        ThreadContext.clearAll(); // prevent context leakage across tasks
      }
    };

    Thread t = new Thread(wrapped, namePrefix + "-t" + threadNumber.getAndIncrement());
    t.setDaemon(true);
    return t;
  }
}
