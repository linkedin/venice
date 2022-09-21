package com.linkedin.venice.utils;

import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A thread factory that sets the threads to create threads with a specific prefix name.
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final Logger LOGGER = LogManager.getLogger(NamedThreadFactory.class);
  private String threadName;
  private final Runnable uncaughtExceptionHook;
  private int threadSequenceNum;

  /**
   * This constructor is used when the thread name is unknown (or do not need to be set at construct time)
   */
  public NamedThreadFactory() {
    this(null);
  }

  public NamedThreadFactory(String threadName) {
    this(threadName, null);
  }

  public NamedThreadFactory(String threadName, Runnable uncaughtExceptionHook) {
    this.threadName = threadName;
    this.uncaughtExceptionHook = uncaughtExceptionHook;
    this.threadSequenceNum = 0;
  }

  public Thread newThreadWithName(Runnable runnable, String threadName) {
    this.threadName = threadName;
    return newThread(runnable);
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = new Thread(runnable, this.threadName + "-" + this.threadSequenceNum);
    thread.setUncaughtExceptionHandler((t, e) -> {
      LOGGER.error("Thread {} throws uncaught exception. ", t.getName(), e);
      if (uncaughtExceptionHook != null) {
        uncaughtExceptionHook.run();
      }
    });
    this.threadSequenceNum++;
    return thread;
  }
}
