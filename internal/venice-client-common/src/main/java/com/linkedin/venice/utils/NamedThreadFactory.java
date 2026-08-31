package com.linkedin.venice.utils;

import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A thread factory that sets the threads to create threads with a specific prefix name.
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final Logger LOGGER = LogManager.getLogger(NamedThreadFactory.class);
  public static final int UNSPECIFIED_PRIORITY = -1;
  private String threadName;
  private final Runnable uncaughtExceptionHook;
  private final int priority;
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

  public NamedThreadFactory(String threadName, int priority) {
    this(threadName, null, priority);
  }

  public NamedThreadFactory(String threadName, Runnable uncaughtExceptionHook) {
    this(threadName, uncaughtExceptionHook, UNSPECIFIED_PRIORITY);
  }

  public NamedThreadFactory(String threadName, Runnable uncaughtExceptionHook, int priority) {
    this.threadName = threadName;
    this.uncaughtExceptionHook = uncaughtExceptionHook;
    this.priority = priority;
    this.threadSequenceNum = 0;
  }

  public Thread newThreadWithName(Runnable runnable, String threadName) {
    this.threadName = threadName;
    return newThread(runnable);
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = new Thread(runnable, this.threadName + "-" + this.threadSequenceNum);
    if (priority != UNSPECIFIED_PRIORITY) {
      thread.setPriority(priority);
    }
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
