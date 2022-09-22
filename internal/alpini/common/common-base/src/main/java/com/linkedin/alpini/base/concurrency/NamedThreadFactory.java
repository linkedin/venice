/*
 * $Id$
 */
package com.linkedin.alpini.base.concurrency;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;


/**
 * ThreadFactory which creates named threads for use in an ExecutorService. Each factory has a pool name
 * and threads in the pool will have the name "poolName-X-thread-Y". If multiple factories are created with the same
 * poolName then each will get its own unique number "X". For example, if two pools were created with the name
 * "workers" then you would see threads like "workers-1-thread-1" and "workers-2-thread-1".
 *
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final ConcurrentHashMap<String, AtomicInteger> INSTANCE_NUMBERS = new ConcurrentHashMap<>();
  private final AtomicInteger _threadNumber = new AtomicInteger(1);
  private final String _namePrefix;
  private final ThreadFactory _factory;

  /**
   * Construct a new NamedThreadFactory with the given pool name.
   *
   * @param poolName the name to be used as part of the prefix for this factory.
   */
  public NamedThreadFactory(String poolName) {
    this(Executors.defaultThreadFactory(), poolName);
  }

  /**
   * Construct a new NamedThreadFactory with the given thread group and pool name.
   *
   * @param threadGroup the thread group which the new threads belong.
   * @param poolName the name to be used as part of the prefix for this factory.
   */
  public NamedThreadFactory(ThreadGroup threadGroup, String poolName) {
    this(r -> new Thread(threadGroup, r), poolName);
    Objects.requireNonNull(threadGroup, "threadGroup");
  }

  public NamedThreadFactory(ThreadFactory threadFactory, String poolName) {
    _factory = Objects.requireNonNull(threadFactory, "threadFactory");

    AtomicInteger instanceNumber = INSTANCE_NUMBERS.get(Objects.requireNonNull(poolName, "poolName"));
    if (instanceNumber == null) {
      INSTANCE_NUMBERS.putIfAbsent(poolName, new AtomicInteger(1));
      instanceNumber = INSTANCE_NUMBERS.get(poolName);
    }
    _namePrefix = poolName + "-" + instanceNumber.getAndIncrement() + "-thread";
  }

  /**
   * Create a new Thread for the specified {@link Runnable}.
   *
   * @param runnable the {@link Runnable} to be executed by the new thread.
   * @return a new {@link Thread} instance
   */
  @Override
  public Thread newThread(@Nonnull Runnable runnable) {
    Thread thread = _factory.newThread(Objects.requireNonNull(runnable));
    thread.setName(_namePrefix + "-" + _threadNumber.getAndIncrement());
    return init(thread);
  }

  protected Thread init(Thread t) {
    if (t.isDaemon()) {
      t.setDaemon(false);
    }
    if (t.getPriority() != Thread.NORM_PRIORITY) {
      t.setPriority(Thread.NORM_PRIORITY);
    }
    return t;
  }
}
