package com.linkedin.venice.utils.concurrent;

import static com.linkedin.venice.utils.concurrent.BlockingQueueType.ARRAY_BLOCKING_QUEUE;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public final class ThreadPoolFactory {
  private ThreadPoolFactory() {
  }

  public static ThreadPoolExecutor createThreadPool(
      int threadCount,
      String threadNamePrefix,
      int capacity,
      BlockingQueueType blockingQueueType) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threadCount,
        threadCount,
        600,
        TimeUnit.MILLISECONDS,
        getExecutionQueue(capacity, blockingQueueType),
        new DaemonThreadFactory(threadNamePrefix));
    /**
     * When the capacity is fully saturated, the scheduled task will be executed in the caller thread.
     * We will leverage this policy to propagate the back pressure to the caller, so that no more tasks will be
     * scheduled.
     */
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

    return executor;
  }

  private static BlockingQueue<Runnable> getExecutionQueue(int capacity, BlockingQueueType blockingQueueType) {
    switch (blockingQueueType) {
      case LINKED_BLOCKING_QUEUE:
        return new LinkedBlockingQueue<>(capacity);
      case ARRAY_BLOCKING_QUEUE:
        if (capacity == Integer.MAX_VALUE) {
          throw new VeniceException("Queue capacity must be specified when using " + ARRAY_BLOCKING_QUEUE);
        }
        return new ArrayBlockingQueue<>(capacity);
      default:
        throw new VeniceException("Unknown blocking queue type: " + blockingQueueType);
    }
  }
}
