package com.linkedin.venice.controller.multitaskscheduler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;


/**
 * Abstract class for managing scheduled tasks.
 * Other classes can extend this class to implement specific task management logic,
 * and be loaded into MultiTaskSchedulerService to get it running.
 * This class provides a framework for creating and managing a scheduled executor service.
 * It also provides methods for scheduling tasks and shutting down the executor service.
 */
public abstract class ScheduledTaskManager {
  protected final ScheduledExecutorService executorService;

  public ScheduledTaskManager(int threadPoolSize) {
    this.executorService = createExecutorService(threadPoolSize);
  }

  protected abstract ScheduledExecutorService createExecutorService(int threadPoolSize);

  protected abstract Logger getLogger();

  public void shutdown() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        getLogger().warn(
            "{} executor did not terminate within the specified timeout. Initiating forceful shutdown...",
            getClass().getSimpleName());
        executorService.shutdownNow();
        getLogger().info("{} executor shutdown timed out and was forcefully shut down", getClass().getSimpleName());
      } else {
        getLogger().info("{} executor shutdown completed successfully", getClass().getSimpleName());
      }
    } catch (InterruptedException e) {
      getLogger().error(
          "{} shutdown process was interrupted. Forcefully shutting down the executor...",
          getClass().getSimpleName(),
          e);
      executorService.shutdownNow();
    }
    getLogger().info("{} executor has been shut down", getClass().getSimpleName());
  }

  public ScheduledFuture<?> scheduleNextStep(Runnable task, int delayInSeconds) {
    return executorService.schedule(task, delayInSeconds, TimeUnit.SECONDS);
  }
}
