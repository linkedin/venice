package com.linkedin.venice.datarecovery;

import static java.lang.Thread.currentThread;

import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataRecoveryWorker {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryWorker.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  public static final int INTERVAL_UNSET = -1;
  protected final int poolSize;
  protected int interval = INTERVAL_UNSET;
  protected final ExecutorService pool;
  protected List<DataRecoveryTask> tasks;

  public DataRecoveryWorker() {
    this(DEFAULT_POOL_SIZE);
  }

  public DataRecoveryWorker(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  abstract List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command.Params params);

  abstract void displayTaskResult(DataRecoveryTask task);

  public List<DataRecoveryTask> getTasks() {
    return tasks;
  }

  /**
   * For some task, it is benefit to wait for the first task to complete before starting to run the remaining ones.
   * e.g. the first run of task can set up local session files that can be used by follow-up tasks.
   */
  public boolean needWaitForFirstTaskToComplete(DataRecoveryTask task) {
    return task.needWaitForFirstTaskToComplete();
  }

  public void perform(Set<String> storeNames, Command.Params params) {
    tasks = buildTasks(storeNames, params);
    if (tasks.isEmpty()) {
      return;
    }

    List<DataRecoveryTask> concurrentTasks = tasks;
    DataRecoveryTask firstTask = tasks.get(0);
    if (needWaitForFirstTaskToComplete(firstTask)) {
      // Let the main thread run the first task to completion if there is a need.
      firstTask.run();
      if (firstTask.getTaskResult().isError()) {
        displayTaskResult(firstTask);
        return;
      }
      // Exclude the 1st item from the list as it has finished.
      concurrentTasks = tasks.subList(1, tasks.size());
    }

    /*
     * Keep polling the states (for monitor) of all tasks at given intervals when interval is set to certain value
     * plus not all tasks are finished. Otherwise, if interval is unset, just do a one time execution for all tasks.
     */
    do {
      try (Timer ignored = Timer.run(elapsedTimeInMs -> {
        if (continuePollingState()) {
          Utils.sleep(computeTimeToSleepInMillis(elapsedTimeInMs));
        }
      })) {
        List<CompletableFuture<Void>> taskFutures = concurrentTasks.stream()
            .map(dataRecoveryTask -> CompletableFuture.runAsync(dataRecoveryTask, pool))
            .collect(Collectors.toList());
        taskFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());

        processData();
        displayAllTasksResult();
      }
    } while (continuePollingState());
  }

  public void processData() {
  }

  private void displayAllTasksResult() {
    int numDoneTasks = 0;
    int numSuccessfullyDoneTasks = 0;

    for (DataRecoveryTask dataRecoveryTask: tasks) {
      displayTaskResult(dataRecoveryTask);
      if (dataRecoveryTask.getTaskResult().isCoreWorkDone()) {
        numDoneTasks++;
        if (!dataRecoveryTask.getTaskResult().isError()) {
          numSuccessfullyDoneTasks++;
        }
      }
    }
    LOGGER.info(
        "Total: {}, Succeeded: {}, Error: {}, Uncompleted: {}",
        tasks.size(),
        numSuccessfullyDoneTasks,
        numDoneTasks - numSuccessfullyDoneTasks,
        tasks.size() - numDoneTasks);
  }

  private boolean continuePollingState() {
    return isIntervalSet() && !areAllCoreWorkDone();
  }

  private boolean isIntervalSet() {
    return interval != INTERVAL_UNSET;
  }

  private boolean areAllCoreWorkDone() {
    for (DataRecoveryTask task: tasks) {
      if (!task.getTaskResult().isCoreWorkDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Calculate the sleep time based on the interval setting and the latency that has already occurred.
   */
  private long computeTimeToSleepInMillis(double latency) {
    long sleepTime = TimeUnit.SECONDS.toMillis(interval) - (long) latency;
    return sleepTime > 0 ? sleepTime : 0;
  }

  public void shutdownAndAwaitTermination() {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(DEFAULT_POOL_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
        // Cancel currently executing tasks.
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }
}
