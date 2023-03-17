package com.linkedin.venice.datarecovery;

import static java.lang.Thread.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DataRecoveryExecutor is the engine to run tasks in data recovery.
 */
public class DataRecoveryExecutor {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryExecutor.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  private final int poolSize;
  private final ExecutorService pool;
  private List<DataRecoveryTask> tasks;

  public DataRecoveryExecutor() {
    this(DEFAULT_POOL_SIZE);
  }

  public DataRecoveryExecutor(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  /**
   * For some task, it is benefit to wait for the first task to complete before starting to run the remaining ones.
   * e.g. the first run of task can set up local session files that can be used by follow-up tasks.
   */
  public boolean needWaitForFirstTaskToComplete(DataRecoveryTask task) {
    return task.needWaitForFirstTaskToComplete();
  }

  public void perform(Set<String> storeNames, StoreRepushCommand.Params params) {
    tasks = buildTasks(storeNames, params);
    if (tasks.isEmpty()) {
      return;
    }

    List<DataRecoveryTask> concurrentTasks = tasks;
    DataRecoveryTask firstTask = tasks.get(0);
    if (needWaitForFirstTaskToComplete(firstTask)) {
      // Let the main thread run the first task to completion if there is a need.
      DataRecoveryTask sentinel = firstTask;
      sentinel.run();
      if (sentinel.getTaskResult().isError()) {
        displayTaskResult(sentinel);
        return;
      }
      // Exclude the 1st item from the list as it has finished.
      concurrentTasks = tasks.subList(1, tasks.size());
    }

    List<CompletableFuture<Void>> taskFutures = concurrentTasks.stream()
        .map(dataRecoveryTask -> CompletableFuture.runAsync(dataRecoveryTask, pool))
        .collect(Collectors.toList());
    taskFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    displayAllTasksResult();
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

  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, StoreRepushCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(
          new DataRecoveryTask(new StoreRepushCommand(taskParams.getStore(), taskParams.getCmdParams()), taskParams));
    }
    return tasks;
  }

  private void displayAllTasksResult() {
    for (DataRecoveryTask dataRecoveryTask: tasks) {
      displayTaskResult(dataRecoveryTask);
    }
  }

  private void displayTaskResult(DataRecoveryTask task) {
    LOGGER.info(
        "[store: {}, status: {}, message: {}]",
        task.getTaskParams().getStore(),
        task.getTaskResult().isError() ? "failed" : "started",
        task.getTaskResult().isError() ? task.getTaskResult().getError() : task.getTaskResult().getMessage());
  }

  public List<DataRecoveryTask> getTasks() {
    return tasks;
  }
}
