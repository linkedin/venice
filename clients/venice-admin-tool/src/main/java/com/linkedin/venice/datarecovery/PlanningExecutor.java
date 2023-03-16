package com.linkedin.venice.datarecovery;

import static java.lang.Thread.*;

import com.linkedin.venice.controllerapi.ControllerClient;
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
public class PlanningExecutor {
  private final Logger LOGGER = LogManager.getLogger(PlanningExecutor.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  private final int poolSize;
  private final ExecutorService pool;
  private List<PlanningTask> tasks;

  public PlanningExecutor() {
    this(DEFAULT_POOL_SIZE);
  }

  public PlanningExecutor(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  public void perform(String clusterName, Set<String> storeNames, ControllerClient controllerClient) {
    tasks = buildTasks(clusterName, storeNames, controllerClient);
    List<CompletableFuture<Void>> taskFutures = tasks.stream()
        .map(dataRecoveryTask -> CompletableFuture.runAsync(dataRecoveryTask, pool))
        .collect(Collectors.toList());
    taskFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    displayTaskResult();

    shutdownAndAwaitTermination();
  }

  private void shutdownAndAwaitTermination() {
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

  public List<PlanningTask> buildTasks(String clusterName, Set<String> storeNames, ControllerClient controllerClient) {
    List<PlanningTask> tasks = new ArrayList<>();
    for (String storeName: storeNames) {
      PlanningTask.TaskParams taskParams = new PlanningTask.TaskParams(clusterName, storeName);
      tasks.add(new PlanningTask(taskParams, controllerClient));
    }
    return tasks;
  }

  private void displayTaskResult() {
    for (PlanningTask task: tasks) {
      Integer estimatedRecoveryTime = task.getEstimatedTimeResult();
      String storeStatus = "started";
      if (estimatedRecoveryTime == -1)
        storeStatus = "failed";
      else if (estimatedRecoveryTime == -2)
        storeStatus = "not started";
      else if (estimatedRecoveryTime > 0)
        storeStatus = "Estimated recovery time: " + estimatedRecoveryTime;
      LOGGER.info("[store: {}, status: {}]", task.getTaskParams().getStoreName(), storeStatus);
    }
  }

  public List<PlanningTask> getTasks() {
    return tasks;
  }
}
