package com.linkedin.venice.datarecovery;

import java.io.Console;
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
 * Module is the engine to run Tasks in data recovery.
 */
public class DataRecoveryModule {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryModule.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  private final int poolSize;
  private final ExecutorService pool;
  private List<DataRecoveryTask> tasks;

  public DataRecoveryModule() {
    this(DEFAULT_POOL_SIZE);
  }

  public DataRecoveryModule(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  public void perform(Set<String> storeNames, StoreRepushCommand.Params params) {
    String pass = getUserCredentials();
    if (pass == null) {
      LOGGER.error("Cannot get password, exit");
      return;
    }
    params.setPassword(pass);

    tasks = buildTasks(storeNames, params);
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
      // (Re-)Cancel if current thread also interrupted.
      pool.shutdownNow();
    }
  }

  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, StoreRepushCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(new DataRecoveryTask(taskParams));
    }
    return tasks;
  }

  public String getUserCredentials() {
    Console console = System.console();
    if (console == null) {
      LOGGER.warn("System.console is null");
      return null;
    }
    // Read password into character array.
    char[] passwordVip = console.readPassword("Enter Credentials: ");
    return String.copyValueOf(passwordVip);
  }

  private void displayTaskResult() {
    for (DataRecoveryTask dataRecoveryTask: tasks) {
      LOGGER.info(
          "[store: {}, status: {}, message: {}]",
          dataRecoveryTask.getTaskParams().getStore(),
          dataRecoveryTask.getTaskResult().isError() ? "failed" : "started",
          dataRecoveryTask.getTaskResult().isError()
              ? dataRecoveryTask.getTaskResult().getError()
              : dataRecoveryTask.getTaskResult().getMessage());
    }
  }

  public List<DataRecoveryTask> getTasks() {
    return tasks;
  }
}
