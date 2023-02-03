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
public class Module {
  private final Logger LOGGER = LogManager.getLogger(Module.class);
  private final static int DEFAULT_POOL_SIZE = 10;
  private final static int DEFAULT_POOL_TIMEOUT_IN_SECONDS = 30;
  private final int poolSize;
  private final ExecutorService pool;
  private List<Task> tasks;

  public Module() {
    this(DEFAULT_POOL_SIZE);
  }

  public Module(int poolSize) {
    this.poolSize = poolSize;
    this.pool = Executors.newFixedThreadPool(this.poolSize);
  }

  public void perform(Set<String> storeNames, StoreRepushCommand.Params params) {
    String pass = getPasswordVIP();
    if (pass == null) {
      LOGGER.error("Cannot get password, exit");
      return;
    }
    params.setPassword(pass);

    tasks = buildTasks(storeNames, params);
    List<CompletableFuture<Void>> taskFutures =
        tasks.stream().map(task -> CompletableFuture.runAsync(task, pool)).collect(Collectors.toList());
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

  public List<Task> buildTasks(Set<String> storeNames, StoreRepushCommand.Params params) {
    List<Task> tasks = new ArrayList<>();
    for (String name: storeNames) {
      Task.TaskParams taskParams = new Task.TaskParams(name, params);
      tasks.add(new Task(taskParams));
    }
    return tasks;
  }

  public String getPasswordVIP() {
    Console console = System.console();
    if (console == null) {
      LOGGER.warn("System.console is null");
      return null;
    }
    // Read password into character array.
    char[] passwordVip = console.readPassword("Enter password + VIP : ");
    return String.copyValueOf(passwordVip);
  }

  private void displayTaskResult() {
    for (Task task: tasks) {
      LOGGER.info(
          "[store: {}, status: {}, message: {}]",
          task.getTaskParams().getStore(),
          task.getTaskResult().isError() ? "failed" : "started",
          task.getTaskResult().isError() ? task.getTaskResult().getError() : task.getTaskResult().getMessage());
    }
  }

  public List<Task> getTasks() {
    return tasks;
  }
}
