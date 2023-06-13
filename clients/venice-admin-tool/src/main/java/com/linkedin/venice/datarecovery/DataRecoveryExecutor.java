package com.linkedin.venice.datarecovery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DataRecoveryExecutor is the engine to run tasks in data recovery.
 */
public class DataRecoveryExecutor extends DataRecoveryWorker {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryExecutor.class);
  private Set<String> skippedStores;

  public DataRecoveryExecutor() {
    super();
    this.skippedStores = new HashSet<>();
  }

  @Override
  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    StoreRepushCommand.Params.Builder builder =
        new StoreRepushCommand.Params.Builder((StoreRepushCommand.Params) params);
    for (String name: storeNames) {
      StoreRepushCommand.Params p = builder.build();
      p.setStore(name);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
      tasks.add(new DataRecoveryTask(new StoreRepushCommand(p), taskParams));
    }
    return tasks;
  }

  // for testing

  public Set<String> getSkippedStores() {
    return skippedStores;
  }

  @Override
  public void displayTaskResult(DataRecoveryTask task) {
    LOGGER.info(
        "[store: {}, status: {}, message: {}]",
        task.getTaskParams().getStore(),
        task.getTaskResult().isError() ? "failed" : "started",
        task.getTaskResult().isError() ? task.getTaskResult().getError() : task.getTaskResult().getMessage());
  }
}
