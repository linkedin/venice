package com.linkedin.venice.datarecovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DataRecoveryExecutor is the engine to run tasks in data recovery.
 */
public class DataRecoveryExecutor extends DataRecoveryWorker {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryExecutor.class);

  public DataRecoveryExecutor() {
    super();
  }

  @Override
  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(
          new DataRecoveryTask(
              new StoreRepushCommand((StoreRepushCommand.Params) taskParams.getCmdParams()),
              taskParams));
    }
    return tasks;
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
