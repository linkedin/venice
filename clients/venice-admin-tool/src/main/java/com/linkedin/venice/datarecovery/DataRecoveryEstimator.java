package com.linkedin.venice.datarecovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * DataRecoveryExecutor is the engine to run tasks in data recovery.
 */
public class DataRecoveryEstimator extends DataRecoveryWorker {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryEstimator.class);
  private Long totalEstimatedTime = 0L;

  public DataRecoveryEstimator() {
    super();
  }

  @Override
  public void processData() {
    for (DataRecoveryTask t: tasks) {
      totalEstimatedTime += ((EstimateDataRecoveryTimeCommand.Result) t.getTaskResult().getCmdResult())
          .getEstimatedRecoveryTimeInSeconds();
    }
  }

  public Long getTotalEstimatedTime() {
    return totalEstimatedTime;
  }

  @Override
  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String storeName: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(storeName, params);
      tasks.add(new DataRecoveryTask(new EstimateDataRecoveryTimeCommand(), taskParams));
    }
    return tasks;
  }

  @Override
  public void displayTaskResult(DataRecoveryTask task) {
    LOGGER.info("[store: {}, result: {}]", task.getTaskParams().getStore(), task.getTaskResult());
  }
}
