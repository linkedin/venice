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
    EstimateDataRecoveryTimeCommand.Params.Builder builder =
        new EstimateDataRecoveryTimeCommand.Params.Builder((EstimateDataRecoveryTimeCommand.Params) params);
    for (String storeName: storeNames) {
      EstimateDataRecoveryTimeCommand.Params p = builder.build();
      p.setStore(storeName);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(storeName, p);
      tasks.add(new DataRecoveryTask(new EstimateDataRecoveryTimeCommand(p), taskParams));
    }
    return tasks;
  }

  @Override
  public void displayTaskResult(DataRecoveryTask task) {
    EstimateDataRecoveryTimeCommand.Result result =
        (EstimateDataRecoveryTimeCommand.Result) task.getTaskResult().getCmdResult();
    if (result.isError()) {
      LOGGER.info("[store: {}, error: {}]", task.getTaskParams().getStore(), result.getError());
    } else {
      LOGGER.info("[store: {}, recovery time estimate: {}]", task.getTaskParams().getStore(), result.getTimestamp());
    }
  }
}
