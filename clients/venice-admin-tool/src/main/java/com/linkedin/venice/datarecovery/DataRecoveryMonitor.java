package com.linkedin.venice.datarecovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DataRecoveryMonitor extends DataRecoveryWorker {
  private final Logger LOGGER = LogManager.getLogger(DataRecoveryMonitor.class);

  public DataRecoveryMonitor() {
    super();
  }

  public void setInterval(int interval) {
    this.interval = interval;
  }

  @Override
  public List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    MonitorCommand.Params.Builder builder = new MonitorCommand.Params.Builder((MonitorCommand.Params) params);
    for (String name: storeNames) {
      MonitorCommand.Params p = builder.build();
      p.setStore(name);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
      tasks.add(new DataRecoveryTask(new MonitorCommand(p), taskParams));
    }
    return tasks;
  }

  @Override
  public void displayTaskResult(DataRecoveryTask task) {
    LOGGER.info(
        "[store: {}, {}: {}]",
        task.getTaskParams().getStore(),
        task.getTaskResult().isError() ? "err" : "msg",
        task.getTaskResult().isError() ? task.getTaskResult().getError() : task.getTaskResult().getMessage());
  }
}
