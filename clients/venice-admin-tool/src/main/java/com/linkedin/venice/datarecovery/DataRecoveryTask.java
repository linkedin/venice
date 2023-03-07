package com.linkedin.venice.datarecovery;

/**
 * A Task is wrapper class that designed to execute multiple commands in sequence.
 */
public class DataRecoveryTask implements Runnable {
  private final TaskParams taskParams;
  private final StoreRepushCommand command;
  private TaskResult taskResult;

  public DataRecoveryTask(StoreRepushCommand command, TaskParams params) {
    this.taskParams = params;
    this.command = command;
  }

  @Override
  public void run() {
    command.execute();
    taskResult = new TaskResult(command.getResult());
  }

  public boolean requiresSentinelRun() {
    return true;
  }

  public TaskResult getTaskResult() {
    return taskResult;
  }

  public TaskParams getTaskParams() {
    return taskParams;
  }

  public static class TaskResult {
    private final StoreRepushCommand.Result cmdResult;

    public TaskResult(StoreRepushCommand.Result cmdResult) {
      this.cmdResult = cmdResult;
    }

    public boolean isError() {
      return cmdResult.isError();
    }

    public String getError() {
      return cmdResult.getError();
    }

    public String getMessage() {
      return cmdResult.getMessage();
    }
  }

  public static class TaskParams {
    // Store name.
    private final String store;
    private final StoreRepushCommand.Params cmdParams;

    public TaskParams(String storeName, StoreRepushCommand.Params cmdParams) {
      this.store = storeName;
      this.cmdParams = cmdParams;
    }

    public String getStore() {
      return store;
    }

    public StoreRepushCommand.Params getCmdParams() {
      return this.cmdParams;
    }
  }
}
