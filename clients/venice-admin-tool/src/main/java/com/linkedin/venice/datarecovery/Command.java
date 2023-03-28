package com.linkedin.venice.datarecovery;

public abstract class Command {
  public abstract void execute();

  public abstract Result getResult();

  public abstract boolean needWaitForFirstTaskToComplete();

  public abstract static class Params {
    // Store name.
    protected String store;

    public String getStore() {
      return store;
    }

    public void setStore(String store) {
      this.store = store;
    }
  }

  public abstract static class Result {
    private String cluster;
    private String store;
    protected String error;
    protected String message;

    // isCoreWorkDone indicates if the core task is finished when an interval is specified.
    protected boolean isCoreWorkDone = false;

    public String getCluster() {
      return cluster;
    }

    public void setCluster(String cluster) {
      this.cluster = cluster;
    }

    public String getStore() {
      return store;
    }

    public void setStore(String store) {
      this.store = store;
    }

    public boolean isError() {
      return error != null;
    }

    public void setError(String error) {
      this.error = error;
    }

    public String getError() {
      return error;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public boolean isCoreWorkDone() {
      return isCoreWorkDone;
    }

    public void setCoreWorkDone(boolean done) {
      isCoreWorkDone = done;
    }
  }
}
