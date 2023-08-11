package com.linkedin.venice.datarecovery.meta;

public class RepushViabilityInfo {
  private boolean isHybrid;
  private boolean isViable;
  private RepushViabilityInfo.Result result;

  public RepushViabilityInfo() {
    setHybrid(false);
    setViable(false);
    setResult(Result.NOT_STARTED);
  }

  public boolean isHybrid() {
    return isHybrid;
  }

  public void setHybrid(boolean hybrid) {
    isHybrid = hybrid;
  }

  public boolean isViable() {
    return isViable;
  }

  public void setViable(boolean viable) {
    isViable = viable;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  public RepushViabilityInfo succeedWithResult(Result result) {
    setResult(result);
    setViable(true);
    return this;
  }

  public RepushViabilityInfo failWithResult(Result result) {
    setResult(result);
    setViable(false);
    return this;
  }

  public enum Result {
    NOT_STARTED, SUCCESS, DISCOVERY_ERROR, NO_FUTURE_VERSION, TIMESTAMP_MISMATCH, ONGOING_PUSH, EXCEPTION_THROWN
  }
}
