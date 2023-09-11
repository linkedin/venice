package com.linkedin.venice.datarecovery.meta;

public class RepushViabilityInfo {
  private boolean isHybrid;
  private boolean isViable;
  private boolean isError = false;
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

  public boolean isError() {
    return isError;
  }

  public void setError(boolean error) {
    isError = error;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  public RepushViabilityInfo viableWithResult(Result result) {
    setResult(result);
    setViable(true);
    return this;
  }

  public RepushViabilityInfo inViableWithResult(Result result) {
    setResult(result);
    setViable(false);
    return this;
  }

  public RepushViabilityInfo inViableWithError(Result result) {
    setResult(result);
    setViable(false);
    setError(true);
    return this;
  }

  public enum Result {
    NOT_STARTED, SUCCESS, DISCOVERY_ERROR, NO_CURRENT_VERSION, CURRENT_VERSION_IS_NEWER, ONGOING_PUSH, EXCEPTION_THROWN
  }
}
