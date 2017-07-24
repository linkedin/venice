package com.linkedin.venice.stats;

public class DIVStats {
  private long duplicateMsg = 0;
  private long missingMsg = 0;
  private long corruptedMsg = 0;
  private long successMsg = 0;
  private long currentIdleTime = 0;
  private long overallIdleTime = 0;

  public long getDuplicateMsg() {
    return duplicateMsg;
  }

  public void recordDuplicateMsg() {
    duplicateMsg += 1;
  }

  public long getMissingMsg() {
    return missingMsg;
  }

  public void recordMissingMsg() {
    missingMsg += 1;
  }

  public long getCorruptedMsg() {
    return corruptedMsg;
  }

  public void recordCorruptedMsg() {
    corruptedMsg += 1;
  }

  public long getSuccessMsg() {
    return successMsg;
  }

  public void recordSuccessMsg() {
    successMsg += 1;
  }

  public long getCurrentIdleTime() {
    return currentIdleTime;
  }

  public void recordCurrentIdleTime() {
    currentIdleTime += 1;
  }

  public void resetCurrentIdleTime() {
    currentIdleTime = 0;
  }

  public long getOverallIdleTime() {
    return overallIdleTime;
  }

  public void recordOverallIdleTime() {
    overallIdleTime += 1;
  }
}
