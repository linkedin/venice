package com.linkedin.venice.listener.response.stats;

public class MultiKeyResponseStats extends AbstractReadResponseStats {
  private int recordCount = -1;

  @Override
  public void addKeySize(int size) {
  }

  @Override
  public void addValueSize(int size) {
  }

  public void setRecordCount(int count) {
    this.recordCount = count;
  }

  @Override
  protected int getRecordCount() {
    return this.recordCount;
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    super.merge(other);
    if (other instanceof MultiKeyResponseStats) {
      MultiKeyResponseStats otherStats = (MultiKeyResponseStats) other;
      this.recordCount += otherStats.recordCount;
    }
  }
}
