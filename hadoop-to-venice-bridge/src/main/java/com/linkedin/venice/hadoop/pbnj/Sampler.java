package com.linkedin.venice.hadoop.pbnj;

public class Sampler {
  private final double samplingRatio;

  public Sampler(double samplingRatio) {
    if (samplingRatio <= 0.0) {
      throw new IllegalArgumentException("Sampling ratio cannot be zero or less.");
    }
    if (samplingRatio > 1.0) {
      throw new IllegalArgumentException("Sampling ratio cannot be greater than one.");
    }
    this.samplingRatio = samplingRatio;
  }

  /**
   * @return true if we should skip, or false if we should process
   */
  public boolean checkWhetherToSkip(long queriedRecords, long skippedRecords) {
    if (samplingRatio == 1.0) {
      return false;
    }

    double mappedRecords = queriedRecords + skippedRecords;
    if (mappedRecords <= 0) {
      // to avoid dividing by zero. This also ensures that we at least query the first record.
      return false;
    }

    double currentSamplingRatio = queriedRecords / mappedRecords;
    return currentSamplingRatio > samplingRatio;
  }
}
