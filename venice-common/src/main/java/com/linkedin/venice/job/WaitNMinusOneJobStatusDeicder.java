package com.linkedin.venice.job;

import com.linkedin.venice.meta.OfflinePushStrategy;


/**
 * Wait N-1 strategy stands for Venice could tolerate one of task/node failed for each of partition. But if more than
 * one of them are failed, Venice should fail the whole job.
 */
public class WaitNMinusOneJobStatusDeicder extends JobStatusDecider {
  @Override
  protected boolean hasEnoughReplicasForOnePartition(int actual, int expected) {
    if (expected == 1) {
      return actual < expected ? false : true;
    } else {
      return actual < expected - 1 ? false : true;
    }
  }

  @Override
  public OfflinePushStrategy getStrategy() {
    return OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;
  }
}
