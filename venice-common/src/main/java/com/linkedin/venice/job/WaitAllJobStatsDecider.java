package com.linkedin.venice.job;

import com.linkedin.venice.meta.OfflinePushStrategy;


/**
 * Wait all strategy stands for Venice do not allow one task/node to be failed. If failure happen, Venice fail the whole
 * job.
 */
public class WaitAllJobStatsDecider extends JobStatusDecider {

  @Override
  protected boolean hasEnoughReplicasForOnePartition(int actual, int expected) {
    return actual < expected ? false : true;
  }

  @Override
  public OfflinePushStrategy getStrategy() {
    return OfflinePushStrategy.WAIT_ALL_REPLICAS;
  }
}
