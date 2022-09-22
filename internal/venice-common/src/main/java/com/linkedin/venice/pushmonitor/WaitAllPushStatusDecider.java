package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.OfflinePushStrategy;


/**
 * Wait all strategy stands for Venice do not allow one replica to be failed. In case of failure, Venice fail the whole
 * push.
 */
public class WaitAllPushStatusDecider extends PushStatusDecider {
  @Override
  public OfflinePushStrategy getStrategy() {
    return OfflinePushStrategy.WAIT_ALL_REPLICAS;
  }

  @Override
  public boolean hasEnoughReplicasForOnePartition(int actual, int expected) {
    return actual >= expected;
  }

  @Override
  protected int getNumberOfToleratedErrors() {
    return 0;
  }
}
