package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.OfflinePushStrategy;


/**
 * Wait N-1 strategy stands for Venice could tolerate one of replica to be failed for each partition. Otherwise, once
 * more replica failed, Venice should fail the whole push.
 */
public class WaitNMinusOnePushStatusDecider extends PushStatusDecider {
  @Override
  public OfflinePushStrategy getStrategy() {
    return OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;
  }

  @Override
  public boolean hasEnoughReplicasForOnePartition(int actual, int expected) {
    if (expected == 1) {
      return actual >= expected;
    } else {
      return actual >= expected - 1;
    }
  }

  @Override
  protected int getNumberOfToleratedErrors() {
    return 1;
  }
}
