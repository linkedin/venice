package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.pushmonitor.WaitAllPushStatusDecider;
import com.linkedin.venice.pushmonitor.WaitNMinusOnePushStatusDecider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enum of strategies used to decide the when the data is ready to serve in off-line push.
 */
public enum OfflinePushStrategy {
  /*Wait all replica is ready, the version is ready to serve.*/
  WAIT_ALL_REPLICAS(0, new WaitAllPushStatusDecider()),
  /*Wait until N-1 replicas are ready, the version is ready to serve*/
  WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION(1, new WaitNMinusOnePushStatusDecider());

  public final int value;
  private final PushStatusDecider pushStatusDecider;

  OfflinePushStrategy(int value, PushStatusDecider decider) {
    this.value = value;
    this.pushStatusDecider = decider;
  }

  private static final Map<Integer, OfflinePushStrategy> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static OfflinePushStrategy getOfflinePushStrategyFromInt(int v) {
    OfflinePushStrategy strategy = idMapping.get(v);
    if (strategy == null) {
      throw new VeniceException("Invalid OfflinePushStrategy id: " + v);
    }
    return strategy;
  }

  public PushStatusDecider getPushStatusDecider() {
    return pushStatusDecider;
  }
}
