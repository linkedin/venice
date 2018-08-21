package com.linkedin.venice.router.throttle;

import java.util.Optional;


public class NoopRouterThrottler implements RouterThrottler{
  public NoopRouterThrottler(){}

  @Override
  public void mayThrottleRead(String storeName, double readCapacityUnit, Optional<String> storageNodeId) {
    //noop
  }

  @Override
  public int getReadCapacity() {
    return 1;
  }
}
