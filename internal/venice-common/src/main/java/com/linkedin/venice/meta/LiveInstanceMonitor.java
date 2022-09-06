package com.linkedin.venice.meta;

import java.util.Set;


public interface LiveInstanceMonitor {
  /**
   * Check whether current instance is alive or not.
   *
   * @param instance
   * @return
   */
  boolean isInstanceAlive(Instance instance);

  Set<Instance> getAllLiveInstances();

  void registerLiveInstanceChangedListener(LiveInstanceChangedListener listener);
}
