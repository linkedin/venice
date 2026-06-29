package com.linkedin.venice.controller;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * @deprecated Use {@link StoreLifecycleHooksCache} instead.
 */
@Deprecated
public class StoreLifecycleHookExecutor extends StoreLifecycleHooksCache {
  public StoreLifecycleHookExecutor(VeniceProperties globalProps) {
    super(globalProps);
  }
}
