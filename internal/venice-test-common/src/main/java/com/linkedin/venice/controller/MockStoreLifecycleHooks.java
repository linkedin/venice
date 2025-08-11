package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;


public class MockStoreLifecycleHooks extends StoreLifecycleHooks {
  public MockStoreLifecycleHooks(VeniceProperties defaultConfigs) {
    super(defaultConfigs);
  }

  @Override
  public StoreVersionLifecycleEventOutcome postStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    String outcome = storeHooksConfigs.getString("outcome");
    if (StoreVersionLifecycleEventOutcome.PROCEED.toString().equals(outcome)) {
      return StoreVersionLifecycleEventOutcome.PROCEED;
    } else if (StoreVersionLifecycleEventOutcome.ROLLBACK.toString().equals(outcome)) {
      return StoreVersionLifecycleEventOutcome.ROLLBACK;
    }

    return StoreVersionLifecycleEventOutcome.WAIT;
  }
}
