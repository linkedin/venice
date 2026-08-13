package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;


public class MockStoreVersionCreationLifecycleHooks extends StoreLifecycleHooks {
  public MockStoreVersionCreationLifecycleHooks(VeniceProperties defaultConfigs) {
    super(defaultConfigs);
  }

  @Override
  public StoreVersionLifecycleEventOutcome preStoreVersionCreation(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.valueOf(storeHooksConfigs.getString("outcome"));
  }
}
