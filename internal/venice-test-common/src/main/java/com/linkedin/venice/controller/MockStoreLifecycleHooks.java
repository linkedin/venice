package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;


public class MockStoreLifecycleHooks extends StoreLifecycleHooks {
  public static final String PRE_STORE_VERSION_CREATION_OUTCOME = "preStoreVersionCreationOutcome";

  public MockStoreLifecycleHooks(VeniceProperties defaultConfigs) {
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
    return StoreVersionLifecycleEventOutcome.valueOf(
        storeHooksConfigs
            .getString(PRE_STORE_VERSION_CREATION_OUTCOME, StoreVersionLifecycleEventOutcome.PROCEED.toString()));
  }

  @Override
  public StoreVersionLifecycleEventOutcome preStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      Lazy<JobStatusQueryResponse> jobStatus,
      VeniceProperties storeHooksConfigs) {
    return outcomeFromConfig(storeHooksConfigs);
  }

  private StoreVersionLifecycleEventOutcome outcomeFromConfig(VeniceProperties storeHooksConfigs) {
    String outcome = storeHooksConfigs.getString("outcome");
    if (StoreVersionLifecycleEventOutcome.PROCEED.toString().equals(outcome)) {
      return StoreVersionLifecycleEventOutcome.PROCEED;
    } else if (StoreVersionLifecycleEventOutcome.ROLLBACK.toString().equals(outcome)) {
      return StoreVersionLifecycleEventOutcome.ROLLBACK;
    }
    return StoreVersionLifecycleEventOutcome.WAIT;
  }
}
