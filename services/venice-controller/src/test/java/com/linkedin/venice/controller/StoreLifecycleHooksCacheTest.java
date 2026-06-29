package com.linkedin.venice.controller;

import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreLifecycleHooksCacheTest {
  private VeniceProperties globalProps;
  private StoreLifecycleHooksCache storeLifecycleHooksCache;

  @BeforeMethod
  public void setUp() {
    globalProps = new VeniceProperties(new Properties());
    storeLifecycleHooksCache = new StoreLifecycleHooksCache(globalProps);
  }

  @Test
  public void testGetOrInstantiateHookCachesInstance() {
    StoreLifecycleHooks hook1 = storeLifecycleHooksCache.getOrInstantiateHook(NoOpProceedHook.class.getName());
    StoreLifecycleHooks hook2 = storeLifecycleHooksCache.getOrInstantiateHook(NoOpProceedHook.class.getName());
    Assert.assertSame(hook1, hook2, "Second call should return cached instance");
  }

  @Test
  public void testGetOrInstantiateHookReturnsNullOnBadClassName() {
    StoreLifecycleHooks hook = storeLifecycleHooksCache.getOrInstantiateHook("com.nonexistent.Hook");
    Assert.assertNull(hook, "Unknown class should return null");
  }

  @Test
  public void testGetOrInstantiateHookReturnsNullForFailedClass() {
    String badClassName = "com.nonexistent.Foo";

    // First call: attempts instantiation, fails, caches failure
    StoreLifecycleHooks result1 = storeLifecycleHooksCache.getOrInstantiateHook(badClassName);
    Assert.assertNull(result1, "Should return null for unknown class");

    // Second call: should return null from failedClasses cache without retrying
    StoreLifecycleHooks result2 = storeLifecycleHooksCache.getOrInstantiateHook(badClassName);
    Assert.assertNull(result2, "Should return null from failure cache without retry");
  }

  /** Minimal hook that always returns PROCEED. Must have a VeniceProperties constructor. */
  public static class NoOpProceedHook extends StoreLifecycleHooks {
    public NoOpProceedHook(VeniceProperties props) {
      super(props);
    }
  }

  /** Minimal hook that always returns ABORT from postStoreVersionSwap. */
  public static class NoOpAbortHook extends StoreLifecycleHooks {
    public NoOpAbortHook(VeniceProperties props) {
      super(props);
    }

    @Override
    public StoreVersionLifecycleEventOutcome postStoreVersionSwap(
        String clusterName,
        String storeName,
        int versionNumber,
        int previousVersion,
        String regionName,
        com.linkedin.venice.utils.lazy.Lazy<com.linkedin.venice.controllerapi.JobStatusQueryResponse> jobStatus,
        VeniceProperties storeHooksConfigs) {
      return StoreVersionLifecycleEventOutcome.ABORT;
    }
  }
}
