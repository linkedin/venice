package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreLifecycleHookExecutorTest {
  private VeniceProperties globalProps;
  private StoreLifecycleHookExecutor executor;
  private Store store;

  @BeforeMethod
  public void setUp() {
    globalProps = new VeniceProperties(new Properties());
    executor = new StoreLifecycleHookExecutor(globalProps);
    store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
  }

  @Test
  public void testInvokePostVersionSwapHooksNoHooks() {
    // Store with no lifecycle hooks — should be a no-op
    when(store.getStoreLifecycleHooks()).thenReturn(Collections.emptyList());
    // Should not throw
    executor.invokePostVersionSwapHooks("cluster1", store, 2, "prod-lor1", null);
  }

  @Test
  public void testInvokePostVersionSwapHooksLogsAndProceedsOnAbort() {
    // Create a fake LifecycleHooksRecord pointing to a no-op hook
    LifecycleHooksRecord record = mock(LifecycleHooksRecord.class);
    when(record.getStoreLifecycleHooksClassName()).thenReturn(NoOpAbortHook.class.getName());
    when(record.getStoreLifecycleHooksParams()).thenReturn(new java.util.HashMap<>());
    when(store.getStoreLifecycleHooks()).thenReturn(Collections.singletonList(record));

    // Should not throw even though the hook returns ABORT
    executor.invokePostVersionSwapHooks("cluster1", store, 2, "prod-lor1", null);
    // Verify the hook was called (indirectly — we can check via static counter if needed)
  }

  @Test
  public void testGetOrInstantiateHookCachesInstance() {
    StoreLifecycleHooks hook1 = executor.getOrInstantiateHook(NoOpProceedHook.class.getName());
    StoreLifecycleHooks hook2 = executor.getOrInstantiateHook(NoOpProceedHook.class.getName());
    Assert.assertSame(hook1, hook2, "Second call should return cached instance");
  }

  @Test
  public void testGetOrInstantiateHookReturnsNullOnBadClassName() {
    StoreLifecycleHooks hook = executor.getOrInstantiateHook("com.nonexistent.Hook");
    Assert.assertNull(hook, "Unknown class should return null");
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
        String regionName,
        com.linkedin.venice.utils.lazy.Lazy<com.linkedin.venice.controllerapi.JobStatusQueryResponse> jobStatus,
        VeniceProperties storeHooksConfigs) {
      return StoreVersionLifecycleEventOutcome.ABORT;
    }
  }
}
