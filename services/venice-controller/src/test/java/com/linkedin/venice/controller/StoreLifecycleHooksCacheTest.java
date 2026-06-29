package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @Test
  public void testInvokePostVersionSwapHooksEmptyListReturnsProceed() {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getStoreLifecycleHooks()).thenReturn(Collections.emptyList());

    StoreVersionLifecycleEventOutcome outcome =
        storeLifecycleHooksCache.invokePostVersionSwapHooks("cluster1", store, 2, 1, "prod-lor1", null);

    Assert.assertEquals(outcome, StoreVersionLifecycleEventOutcome.PROCEED);
  }

  @Test
  public void testInvokePostVersionSwapHooksReturnsWorstOutcome() {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getStoreLifecycleHooks()).thenReturn(buildRecords(NoOpProceedHook.class, NoOpAbortHook.class));

    StoreVersionLifecycleEventOutcome outcome =
        storeLifecycleHooksCache.invokePostVersionSwapHooks("cluster1", store, 2, 1, "prod-lor1", null);

    Assert.assertEquals(outcome, StoreVersionLifecycleEventOutcome.ABORT, "ABORT should dominate PROCEED");
  }

  @Test
  public void testInvokePostVersionSwapHooksActuallyInvokesHook() {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getStoreLifecycleHooks()).thenReturn(buildRecords(NoOpAbortHook.class));

    // If the hook is invoked we get ABORT; if no-op we get PROCEED.
    StoreVersionLifecycleEventOutcome outcome =
        storeLifecycleHooksCache.invokePostVersionSwapHooks("cluster1", store, 2, 1, "prod-lor1", null);

    Assert.assertEquals(outcome, StoreVersionLifecycleEventOutcome.ABORT, "Hook must be invoked");
  }

  private static List<LifecycleHooksRecord> buildRecords(Class<?>... hookClasses) {
    List<LifecycleHooksRecord> records = new ArrayList<>();
    for (Class<?> hookClass: hookClasses) {
      records.add(new LifecycleHooksRecordImpl(hookClass.getName(), Collections.emptyMap()));
    }
    return records;
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
