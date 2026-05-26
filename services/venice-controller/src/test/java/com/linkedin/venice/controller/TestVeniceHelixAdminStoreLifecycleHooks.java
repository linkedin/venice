package com.linkedin.venice.controller;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hooks.StoreLifecycleEventOutcome;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminStoreLifecycleHooks {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final String REGION = "test-region";
  private static final int VERSION = 1;

  private VeniceHelixAdmin admin;
  private Map<String, StoreLifecycleHooks> hookCache;

  @BeforeMethod
  public void setUp() throws Exception {
    admin = mock(VeniceHelixAdmin.class);
    doReturn(REGION).when(admin).getRegionName();

    doCallRealMethod().when(admin).getOrCreateHookInstance(ArgumentMatchers.any());
    doCallRealMethod().when(admin)
        .invokePreStoreVersionCreationHooks(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any());
    doCallRealMethod().when(admin)
        .invokePostStoreVersionCreationHooks(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any());
    doCallRealMethod().when(admin)
        .invokePreStoreVersionDeletionHooks(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any());
    doCallRealMethod().when(admin)
        .invokePostStoreVersionDeletionHooks(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any());

    hookCache = new VeniceConcurrentHashMap<>();
    Set<String> failedHookClasses = ConcurrentHashMap.newKeySet();
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field cacheField = VeniceHelixAdmin.class.getDeclaredField("storeLifecycleHooksInstanceCache");
        cacheField.setAccessible(true);
        cacheField.set(admin, hookCache);
        Field failedField = VeniceHelixAdmin.class.getDeclaredField("failedHookClasses");
        failedField.setAccessible(true);
        failedField.set(admin, failedHookClasses);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @DataProvider(name = "hookMethods")
  public Object[][] hookMethods() {
    return new Object[][] {
        { "pre-creation", (HookInvoker) (a, r) -> a.invokePreStoreVersionCreationHooks(CLUSTER, STORE, VERSION, r),
            (Predicate<TrackingHooks>) h -> h.preCreationCalled },
        { "post-creation", (HookInvoker) (a, r) -> a.invokePostStoreVersionCreationHooks(CLUSTER, STORE, VERSION, r),
            (Predicate<TrackingHooks>) h -> h.postCreationCalled },
        { "pre-deletion", (HookInvoker) (a, r) -> a.invokePreStoreVersionDeletionHooks(CLUSTER, STORE, VERSION, r),
            (Predicate<TrackingHooks>) h -> h.preDeletionCalled },
        { "post-deletion", (HookInvoker) (a, r) -> a.invokePostStoreVersionDeletionHooks(CLUSTER, STORE, VERSION, r),
            (Predicate<TrackingHooks>) h -> h.postDeletionCalled }, };
  }

  @DataProvider(name = "preHookMethods")
  public Object[][] preHookMethods() {
    return new Object[][] {
        { "pre-creation", (Consumer<TrackingHooks>) h -> h.preCreationOutcome = StoreVersionLifecycleEventOutcome.ABORT,
            (HookInvoker) (a, r) -> a.invokePreStoreVersionCreationHooks(CLUSTER, STORE, VERSION, r) },
        { "pre-deletion", (Consumer<TrackingHooks>) h -> h.preDeletionOutcome = StoreLifecycleEventOutcome.ABORT,
            (HookInvoker) (a, r) -> a.invokePreStoreVersionDeletionHooks(CLUSTER, STORE, VERSION, r) }, };
  }

  @Test(dataProvider = "hookMethods")
  public void testHookIsCalled(String desc, HookInvoker invoke, Predicate<TrackingHooks> calledFlag) {
    TrackingHooks hook = registerHook(new TrackingHooks());
    invoke.accept(admin, hookRecords(TrackingHooks.class));
    assertTrue(calledFlag.test(hook), desc + " hook should have been called");
  }

  @Test(dataProvider = "hookMethods")
  public void testHookReceivesCorrectParams(String desc, HookInvoker invoke, Predicate<TrackingHooks> ignored) {
    TrackingHooks hook = registerHook(new TrackingHooks());
    invoke.accept(admin, hookRecords(TrackingHooks.class));
    assertEquals(hook.lastCluster, CLUSTER, desc + ": cluster");
    assertEquals(hook.lastStore, STORE, desc + ": store");
    assertEquals(hook.lastVersion, VERSION, desc + ": version");
    assertEquals(hook.lastRegion, REGION, desc + ": region");
  }

  @Test(dataProvider = "hookMethods")
  public void testExceptionInHookIsSwallowed(String desc, HookInvoker invoke, Predicate<TrackingHooks> ignored) {
    registerHook(new ThrowingHooks());
    invoke.accept(admin, hookRecords(ThrowingHooks.class));
  }

  @Test(dataProvider = "hookMethods")
  public void testEmptyHookListIsNoOp(String desc, HookInvoker invoke, Predicate<TrackingHooks> ignored) {
    invoke.accept(admin, Collections.emptyList());
  }

  @Test(dataProvider = "hookMethods")
  public void testUnknownHookClassIsSkipped(String desc, HookInvoker invoke, Predicate<TrackingHooks> ignored) {
    LifecycleHooksRecord badRecord = new LifecycleHooksRecordImpl("com.nonexistent.HookClass", Collections.emptyMap());
    invoke.accept(admin, Collections.singletonList(badRecord));
  }

  @Test(dataProvider = "preHookMethods")
  public void testAbortOutcomeThrows(String desc, Consumer<TrackingHooks> configureAbort, HookInvoker invoke) {
    TrackingHooks hook = registerHook(new TrackingHooks());
    configureAbort.accept(hook);
    expectThrows(VeniceException.class, () -> invoke.accept(admin, hookRecords(TrackingHooks.class)));
  }

  @Test
  public void testHookInstanceIsCached() {
    TrackingHooks hook = registerHook(new TrackingHooks());
    LifecycleHooksRecord record = new LifecycleHooksRecordImpl(TrackingHooks.class.getName(), Collections.emptyMap());

    StoreLifecycleHooks first = admin.getOrCreateHookInstance(record);
    StoreLifecycleHooks second = admin.getOrCreateHookInstance(record);

    assertNotNull(first);
    assertSame(first, second, "same instance should be returned for the same class name");
    assertSame(first, hook, "cached instance should be the one we registered");
  }

  @Test
  public void testHookInstanceCreatedViaReflectionIfNotCached() {
    LifecycleHooksRecord record = new LifecycleHooksRecordImpl(TrackingHooks.class.getName(), Collections.emptyMap());
    injectMultiClusterConfigs();

    StoreLifecycleHooks created = admin.getOrCreateHookInstance(record);
    assertNotNull(created);
    assertTrue(created instanceof TrackingHooks);
  }

  @Test
  public void testHookInstanceReturnsNullForUnknownClass() {
    LifecycleHooksRecord record = new LifecycleHooksRecordImpl("com.nonexistent.HookClass", Collections.emptyMap());
    injectMultiClusterConfigs();

    assertFalse(admin.getOrCreateHookInstance(record) != null, "should return null for unknown hook class");
  }

  @Test
  public void testMultipleHooksAllInvoked() {
    TrackingHooks hook1 = new TrackingHooks();
    TrackingHooks hook2 = new TrackingHooks();
    hookCache.put("hook1", hook1);
    hookCache.put("hook2", hook2);

    admin.invokePreStoreVersionCreationHooks(CLUSTER, STORE, VERSION, recordsForKeys("hook1", "hook2"));

    assertTrue(hook1.preCreationCalled);
    assertTrue(hook2.preCreationCalled);
  }

  @Test
  public void testFirstHookAbortStopsRemainingHooks() {
    TrackingHooks hook1 = new TrackingHooks();
    hook1.preCreationOutcome = StoreVersionLifecycleEventOutcome.ABORT;
    TrackingHooks hook2 = new TrackingHooks();
    hookCache.put("hook1", hook1);
    hookCache.put("hook2", hook2);

    expectThrows(
        VeniceException.class,
        () -> admin.invokePreStoreVersionCreationHooks(CLUSTER, STORE, VERSION, recordsForKeys("hook1", "hook2")));
    assertFalse(hook2.preCreationCalled, "second hook should not be called after first hook aborts");
  }

  private List<LifecycleHooksRecord> hookRecords(Class<? extends StoreLifecycleHooks> hookClass) {
    return Collections.singletonList(new LifecycleHooksRecordImpl(hookClass.getName(), Collections.emptyMap()));
  }

  private List<LifecycleHooksRecord> recordsForKeys(String... keys) {
    List<LifecycleHooksRecord> records = new ArrayList<>();
    for (String key: keys) {
      records.add(new LifecycleHooksRecordImpl(key, Collections.emptyMap()));
    }
    return records;
  }

  private <T extends StoreLifecycleHooks> T registerHook(T hook) {
    hookCache.put(hook.getClass().getName(), hook);
    return hook;
  }

  private void injectMultiClusterConfigs() {
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(clusterConfig).when(multiClusterConfigs).getCommonConfig();
    doReturn(new VeniceProperties(new Properties())).when(clusterConfig).getProps();
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field field = VeniceHelixAdmin.class.getDeclaredField("multiClusterConfigs");
        field.setAccessible(true);
        field.set(admin, multiClusterConfigs);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @FunctionalInterface
  interface HookInvoker extends BiConsumer<VeniceHelixAdmin, List<LifecycleHooksRecord>> {
  }

  public static class TrackingHooks extends StoreLifecycleHooks {
    boolean preCreationCalled;
    boolean postCreationCalled;
    boolean preDeletionCalled;
    boolean postDeletionCalled;

    String lastCluster;
    String lastStore;
    int lastVersion;
    String lastRegion;

    StoreVersionLifecycleEventOutcome preCreationOutcome = StoreVersionLifecycleEventOutcome.PROCEED;
    StoreLifecycleEventOutcome preDeletionOutcome = StoreLifecycleEventOutcome.PROCEED;

    public TrackingHooks() {
      super(new VeniceProperties(new Properties()));
    }

    public TrackingHooks(VeniceProperties defaultConfigs) {
      super(defaultConfigs);
    }

    @Override
    public StoreVersionLifecycleEventOutcome preStoreVersionCreation(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        com.linkedin.venice.utils.lazy.Lazy<com.linkedin.venice.controllerapi.JobStatusQueryResponse> jobStatus,
        VeniceProperties storeHooksConfigs) {
      preCreationCalled = true;
      lastCluster = clusterName;
      lastStore = storeName;
      lastVersion = versionNumber;
      lastRegion = regionName;
      return preCreationOutcome;
    }

    @Override
    public void postStoreVersionCreation(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      postCreationCalled = true;
      lastCluster = clusterName;
      lastStore = storeName;
      lastVersion = versionNumber;
      lastRegion = regionName;
    }

    @Override
    public StoreLifecycleEventOutcome preStoreVersionDeletion(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      preDeletionCalled = true;
      lastCluster = clusterName;
      lastStore = storeName;
      lastVersion = versionNumber;
      lastRegion = regionName;
      return preDeletionOutcome;
    }

    @Override
    public void postStoreVersionDeletion(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      postDeletionCalled = true;
      lastCluster = clusterName;
      lastStore = storeName;
      lastVersion = versionNumber;
      lastRegion = regionName;
    }
  }

  public static class ThrowingHooks extends StoreLifecycleHooks {
    public ThrowingHooks() {
      super(new VeniceProperties(new Properties()));
    }

    public ThrowingHooks(VeniceProperties defaultConfigs) {
      super(defaultConfigs);
    }

    @Override
    public StoreVersionLifecycleEventOutcome preStoreVersionCreation(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        com.linkedin.venice.utils.lazy.Lazy<com.linkedin.venice.controllerapi.JobStatusQueryResponse> jobStatus,
        VeniceProperties storeHooksConfigs) {
      throw new RuntimeException("pre-creation hook failure");
    }

    @Override
    public void postStoreVersionCreation(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      throw new RuntimeException("post-creation hook failure");
    }

    @Override
    public StoreLifecycleEventOutcome preStoreVersionDeletion(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      throw new RuntimeException("pre-deletion hook failure");
    }

    @Override
    public void postStoreVersionDeletion(
        String clusterName,
        String storeName,
        int versionNumber,
        String regionName,
        VeniceProperties storeHooksConfigs) {
      throw new RuntimeException("post-deletion hook failure");
    }
  }
}
