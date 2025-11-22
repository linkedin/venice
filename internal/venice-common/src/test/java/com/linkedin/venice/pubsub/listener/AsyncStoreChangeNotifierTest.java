package com.linkedin.venice.pubsub.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.LogContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AsyncStoreChangeNotifierTest {
  private AsyncStoreChangeNotifier notifier;

  @BeforeMethod
  public void setUp() {
    notifier = new AsyncStoreChangeNotifier(VeniceComponent.SERVER, LogContext.EMPTY, 2);
  }

  @AfterMethod
  public void tearDown() {
    if (notifier != null) {
      notifier.close();
    }
  }

  @Test
  public void testRegisterTasks() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreCreated(store -> {}).build();

    String taskId = notifier.registerTasks("TestAdapter", tasks);

    assertNotNull(taskId);
    assertTrue(taskId.startsWith("TestAdapter-"));
    assertEquals(notifier.getRegisteredTaskCount(), 1);
  }

  @Test
  public void testRegisterMultipleTasks() {
    StoreChangeTasks tasks1 = StoreChangeTasks.builder().onStoreCreated(store -> {}).build();
    StoreChangeTasks tasks2 = StoreChangeTasks.builder().onStoreDeleted(store -> {}).build();

    String taskId1 = notifier.registerTasks("Adapter1", tasks1);
    String taskId2 = notifier.registerTasks("Adapter2", tasks2);

    assertNotEquals(taskId1, taskId2);
    assertEquals(notifier.getRegisteredTaskCount(), 2);
  }

  @Test
  public void testUnregisterTasks() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreCreated(store -> {}).build();
    String taskId = notifier.registerTasks("TestAdapter", tasks);

    assertTrue(notifier.unregisterTasks(taskId));
    assertEquals(notifier.getRegisteredTaskCount(), 0);
    assertFalse(notifier.unregisterTasks(taskId)); // Second call returns false
  }

  @Test
  public void testRegisterTasksWithNullClientId() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().build();
    expectThrows(IllegalArgumentException.class, () -> notifier.registerTasks(null, tasks));
  }

  @Test
  public void testRegisterTasksWithEmptyClientId() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().build();
    expectThrows(IllegalArgumentException.class, () -> notifier.registerTasks("", tasks));
  }

  @Test
  public void testRegisterTasksWithNullTasks() {
    expectThrows(IllegalArgumentException.class, () -> notifier.registerTasks("TestAdapter", null));
  }

  @Test
  public void testRegisterTasksAfterClose() {
    notifier.close();
    StoreChangeTasks tasks = StoreChangeTasks.builder().build();
    expectThrows(IllegalStateException.class, () -> notifier.registerTasks("TestAdapter", tasks));
  }

  @Test
  public void testHandleStoreCreated() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreCreated(store -> {
      callCount.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));

    notifier.handleStoreCreated(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
  }

  @Test
  public void testHandleStoreDeleted() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreDeleted(store -> {
      callCount.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");

    notifier.handleStoreDeleted(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
  }

  @Test
  public void testHandleVersionAdded() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);
    AtomicInteger capturedVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onVersionAdded((store, version) -> {
      callCount.incrementAndGet();
      capturedVersion.set(version);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));

    // Initialize state
    notifier.handleStoreCreated(store);

    // Add version 3
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3 }));
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
    assertEquals(capturedVersion.get(), 3);
  }

  @Test
  public void testHandleVersionDeleted() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);
    AtomicInteger capturedVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onVersionDeleted((store, version) -> {
      callCount.incrementAndGet();
      capturedVersion.set(version);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3 }));

    // Initialize state
    notifier.handleStoreCreated(store);

    // Delete version 2
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 3 }));
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
    assertEquals(capturedVersion.get(), 2);
  }

  @Test
  public void testMultipleTasksExecuted() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger adapter1Calls = new AtomicInteger(0);
    AtomicInteger adapter2Calls = new AtomicInteger(0);

    StoreChangeTasks tasks1 = StoreChangeTasks.builder().onStoreCreated(store -> {
      adapter1Calls.incrementAndGet();
      latch.countDown();
    }).build();

    StoreChangeTasks tasks2 = StoreChangeTasks.builder().onStoreCreated(store -> {
      adapter2Calls.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("Adapter1", tasks1);
    notifier.registerTasks("Adapter2", tasks2);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1 }));

    notifier.handleStoreCreated(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(adapter1Calls.get(), 1);
    assertEquals(adapter2Calls.get(), 1);
  }

  @Test
  public void testTaskExceptionIsolation() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger goodTaskCalls = new AtomicInteger(0);

    StoreChangeTasks badTasks = StoreChangeTasks.builder().onStoreCreated(store -> {
      throw new RuntimeException("Task failure");
    }).build();

    StoreChangeTasks goodTasks = StoreChangeTasks.builder().onStoreCreated(store -> {
      goodTaskCalls.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("BadAdapter", badTasks);
    notifier.registerTasks("GoodAdapter", goodTasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1 }));

    notifier.handleStoreCreated(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(goodTaskCalls.get(), 1); // Good task still executed
  }

  @Test
  public void testHandleNullStore() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreCreated(store -> fail("Should not be called")).build();

    notifier.registerTasks("TestAdapter", tasks);

    // Should not throw exception or call task
    notifier.handleStoreCreated(null);
    notifier.handleStoreDeleted((Store) null);
    notifier.handleStoreChanged(null);
  }

  @Test
  public void testClose() {
    StoreChangeTasks tasks = StoreChangeTasks.builder().onStoreCreated(store -> {}).build();
    notifier.registerTasks("TestAdapter", tasks);

    notifier.close();

    assertEquals(notifier.getRegisteredTaskCount(), 0);

    // Second close should be idempotent
    notifier.close();
  }

  @Test
  public void testMultipleVersionChanges() throws InterruptedException {
    CountDownLatch addedLatch = new CountDownLatch(2);
    CountDownLatch deletedLatch = new CountDownLatch(1);
    AtomicInteger addedCount = new AtomicInteger(0);
    AtomicInteger deletedCount = new AtomicInteger(0);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onVersionAdded((store, version) -> {
      addedCount.incrementAndGet();
      addedLatch.countDown();
    }).onVersionDeleted((store, version) -> {
      deletedCount.incrementAndGet();
      deletedLatch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));

    // Initialize
    notifier.handleStoreCreated(store);

    // Add versions 3 and 4, delete version 1
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 2, 3, 4 }));
    notifier.handleStoreChanged(store);

    assertTrue(addedLatch.await(5, TimeUnit.SECONDS));
    assertTrue(deletedLatch.await(5, TimeUnit.SECONDS));
    assertEquals(addedCount.get(), 2);
    assertEquals(deletedCount.get(), 1);
  }

  @Test
  public void testConstructorWithInvalidThreadPoolSize() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new AsyncStoreChangeNotifier(VeniceComponent.SERVER, LogContext.EMPTY, 0));
  }

  @Test
  public void testConstructorWithNullLogContext() {
    expectThrows(IllegalArgumentException.class, () -> new AsyncStoreChangeNotifier(VeniceComponent.SERVER, null, 2));
  }

  @Test
  public void testConstructorWithNullVeniceComponent() {
    expectThrows(IllegalArgumentException.class, () -> new AsyncStoreChangeNotifier(null, LogContext.EMPTY, 2));
  }

  @Test
  public void testHandleCurrentVersionChanged() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);
    AtomicInteger capturedNewVersion = new AtomicInteger(-1);
    AtomicInteger capturedPreviousVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      callCount.incrementAndGet();
      capturedNewVersion.set(newVersion);
      capturedPreviousVersion.set(prevVersion);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(1);

    // Initialize state
    notifier.handleStoreCreated(store);

    // Change current version from 1 to 2
    when(store.getCurrentVersion()).thenReturn(2);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
    assertEquals(capturedNewVersion.get(), 2);
    assertEquals(capturedPreviousVersion.get(), 1);
  }

  @Test
  public void testCurrentVersionChangedWithNoPreviousVersion() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger capturedNewVersion = new AtomicInteger(-1);
    AtomicInteger capturedPreviousVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      capturedNewVersion.set(newVersion);
      capturedPreviousVersion.set(prevVersion);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1 }));
    when(store.getCurrentVersion()).thenReturn(Store.NON_EXISTING_VERSION);

    // Initialize state with no current version
    notifier.handleStoreCreated(store);

    // First version goes online
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1 }));
    when(store.getCurrentVersion()).thenReturn(1);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(capturedNewVersion.get(), 1);
    assertEquals(capturedPreviousVersion.get(), Store.NON_EXISTING_VERSION);
  }

  @Test
  public void testCurrentVersionRollback() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger capturedNewVersion = new AtomicInteger(-1);
    AtomicInteger capturedPreviousVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      capturedNewVersion.set(newVersion);
      capturedPreviousVersion.set(prevVersion);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3 }));
    when(store.getCurrentVersion()).thenReturn(3);

    // Initialize state with version 3
    notifier.handleStoreCreated(store);

    // Rollback from version 3 to version 2
    when(store.getCurrentVersion()).thenReturn(2);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(capturedNewVersion.get(), 2);
    assertEquals(capturedPreviousVersion.get(), 3);
  }

  @Test
  public void testNoNotificationWhenCurrentVersionUnchanged() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      callCount.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(2);

    // Initialize state
    notifier.handleStoreCreated(store);

    // Trigger change with same current version
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3 }));
    when(store.getCurrentVersion()).thenReturn(2); // Same as before
    notifier.handleStoreChanged(store);

    // Wait a bit to ensure no notification
    assertFalse(latch.await(1, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 0);
  }

  @Test
  public void testCurrentVersionChangedIsolation() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger goodTaskCalls = new AtomicInteger(0);

    StoreChangeTasks badTasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      throw new RuntimeException("Task failure");
    }).build();

    StoreChangeTasks goodTasks =
        StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
          goodTaskCalls.incrementAndGet();
          latch.countDown();
        }).build();

    notifier.registerTasks("BadAdapter", badTasks);
    notifier.registerTasks("GoodAdapter", goodTasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(1);

    // Initialize state
    notifier.handleStoreCreated(store);

    // Change current version
    when(store.getCurrentVersion()).thenReturn(2);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(goodTaskCalls.get(), 1); // Good task still executed
  }

  @Test
  public void testStoreCreatedInitializesCurrentVersion() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger capturedNewVersion = new AtomicInteger(-1);
    AtomicInteger capturedPreviousVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      capturedNewVersion.set(newVersion);
      capturedPreviousVersion.set(prevVersion);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(2);

    // Create store with existing current version - should initialize but not notify
    notifier.handleStoreCreated(store);

    // Now change it to verify initialization worked
    when(store.getCurrentVersion()).thenReturn(1);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(capturedNewVersion.get(), 1);
    assertEquals(capturedPreviousVersion.get(), 2); // Proves initialization happened
  }

  @Test
  public void testStoreDeletedClearsCurrentVersion() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);
    AtomicInteger capturedPreviousVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      callCount.incrementAndGet();
      capturedPreviousVersion.set(prevVersion);
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(2);

    // Initialize state
    notifier.handleStoreCreated(store);

    // Delete store
    notifier.handleStoreDeleted(store);

    // Recreate store - should treat as first version (no previous tracked)
    when(store.getCurrentVersion()).thenReturn(1);
    notifier.handleStoreCreated(store);

    // Change version to trigger notification
    when(store.getCurrentVersion()).thenReturn(2);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 1);
    assertEquals(capturedPreviousVersion.get(), 1); // Previous is 1, not the old deleted store's version
  }

  @Test
  public void testMultipleCurrentVersionChanges() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(3);
    AtomicInteger callCount = new AtomicInteger(0);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      callCount.incrementAndGet();
      latch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3, 4 }));
    when(store.getCurrentVersion()).thenReturn(1);

    // Initialize
    notifier.handleStoreCreated(store);

    // Change 1 -> 2
    when(store.getCurrentVersion()).thenReturn(2);
    notifier.handleStoreChanged(store);

    // Change 2 -> 3
    when(store.getCurrentVersion()).thenReturn(3);
    notifier.handleStoreChanged(store);

    // Change 3 -> 4
    when(store.getCurrentVersion()).thenReturn(4);
    notifier.handleStoreChanged(store);

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(callCount.get(), 3);
  }

  @Test
  public void testCurrentVersionChangeWithVersionAddDelete() throws InterruptedException {
    CountDownLatch versionAddedLatch = new CountDownLatch(1);
    CountDownLatch currentVersionLatch = new CountDownLatch(1);
    AtomicInteger versionAddedCalls = new AtomicInteger(0);
    AtomicInteger currentVersionCalls = new AtomicInteger(0);
    AtomicInteger capturedAddedVersion = new AtomicInteger(-1);
    AtomicInteger capturedNewCurrentVersion = new AtomicInteger(-1);

    StoreChangeTasks tasks = StoreChangeTasks.builder().onVersionAdded((store, version) -> {
      versionAddedCalls.incrementAndGet();
      capturedAddedVersion.set(version);
      versionAddedLatch.countDown();
    }).onCurrentVersionChanged((store, newVersion, prevVersion) -> {
      currentVersionCalls.incrementAndGet();
      capturedNewCurrentVersion.set(newVersion);
      currentVersionLatch.countDown();
    }).build();

    notifier.registerTasks("TestAdapter", tasks);

    Store store = mock(Store.class);
    when(store.getName()).thenReturn("testStore");
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2 }));
    when(store.getCurrentVersion()).thenReturn(1);

    // Initialize
    notifier.handleStoreCreated(store);

    // Add version 3 and make it current
    when(store.getVersionNumbers()).thenReturn(new IntOpenHashSet(new int[] { 1, 2, 3 }));
    when(store.getCurrentVersion()).thenReturn(3);
    notifier.handleStoreChanged(store);

    assertTrue(versionAddedLatch.await(5, TimeUnit.SECONDS));
    assertTrue(currentVersionLatch.await(5, TimeUnit.SECONDS));
    assertEquals(versionAddedCalls.get(), 1);
    assertEquals(currentVersionCalls.get(), 1);
    assertEquals(capturedAddedVersion.get(), 3);
    assertEquals(capturedNewCurrentVersion.get(), 3);
  }
}
