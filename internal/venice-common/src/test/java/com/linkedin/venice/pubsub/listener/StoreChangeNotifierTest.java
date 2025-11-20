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


public class StoreChangeNotifierTest {
  private StoreChangeNotifier notifier;

  @BeforeMethod
  public void setUp() {
    notifier = new StoreChangeNotifier(VeniceComponent.SERVER, LogContext.EMPTY, 2);
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
        () -> new StoreChangeNotifier(VeniceComponent.SERVER, LogContext.EMPTY, 0));
  }

  @Test
  public void testConstructorWithNullLogContext() {
    expectThrows(IllegalArgumentException.class, () -> new StoreChangeNotifier(VeniceComponent.SERVER, null, 2));
  }

  @Test
  public void testConstructorWithNullVeniceComponent() {
    expectThrows(IllegalArgumentException.class, () -> new StoreChangeNotifier(null, LogContext.EMPTY, 2));
  }
}
