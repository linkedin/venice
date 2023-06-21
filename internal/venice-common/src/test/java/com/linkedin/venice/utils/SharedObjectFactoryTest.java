package com.linkedin.venice.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class SharedObjectFactoryTest {
  private static final AtomicInteger SHARED_OBJECT_COUNT = new AtomicInteger();
  private static final AtomicInteger MULTIPLE_DESTRUCTION_COUNT = new AtomicInteger();

  private class TestSharedObject implements AutoCloseable {
    boolean destructorInvoked;

    TestSharedObject() {
      destructorInvoked = false;
      SHARED_OBJECT_COUNT.incrementAndGet();
    }

    @Override
    public void close() {
      if (destructorInvoked) {
        MULTIPLE_DESTRUCTION_COUNT.incrementAndGet();
      }
      destructorInvoked = true;
    }
  }

  @BeforeTest
  public void setUp() {
    SHARED_OBJECT_COUNT.set(0);
    MULTIPLE_DESTRUCTION_COUNT.set(0);
  }

  @Test
  public void testSharedObject() {
    SharedObjectFactory<TestSharedObject> factory = new SharedObjectFactory<>();

    String id1 = "id1";
    // Create a new shared object
    TestSharedObject obj1 = factory.get(id1, TestSharedObject::new, TestSharedObject::close);
    Assert.assertFalse(obj1.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 1);

    // Get the same shared object from the factory
    TestSharedObject obj2 = factory.get(id1, TestSharedObject::new, TestSharedObject::close);
    Assert.assertSame(obj1, obj2);
    Assert.assertFalse(obj1.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 2);

    Assert.assertEquals(factory.size(), 1);

    String id2 = "id2";
    // Create a new shared object with a different identifier
    TestSharedObject obj3 = factory.get(id2, TestSharedObject::new, TestSharedObject::close);
    Assert.assertFalse(obj3.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id2), 1);

    Assert.assertEquals(factory.size(), 2);

    // Release a shared object. The destructor will not be invoked since the object is shared
    Assert.assertFalse(factory.release(id1));
    Assert.assertFalse(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 1);
    Assert.assertEquals(factory.size(), 2);

    // Release the last reference of a shared object. The destructor will now be invoked
    Assert.assertTrue(factory.release(id1));
    Assert.assertTrue(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 0);
    Assert.assertEquals(factory.size(), 1);

    // Release the last reference of a shared object. The destructor will now be invoked
    Assert.assertTrue(factory.release(id2));
    Assert.assertTrue(obj2.destructorInvoked);
    Assert.assertEquals(factory.getReferenceCount(id1), 0);
    Assert.assertEquals(factory.size(), 0);

    // Calling release on a non-managed object returns true
    Assert.assertTrue(factory.release(id1));
    String id3 = "id3";
    Assert.assertTrue(factory.release(id3));
  }

  @Test
  public void testMultiThreadedSharedObject() throws InterruptedException {
    SharedObjectFactory<TestSharedObject> factory = new SharedObjectFactory<>();
    int threadPoolSize = 10;
    int numRunnables = 1000;

    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

    CountDownLatch latch1 = new CountDownLatch(numRunnables);
    String id1 = "id1";
    for (int i = 0; i < numRunnables; i++) {
      executorService.submit(() -> {
        factory.get(id1, TestSharedObject::new, TestSharedObject::close);
        latch1.countDown();
      });
    }
    latch1.await();

    Assert.assertEquals(SHARED_OBJECT_COUNT.get(), 1);
    Assert.assertEquals(factory.getReferenceCount(id1), numRunnables);
    Assert.assertEquals(factory.size(), 1);

    CountDownLatch latch2 = new CountDownLatch(numRunnables);
    String id2 = "id2";
    for (int i = 0; i < numRunnables; i++) {
      executorService.submit(() -> {
        // Get new object and close it immediately soon after
        factory.get(id2, TestSharedObject::new, TestSharedObject::close);
        factory.release(id2);
        latch2.countDown();
      });
    }
    latch2.await();
    executorService.shutdownNow();

    Assert.assertEquals(MULTIPLE_DESTRUCTION_COUNT.get(), 0);
    Assert.assertEquals(factory.getReferenceCount(id2), 0);
    Assert.assertEquals(factory.size(), 1);
  }
}
