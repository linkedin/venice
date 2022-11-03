package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.TestUtils;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CloseableThreadLocalTest {
  private static class CloseableClass implements AutoCloseable {
    private boolean closed = false;
    private int value;

    public CloseableClass(int value) {
      this.value = value;
    }

    @Override
    public void close() throws Exception {
      closed = true;
    }
  }

  @Test
  public void testGet() {
    try (CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> new CloseableClass(5))) {
      Assert.assertEquals(numbers.get().value, 5);
    }
  }

  @Test
  public void testRemove() {
    CloseableClass initialValue;
    try (CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> new CloseableClass(5))) {
      initialValue = numbers.get();
      Assert.assertEquals(initialValue.value, 5); // Initialize the value
      numbers.remove();
      Assert.assertTrue(initialValue.closed);
      // Override closed to test if the object gets closed a second time
      initialValue.closed = false;
    }

    // A previously removed object must not get closed a second time. If it does, it indicates a memory leak.
    Assert.assertFalse(initialValue.closed);
  }

  @Test
  public void testSet() {
    AtomicBoolean initializerCalled = new AtomicBoolean(false);
    try (CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> {
      initializerCalled.set(true);
      return new CloseableClass(5);
    })) {
      numbers.set(new CloseableClass(7));
      CloseableClass initialValue = numbers.get();
      Assert.assertEquals(initialValue.value, 7);
    }

    // The initializer must never be invoked
    Assert.assertFalse(initializerCalled.get());
  }

  @Test
  public void testSetClosesPreviousValue() {
    try (CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> new CloseableClass(5))) {
      numbers.set(new CloseableClass(7));
      CloseableClass initialValue = numbers.get();
      numbers.set(new CloseableClass(8));
      Assert.assertTrue(initialValue.closed);
      Assert.assertEquals(numbers.get().value, 8);
    }
  }

  @Test
  public void testClose() {
    CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> new CloseableClass(5));
    Set<CloseableClass> closeableObjects = new HashSet<>();
    int threadPoolSize = 10;
    int numRunnables = 1000;
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    try {
      for (int i = 0; i < numRunnables; i++) {
        executorService.submit(() -> {
          closeableObjects.add(numbers.get());
        });
      }
    } finally {
      executorService.shutdown();
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      try {
        executorService.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Assert.fail();
      }
    });

    // Assert there are separate objects created for each thread
    Assert.assertEquals(closeableObjects.size(), threadPoolSize);

    // Verify that the objects have not been closed yet
    for (CloseableClass object: closeableObjects) {
      Assert.assertFalse(object.closed);
    }

    numbers.close();

    // Verify that the objects have now been closed
    for (CloseableClass object: closeableObjects) {
      Assert.assertTrue(object.closed);
    }
  }
}
