package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.Time;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testClose() throws InterruptedException {
    int threadPoolSize = 10;
    int numRunnables = 1000;

    CloseableThreadLocal<CloseableClass> numbers = new CloseableThreadLocal<>(() -> new CloseableClass(5));
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    Set<CloseableClass> closeableObjects = ConcurrentHashMap.newKeySet();
    Set<Thread> threadIds = ConcurrentHashMap.newKeySet();
    CountDownLatch latch = new CountDownLatch(numRunnables);

    for (int i = 0; i < numRunnables; i++) {
      executorService.submit(() -> {
        closeableObjects.add(numbers.get());
        threadIds.add(Thread.currentThread());
        latch.countDown();
      });
    }
    latch.await();
    executorService.shutdownNow();

    // Assert there are separate objects created for each thread
    Assert.assertEquals(closeableObjects.size(), threadIds.size());

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
