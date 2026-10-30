package com.linkedin.venice.utils.collections;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MemoryBoundBlockingQueueTest {
  @DataProvider
  public Object[][] admissionEpisodes() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "admissionEpisodes")
  public void testRecoveryRejectsEnqueuesFromFailedEpisode(boolean beginsWhileFailed) throws Exception {
    MemoryBoundBlockingQueue<Measurable> queue = new MemoryBoundBlockingQueue<>(1000, 100);
    VeniceException failure = new VeniceException("test failed episode");
    if (beginsWhileFailed) {
      queue.fail(failure);
    }
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> oldPut = executor.submit(() -> {
        queue.put(() -> {
          entered.countDown();
          try {
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VeniceException(e);
          }
          return MeasurableObject.SIZE;
        });
        return null;
      });
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      if (!beginsWhileFailed) {
        queue.fail(failure);
      }
      queue.recover();
      MeasurableObject fresh = new MeasurableObject();
      queue.put(fresh);
      release.countDown();
      Assert.assertSame(
          Assert.expectThrows(ExecutionException.class, () -> oldPut.get(5, TimeUnit.SECONDS)).getCause(),
          failure);
      Assert.assertSame(queue.take(), fresh);
      queue.recover();
      queue.put(fresh);
      VeniceException secondFailure = new VeniceException("second test episode");
      Assert.assertEquals(queue.fail(secondFailure).size(), 1);
      Assert.assertSame(Assert.expectThrows(VeniceException.class, () -> queue.put(fresh)), secondFailure);
      queue.recover();
      queue.put(fresh);
      Assert.assertSame(queue.take(), fresh);
    } finally {
      release.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFailureClearsQueueAndReleasesAllBlockedPuts() throws Exception {
    int recordSize = MeasurableObject.SIZE + MemoryBoundBlockingQueue.LINKED_LIST_NODE_SHALLOW_OVERHEAD;
    MemoryBoundBlockingQueue<MeasurableObject> queue = new MemoryBoundBlockingQueue<>(recordSize * 4, recordSize * 2);
    List<MeasurableObject> queued = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      MeasurableObject record = new MeasurableObject();
      queued.add(record);
      queue.put(record);
    }
    Assert.assertSame(queue.take(), queued.remove(0));
    Assert.assertEquals(queue.remainingMemoryCapacityInByte(), 0L, "Sub-delta freed memory is still reserved");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<Future<?>> puts = new ArrayList<>();
    List<AtomicReference<Thread>> threads = new ArrayList<>();
    try {
      for (int i = 0; i < 2; i++) {
        AtomicReference<Thread> thread = new AtomicReference<>();
        threads.add(thread);
        puts.add(executor.submit(() -> {
          thread.set(Thread.currentThread());
          queue.put(new MeasurableObject());
          return null;
        }));
      }
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        for (AtomicReference<Thread> thread: threads) {
          Assert.assertNotNull(thread.get());
          Assert.assertEquals(thread.get().getState(), Thread.State.WAITING);
        }
      });
      VeniceException failure = new VeniceException("test queue failure");
      Assert.assertEquals(queue.fail(failure), queued);
      for (Future<?> put: puts) {
        ExecutionException error = Assert.expectThrows(ExecutionException.class, () -> put.get(5, TimeUnit.SECONDS));
        Assert.assertSame(error.getCause(), failure);
      }
      Assert.assertTrue(queue.isEmpty());
      Assert.assertEquals(queue.getMemoryUsage(), 0L);
      Assert.assertEquals(queue.remainingMemoryCapacityInByte(), (long) recordSize * 4);
      Assert.assertTrue(queue.fail(new VeniceException("second failure")).isEmpty());
      Assert.assertSame(Assert.expectThrows(VeniceException.class, () -> queue.put(new MeasurableObject())), failure);
      Assert.assertSame(Assert.expectThrows(VeniceException.class, queue::take), failure);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFailureReleasesBlockedTake() throws Exception {
    MemoryBoundBlockingQueue<MeasurableObject> queue = new MemoryBoundBlockingQueue<>(1000, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> thread = new AtomicReference<>();
    try {
      Future<?> take = executor.submit(() -> {
        thread.set(Thread.currentThread());
        return queue.take();
      });
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertNotNull(thread.get());
        Assert.assertEquals(thread.get().getState(), Thread.State.WAITING);
      });
      Assert.expectThrows(NullPointerException.class, () -> queue.fail(null));
      VeniceException failure = new VeniceException("test empty queue failure");
      Assert.assertTrue(queue.fail(failure).isEmpty());
      Assert.assertSame(
          Assert.expectThrows(ExecutionException.class, () -> take.get(5, TimeUnit.SECONDS)).getCause(),
          failure);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static class MeasurableObject implements Measurable {
    public static final int SIZE = 10;

    @Override
    public int getHeapSize() {
      return SIZE;
    }
  }

  @Test
  public void testPut() throws InterruptedException {
    int memoryCap = 5000;
    MemoryBoundBlockingQueue<MeasurableObject> queue = new MemoryBoundBlockingQueue<>(memoryCap, 1000);
    int objectCntAtMost =
        memoryCap / (MemoryBoundBlockingQueue.LINKED_LIST_NODE_SHALLOW_OVERHEAD + MeasurableObject.SIZE);
    Thread t = new Thread(() -> {
      while (true) {
        try {
          queue.put(new MeasurableObject());
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    t.start();
    try {
      Thread.sleep(50);
      Assert.assertTrue(t.isAlive());
      Assert.assertEquals(queue.size(), objectCntAtMost);
    } finally {
      TestUtils.shutdownThread(t);
    }
  }

  @Test
  public void testTake() throws InterruptedException {
    int memoryCap = 5000;
    MemoryBoundBlockingQueue<MeasurableObject> queue = new MemoryBoundBlockingQueue<>(memoryCap, 1000);
    int objectCntAtMost =
        memoryCap / (MemoryBoundBlockingQueue.LINKED_LIST_NODE_SHALLOW_OVERHEAD + MeasurableObject.SIZE);
    for (int i = 0; i < objectCntAtMost; ++i) {
      queue.put(new MeasurableObject());
    }
    AtomicInteger objectTakenNum = new AtomicInteger(0);
    Thread t = new Thread(() -> {
      while (true) {
        try {
          queue.take();
          objectTakenNum.addAndGet(1);
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    t.start();

    try {
      Thread.sleep(50);
      Assert.assertTrue(t.isAlive());
      Assert.assertEquals(objectTakenNum.get(), objectCntAtMost);
      Assert.assertEquals(queue.size(), 0);
    } finally {
      TestUtils.shutdownThread(t);
    }
  }

  @Test
  public void testThrottling() throws InterruptedException {
    int memoryCap = 5000;
    int notifyDelta = 1000;
    MemoryBoundBlockingQueue<MeasurableObject> queue = new MemoryBoundBlockingQueue<>(memoryCap, notifyDelta);
    int objectCntAtMost =
        memoryCap / (MemoryBoundBlockingQueue.LINKED_LIST_NODE_SHALLOW_OVERHEAD + MeasurableObject.SIZE);
    Thread t = new Thread(() -> {
      while (true) {
        try {
          queue.put(new MeasurableObject());
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    t.start();

    try {
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(t.isAlive());
        Assert.assertEquals(queue.size(), objectCntAtMost);
      });

      int previousQueueSize = queue.size();
      // Here we need to take out some objects to allow more put
      double objectCntTakenAtLeast = Math.ceil(
          (double) notifyDelta / (MemoryBoundBlockingQueue.LINKED_LIST_NODE_SHALLOW_OVERHEAD + MeasurableObject.SIZE));
      for (int i = 1; i < objectCntTakenAtLeast; ++i) {
        queue.take();
        Assert.assertEquals(queue.size(), previousQueueSize - 1);
        --previousQueueSize;
      }
      // This will trigger a notification, which will allow more puts
      queue.take();

      TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(t.isAlive());
        Assert.assertEquals(queue.size(), objectCntAtMost);
      });
    } finally {
      TestUtils.shutdownThread(t);
    }
  }
}
