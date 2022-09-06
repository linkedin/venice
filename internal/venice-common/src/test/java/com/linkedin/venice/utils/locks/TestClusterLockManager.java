package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.Time;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLockManager {
  private ClusterLockManager clusterLockManager;

  @BeforeClass
  public void setUp() {
    clusterLockManager = new ClusterLockManager("cluster");
  }

  /**
   * N.B. Invocation count > 1 ensures that the locks are reusable, and thus closed properly after their first usage.
   */
  @Test(invocationCount = 5, timeOut = 5 * Time.MS_PER_SECOND)
  public void testStoreWriteLock() throws Exception {
    final AtomicInteger value = new AtomicInteger(0);
    final int VALUE_WRITTEN_BY_THREAD_1 = 1;
    final int VALUE_WRITTEN_BY_THREAD_2 = 2;

    CountDownLatch thread2StartedLatch = new CountDownLatch(1);
    Thread thread2 = new Thread(() -> {
      thread2StartedLatch.countDown();
      try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
        value.set(VALUE_WRITTEN_BY_THREAD_2);
      }
    });

    try (AutoCloseableLock ignore1 = clusterLockManager.createStoreWriteLock("store")) {
      value.set(VALUE_WRITTEN_BY_THREAD_1);
      thread2.start();
      Assert.assertTrue(
          thread2StartedLatch.await(1, TimeUnit.SECONDS),
          "thread2 must have started for the test to proceed.");
      Assert.assertEquals(
          value.get(),
          VALUE_WRITTEN_BY_THREAD_1,
          "Store write lock is acquired by a thread1. Value could not be updated by thread2.");
    } finally {
      thread2.join();
    }
    Assert.assertEquals(
        value.get(),
        VALUE_WRITTEN_BY_THREAD_2,
        "Thread2 should already acquire the lock and modify tne value.");
  }

  /**
   * N.B. Invocation count > 1 ensures that the locks are reusable, and thus closed properly after their first usage.
   */
  @Test(invocationCount = 5, timeOut = 5 * Time.MS_PER_SECOND)
  public void testClusterAndStoreLock() throws Exception {
    final AtomicInteger value = new AtomicInteger(0);
    final int VALUE_WRITTEN_BY_THREAD_1 = 1;
    final int VALUE_WRITTEN_BY_THREAD_2 = 2;

    CountDownLatch thread2StartedLatch = new CountDownLatch(1);
    Thread thread2 = new Thread(() -> {
      thread2StartedLatch.countDown();
      try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
        value.set(VALUE_WRITTEN_BY_THREAD_2);
      }
    });

    try (AutoCloseableLock ignore1 = clusterLockManager.createClusterWriteLock()) {
      value.set(VALUE_WRITTEN_BY_THREAD_1);
      thread2.start();
      Assert.assertTrue(
          thread2StartedLatch.await(1, TimeUnit.SECONDS),
          "thread2 must have started for the test to proceed.");
      Assert.assertEquals(
          value.get(),
          VALUE_WRITTEN_BY_THREAD_1,
          "Cluster write lock is acquired by a thread1. Value could not be updated by thread2.");
    } finally {
      thread2.join();
    }
    Assert.assertEquals(
        value.get(),
        VALUE_WRITTEN_BY_THREAD_2,
        "Thread2 should already acquire the lock and modify tne value.");
  }
}
