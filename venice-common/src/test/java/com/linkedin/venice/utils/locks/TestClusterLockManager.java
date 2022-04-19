package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  public void testStoreWriteLock() {
    final AtomicInteger value = new AtomicInteger(0);
    final AtomicBoolean thread2HasStarted = new AtomicBoolean(false);
    final int VALUE_WRITTEN_BY_THREAD_1 = 1;
    final int VALUE_WRITTEN_BY_THREAD_2 = 2;
    try (AutoCloseableLock ignore1 = clusterLockManager.createStoreWriteLock("store")) {
      value.set(VALUE_WRITTEN_BY_THREAD_1);
      new Thread(() -> {
        thread2HasStarted.set(true);
        try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
          value.set(VALUE_WRITTEN_BY_THREAD_2);
        }
      }).start();

      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () ->
          Assert.assertTrue(thread2HasStarted.get(), "thread2 must have started for the test to proceed."));
      Assert.assertEquals(value.get(), VALUE_WRITTEN_BY_THREAD_1,
          "Store write lock is acquired by a thread1. Value could not be updated by thread2.");
    }
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () ->
        Assert.assertEquals(value.get(), VALUE_WRITTEN_BY_THREAD_2,
            "Thread2 should already acquire the lock and modify tne value."));
  }

  /**
   * N.B. Invocation count > 1 ensures that the locks are reusable, and thus closed properly after their first usage.
   */
  @Test(invocationCount = 5, timeOut = 5 * Time.MS_PER_SECOND)
  public void testClusterAndStoreLock() {
    final AtomicInteger value = new AtomicInteger(0);
    final AtomicBoolean thread2HasStarted = new AtomicBoolean(false);
    final int VALUE_WRITTEN_BY_THREAD_1 = 1;
    final int VALUE_WRITTEN_BY_THREAD_2 = 2;
    try (AutoCloseableLock ignore1 = clusterLockManager.createClusterWriteLock()) {
      value.set(VALUE_WRITTEN_BY_THREAD_1);
      new Thread(() -> {
        thread2HasStarted.set(true);
        try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
          value.set(VALUE_WRITTEN_BY_THREAD_2);
        }
      }).start();

      TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () ->
          Assert.assertTrue(thread2HasStarted.get(), "thread2 must have started for the test to proceed."));
      Assert.assertEquals(value.get(), VALUE_WRITTEN_BY_THREAD_1 ,
          "Cluster write lock is acquired by a thread1. Value could not be updated by thread2.");
    }
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () ->
        Assert.assertEquals(value.get(), VALUE_WRITTEN_BY_THREAD_2 ,
            "Thread2 should already acquire the lock and modify tne value."));
  }
}
