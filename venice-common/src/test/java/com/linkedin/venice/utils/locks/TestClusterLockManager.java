package com.linkedin.venice.utils.locks;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLockManager {
  private ClusterLockManager clusterLockManager;

  @BeforeClass
  public void setUp() {
    clusterLockManager = new ClusterLockManager("cluster");
  }

  @Test
  public void testStoreWriteLock() throws InterruptedException {
    int[] value = new int[]{0};
    try (AutoCloseableLock ignore1 = clusterLockManager.createStoreWriteLock("store")) {
      value[0] = 1;
      new Thread(() -> {
        try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
          value[0] = 2;
        }
      }).start();

      Thread.sleep(500);
      Assert.assertEquals(value[0], 1 , "Store write lock is acquired by a thread1. Value could not be updated by thread2.");
    }
    Thread.sleep(500);
    Assert.assertEquals(value[0], 2 , "Thread2 should already acquire the lock and modify tne value.");
  }

  @Test
  public void testClusterAndStoreLock() throws InterruptedException {
    int[] value = new int[]{0};
    try (AutoCloseableLock ignore1 = clusterLockManager.createClusterWriteLock()) {
      value[0] = 1;
      new Thread(() -> {
        try (AutoCloseableLock ignore2 = clusterLockManager.createStoreWriteLock("store")) {
          value[0] = 2;
        }
      }).start();

      Thread.sleep(500);
      Assert.assertEquals(value[0], 1 , "Cluster write lock is acquired by a thread1. Value could not be updated by thread2.");
    }
    Thread.sleep(500);
    Assert.assertEquals(value[0], 2 , "Thread2 should already acquire the lock and modify tne value.");
  }
}
