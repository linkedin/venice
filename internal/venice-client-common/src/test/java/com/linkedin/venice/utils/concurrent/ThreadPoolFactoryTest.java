package com.linkedin.venice.utils.concurrent;

import static com.linkedin.venice.utils.concurrent.BlockingQueueType.ARRAY_BLOCKING_QUEUE;
import static com.linkedin.venice.utils.concurrent.BlockingQueueType.LINKED_BLOCKING_QUEUE;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThreadPoolFactoryTest {
  @Test
  public void testGetLinkedBlockingQueue() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(10, "ThreadPoolName1", 10, LINKED_BLOCKING_QUEUE);
    Assert.assertTrue(executor.getQueue() instanceof LinkedBlockingQueue);
    executor.shutdownNow();
  }

  @Test
  public void testGetArrayBlockingQueue() {
    ThreadPoolExecutor executor = ThreadPoolFactory.createThreadPool(10, "ThreadPoolName1", 10, ARRAY_BLOCKING_QUEUE);
    Assert.assertTrue(executor.getQueue() instanceof ArrayBlockingQueue);
    executor.shutdownNow();
  }
}
