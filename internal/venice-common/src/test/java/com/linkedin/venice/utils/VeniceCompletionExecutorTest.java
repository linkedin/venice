package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class VeniceCompletionExecutorTest {
  @Test
  public void testSharedExecutorRetainsNoCoreThread() throws Exception {
    Field executorField = VeniceCompletionExecutor.class.getDeclaredField("EXECUTOR");
    executorField.setAccessible(true);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) executorField.get(null);

    assertEquals(executor.getCorePoolSize(), 0);
    assertTrue(executor.getKeepAliveTime(TimeUnit.NANOSECONDS) > 0);
  }

  @Test
  public void testOwnedDaemonWorkersProgressConcurrently() throws Exception {
    CountDownLatch bothStarted = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<Thread> firstThread = submitBlockingCompletion(bothStarted, release);
    CompletableFuture<Thread> secondThread = submitBlockingCompletion(bothStarted, release);

    try {
      assertTrue(bothStarted.await(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
    }
    Thread first = firstThread.get(5, TimeUnit.SECONDS);
    Thread second = secondThread.get(5, TimeUnit.SECONDS);
    assertNotSame(first, second);
    assertTrue(first.isDaemon());
    assertTrue(second.isDaemon());
    assertTrue(first.getName().startsWith("venice-completion-handoff-t"));
    assertTrue(second.getName().startsWith("venice-completion-handoff-t"));
  }

  private static CompletableFuture<Thread> submitBlockingCompletion(
      CountDownLatch bothStarted,
      CountDownLatch release) {
    CompletableFuture<Thread> thread = new CompletableFuture<>();
    VeniceCompletionExecutor.execute(() -> {
      bothStarted.countDown();
      await(release);
      thread.complete(Thread.currentThread());
    });
    return thread;
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while holding a test completion", exception);
    }
  }
}
