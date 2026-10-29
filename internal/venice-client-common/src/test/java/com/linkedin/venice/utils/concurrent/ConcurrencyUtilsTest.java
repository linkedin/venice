package com.linkedin.venice.utils.concurrent;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConcurrencyUtilsTest {
  @Test
  public void testGetOrRethrowCause() {
    Assert.assertEquals(
        ConcurrencyUtils.getOrRethrowCause(CompletableFuture.completedFuture("value"), "unused"),
        "value");

    // A RuntimeException or an Error is rethrown as is.
    IllegalStateException runtimeFailure = new IllegalStateException("runtime failure");
    CompletableFuture<String> runtimeFailed = new CompletableFuture<>();
    runtimeFailed.completeExceptionally(runtimeFailure);
    Assert.assertSame(
        Assert.expectThrows(
            IllegalStateException.class,
            () -> ConcurrencyUtils.getOrRethrowCause(runtimeFailed, "unused")),
        runtimeFailure);

    AssertionError errorFailure = new AssertionError("error failure");
    CompletableFuture<String> errorFailed = new CompletableFuture<>();
    errorFailed.completeExceptionally(errorFailure);
    Assert.assertSame(
        Assert.expectThrows(AssertionError.class, () -> ConcurrencyUtils.getOrRethrowCause(errorFailed, "unused")),
        errorFailure);

    // Any other exception is wrapped in a VeniceException.
    IOException checkedFailure = new IOException("checked failure");
    CompletableFuture<String> checkedFailed = new CompletableFuture<>();
    checkedFailed.completeExceptionally(checkedFailure);
    VeniceException wrapped =
        Assert.expectThrows(VeniceException.class, () -> ConcurrencyUtils.getOrRethrowCause(checkedFailed, "unused"));
    Assert.assertSame(wrapped.getCause(), checkedFailure);
  }

  @Test
  public void testGetOrRethrowCauseWhenInterrupted() {
    CompletableFuture<String> neverCompleted = new CompletableFuture<>();
    Thread.currentThread().interrupt();
    try {
      VeniceException thrown = Assert.expectThrows(
          VeniceException.class,
          () -> ConcurrencyUtils.getOrRethrowCause(neverCompleted, "Interrupted while waiting"));
      Assert.assertEquals(thrown.getMessage(), "Interrupted while waiting");
      Assert.assertTrue(thrown.getCause() instanceof InterruptedException);
      // The interrupt status is kept.
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testExecuteUnderConditionalLock() throws InterruptedException {
    int numRunnables = 1000;
    AtomicInteger actionCount = new AtomicInteger();

    Runnable action = actionCount::incrementAndGet;

    BooleanSupplier lockConditionOnGlobalState = Mockito.mock(BooleanSupplier.class);
    doAnswer(invocation -> actionCount.get() == 0).when(lockConditionOnGlobalState).getAsBoolean();

    runTest(numRunnables, action, lockConditionOnGlobalState);

    Assert.assertEquals(actionCount.get(), 1);
    Mockito.verify(lockConditionOnGlobalState, atLeast(numRunnables + 1)).getAsBoolean();
    Mockito.verify(lockConditionOnGlobalState, atMost(2 * numRunnables)).getAsBoolean();

    // Lock will always return false now. The action should never get executed and the condition should only be checked
    // in the outer block
    Mockito.reset(lockConditionOnGlobalState);

    runTest(numRunnables, action, lockConditionOnGlobalState);
    Assert.assertEquals(actionCount.get(), 1);
    Mockito.verify(lockConditionOnGlobalState, times(numRunnables)).getAsBoolean();
  }

  @Test
  public void testExecuteUnderConditionalLockWithElse() throws InterruptedException {
    int numRunnables = 1000;
    AtomicInteger actionCount = new AtomicInteger();
    AtomicInteger orElseActionCount = new AtomicInteger();

    Runnable action = actionCount::incrementAndGet;
    Runnable orElseAction = orElseActionCount::incrementAndGet;

    BooleanSupplier lockConditionOnGlobalState = Mockito.mock(BooleanSupplier.class);
    doAnswer(invocation -> actionCount.get() == 0).when(lockConditionOnGlobalState).getAsBoolean();

    runTestWithOrElseAction(numRunnables, action, orElseAction, lockConditionOnGlobalState);

    Assert.assertEquals(actionCount.get(), 1);
    Assert.assertEquals(orElseActionCount.get(), numRunnables - 1);
    Mockito.verify(lockConditionOnGlobalState, times(numRunnables)).getAsBoolean();

    // Lock will always return false now. The action should never get executed but the condition should still be checked
    Mockito.reset(lockConditionOnGlobalState);

    runTestWithOrElseAction(numRunnables, action, orElseAction, lockConditionOnGlobalState);
    Assert.assertEquals(actionCount.get(), 1);
    Assert.assertEquals(orElseActionCount.get(), 2 * numRunnables - 1);
    Mockito.verify(lockConditionOnGlobalState, times(numRunnables)).getAsBoolean();
  }

  private void runTest(int numRunnables, Runnable action, BooleanSupplier lockCondition) throws InterruptedException {
    int threadPoolSize = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    CountDownLatch latch = new CountDownLatch(numRunnables);
    Object lockObject = new Object();
    for (int i = 0; i < numRunnables; i++) {
      executorService.submit(() -> {
        ConcurrencyUtils.executeUnderConditionalLock(action, lockCondition, lockObject);
        latch.countDown();
      });
    }
    latch.await();
    executorService.shutdownNow();
  }

  private void runTestWithOrElseAction(
      int numRunnables,
      Runnable action,
      Runnable orElseAction,
      BooleanSupplier lockCondition) throws InterruptedException {
    int threadPoolSize = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    CountDownLatch latch = new CountDownLatch(numRunnables);
    Object lockObject = new Object();
    for (int i = 0; i < numRunnables; i++) {
      executorService.submit(() -> {
        ConcurrencyUtils.executeUnderLock(action, orElseAction, lockCondition, lockObject);
        latch.countDown();
      });
    }
    latch.await();
    executorService.shutdownNow();
  }
}
