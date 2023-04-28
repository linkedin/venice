package com.linkedin.venice.utils.concurrent;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class ConcurrencyUtilsTest {
  @Test
  public void testExecuteUnderConditionalLock() throws InterruptedException {
    int numRunnables = 1000;
    AtomicReference<Boolean> globalState = new AtomicReference<>(true);

    Runnable action = Mockito.mock(Runnable.class);
    Mockito.doAnswer(invocation -> {
      globalState.set(false);
      return null;
    }).when(action).run();

    BooleanSupplier lockConditionOnGlobalState = Mockito.mock(BooleanSupplier.class);
    doAnswer(invocation -> globalState.get()).when(lockConditionOnGlobalState).getAsBoolean();

    runTest(numRunnables, action, lockConditionOnGlobalState);

    Mockito.verify(action, times(1)).run();
    Mockito.verify(lockConditionOnGlobalState, atLeast(numRunnables + 1)).getAsBoolean();
    Mockito.verify(lockConditionOnGlobalState, atMost(2 * numRunnables)).getAsBoolean();

    // Lock will always return false now. The action should never get executed and the condition should only be checked
    // in the outer block
    Mockito.reset(action);
    Mockito.reset(lockConditionOnGlobalState);

    runTest(numRunnables, action, lockConditionOnGlobalState);
    Mockito.verify(action, never()).run();
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
}
