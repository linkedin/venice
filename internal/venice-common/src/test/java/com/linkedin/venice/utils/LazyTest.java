package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LazyTest {
  @Test(timeOut = 5 * Time.MS_PER_SECOND)
  public void test() throws ExecutionException, InterruptedException {
    int numberOfFutures = 10;
    ExecutorService executor = null;
    try {
      executor = Executors.newFixedThreadPool(numberOfFutures);

      /** We create a {@link Supplier} that can reliably reproduce a race condition and detect the number of invocations. */
      final Object lock = new Object();
      final AtomicInteger startedInvocationCount = new AtomicInteger(0);
      final AtomicInteger finishedInvocationCount = new AtomicInteger(0);
      Supplier<Object> testSupplier = () -> {
        startedInvocationCount.incrementAndGet();
        synchronized (lock) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            throw new VeniceException(e);
          }
        }
        finishedInvocationCount.incrementAndGet();
        return new Object();
      };

      /** We verify whether the custom {@link Supplier} does indeed detect concurrent invocations. */
      CompletableFuture<Object>[] futures = new CompletableFuture[numberOfFutures];
      for (int f = 0; f < numberOfFutures; f++) {
        futures[f] = CompletableFuture.supplyAsync(() -> testSupplier.get(), executor);
      }
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(
              startedInvocationCount.get(),
              numberOfFutures,
              "The testSuppliers should have all started running."));

      Assert.assertEquals(finishedInvocationCount.get(), 0, "The testSuppliers should not have finished running yet.");

      for (int f = 0; f < numberOfFutures; f++) {
        synchronized (lock) {
          lock.notify();
        }
      }
      Object[] supplierResults = new Object[numberOfFutures];
      for (int f = 0; f < numberOfFutures; f++) {
        supplierResults[f] = futures[f].get();
      }
      Assert.assertEquals(
          finishedInvocationCount.get(),
          numberOfFutures,
          "The testSupplier did not successfully keep track of parallel executions.");
      for (int f1 = 0; f1 < numberOfFutures; f1++) {
        for (int f2 = 0; f2 < numberOfFutures; f2++) {
          if (f1 != f2) {
            Assert.assertNotEquals(
                supplierResults[f1],
                supplierResults[f2],
                "The identity of the returned objects should all be different.");
          }
        }
      }

      /** Reset the state */
      startedInvocationCount.set(0);
      finishedInvocationCount.set(0);

      /** Main code under test: does Lazy successfully prevent the race condition? */
      Lazy<Object> lazyObject = Lazy.of(testSupplier);
      CompletableFuture<Object>[] lazyInvocations = new CompletableFuture[numberOfFutures];
      for (int f = 0; f < numberOfFutures; f++) {
        lazyInvocations[f] = CompletableFuture.supplyAsync(() -> lazyObject.get(), executor);
      }
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> Assert
              .assertEquals(startedInvocationCount.get(), 1, "A single testSupplier should have started running."));
      Assert.assertEquals(finishedInvocationCount.get(), 0, "The testSupplier should not have finished running yet.");
      Assert.assertFalse(lazyObject.isPresent(), "The lazyObject should not be present yet.");
      AtomicBoolean isPresent = new AtomicBoolean(false);
      lazyObject.ifPresent(o -> isPresent.set(true));
      Assert.assertFalse(isPresent.get(), "The lazyObject should not be present yet.");
      Optional<SomeWrapper> someWrapperOptional = lazyObject.map(o -> new SomeWrapper(o));
      Assert.assertFalse(someWrapperOptional.isPresent(), "The lazyObject should not be present yet.");

      synchronized (lock) {
        lock.notify();
      }
      Object[] lazyInvocationResults = new Object[numberOfFutures];
      for (int f = 0; f < numberOfFutures; f++) {
        lazyInvocationResults[f] = lazyInvocations[f].get();
      }
      Assert.assertEquals(
          startedInvocationCount.get(),
          1,
          "Lazy did not ensure exactly one run of the supplier. startedInvocationCount: " + startedInvocationCount);
      Assert.assertEquals(
          finishedInvocationCount.get(),
          1,
          "Lazy did not ensure exactly one run of the supplier. finishedInvocationCount: " + finishedInvocationCount);
      Assert.assertTrue(lazyObject.isPresent(), "The lazyObject should now be present.");
      lazyObject.ifPresent(o -> isPresent.set(true));
      Assert.assertTrue(isPresent.get(), "The lazyObject should now be present.");
      someWrapperOptional = lazyObject.map(o -> new SomeWrapper(o));
      Assert.assertTrue(someWrapperOptional.isPresent(), "The lazyObject should now be present.");

      for (int f1 = 0; f1 < numberOfFutures; f1++) {
        for (int f2 = 0; f2 < numberOfFutures; f2++) {
          Assert.assertEquals(
              lazyInvocationResults[f1],
              lazyInvocationResults[f2],
              "The identity of the returned objects should all be the same.");
        }
        Assert.assertEquals(
            someWrapperOptional.get().getStuff(),
            lazyInvocationResults[f1],
            "The identity of the returned objects should all be the same.");
      }
    } finally {
      TestUtils.shutdownExecutor(executor);
    }
  }

  @Test
  public void testNullSupplier() {
    Lazy<Object> lazyObject = Lazy.of(() -> null);
    Assert.assertFalse(lazyObject.isPresent(), "The lazyObject should not be present yet.");
    AtomicBoolean isPresent = new AtomicBoolean(false);
    lazyObject.ifPresent(o -> isPresent.set(true));
    Assert.assertFalse(isPresent.get(), "The lazyObject should not be present yet.");
    Optional<SomeWrapper> someWrapperOptional = lazyObject.map(o -> new SomeWrapper(o));
    Assert.assertFalse(someWrapperOptional.isPresent(), "The lazyObject should not be present yet.");

    // Trigger init
    Assert.assertNull(lazyObject.get(), "The lazyObject should contain null.");

    Assert.assertTrue(lazyObject.isPresent(), "The lazyObject should now be present.");
    lazyObject.ifPresent(o -> isPresent.set(true));
    Assert.assertTrue(isPresent.get(), "The lazyObject should now be present.");
    someWrapperOptional = lazyObject.map(o -> new SomeWrapper(o));
    Assert.assertTrue(someWrapperOptional.isPresent(), "The lazyObject should now be present.");
    Assert.assertNull(someWrapperOptional.get().getStuff(), "The lazyObject should contain null.");
  }

  private static class SomeWrapper<T> {
    private final T stuff;

    SomeWrapper(T stuff) {
      this.stuff = stuff;
    }

    public T getStuff() {
      return stuff;
    }
  }
}
