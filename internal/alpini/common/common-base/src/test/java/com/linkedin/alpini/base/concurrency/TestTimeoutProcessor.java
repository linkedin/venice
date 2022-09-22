package com.linkedin.alpini.base.concurrency;

import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class TestTimeoutProcessor {
  private static final Logger LOG = LogManager.getLogger(TestTimeoutProcessor.class);

  TimeoutProcessor _timeout;

  @DataProvider(name = "TimeoutProcessor")
  // testing skipList and treeMap
  public static Object[] processor() {
    return new Object[] { new TimeoutProcessor(null, 300, TimeoutProcessor.EventStore.SkipList, 1),
        new TimeoutProcessor(null, 300, TimeoutProcessor.EventStore.TreeMap, 1) };
  }

  @AfterMethod(groups = { "unit" })
  protected void finiProcessor() throws InterruptedException {
    _timeout.shutdownNow();
    _timeout.awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testOneFire(TimeoutProcessor processor) throws InterruptedException {

    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);

    TimeoutProcessor.TimeoutFuture future = _timeout.schedule(new Runnable() {
      @Override
      public void run() {
        LOG.debug("future is running.");
        count.incrementAndGet();
      }

    }, 1, TimeUnit.SECONDS);

    Thread.sleep(900L);

    assertEquals(count.get(), 0);

    Thread.sleep(700L);

    assertEquals(count.get(), 1);
    assertTrue(future.isDone());

    Thread.sleep(100L);

    assertEquals(count.get(), 1);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testOneCancel(TimeoutProcessor processor) throws InterruptedException {

    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);

    TimeoutProcessor.TimeoutFuture future = _timeout.schedule(new Runnable() {
      @Override
      public void run() {
        count.incrementAndGet();
      }
    }, 1, TimeUnit.SECONDS);

    Thread.sleep(900L);

    assertEquals(count.get(), 0);
    future.cancel();

    Thread.sleep(200L);

    assertTrue(future.isDone());
    assertEquals(count.get(), 0);

    Thread.sleep(100L);

    assertEquals(count.get(), 0);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testStormCancel(TimeoutProcessor processor) throws InterruptedException {

    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);
    TimeoutProcessor.TimeoutFuture[] futures = new TimeoutProcessor.TimeoutFuture[1000];

    for (int i = 0, j = 0; i < 100000; i++, j++) {
      if (j >= futures.length)
        j = 0;

      if (futures[j] != null) {
        futures[j].cancel();
      }

      futures[j] = _timeout.schedule(new Runnable() {
        @Override
        public void run() {
          count.incrementAndGet();
        }
      }, 1, TimeUnit.SECONDS);
    }

    assertEquals(count.get(), 0);

    for (int j = 0; j < futures.length; j++) {
      futures[j].cancel();
    }

    Thread.sleep(900L);

    assertEquals(count.get(), 0);

    Thread.sleep(900L);

    assertEquals(count.get(), 0);

    for (int j = 0; j < futures.length; j++) {
      assertTrue(futures[j].isDone());
    }
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testStormFib(TimeoutProcessor processor) throws InterruptedException {

    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);
    TimeoutProcessor.TimeoutFuture[] futures = new TimeoutProcessor.TimeoutFuture[1000];
    int test = 0, l1 = 1, l2 = 0;
    int primed = 0, cancelled = 0;
    int limit = 100000;

    for (int i = 0, j = 0; i < limit + futures.length; i++, j++) {
      if (j >= futures.length)
        j = 0;

      if (futures[j] != null) {
        if (++test == l1 + l2) {
          if (futures[j].cancel())
            cancelled++;
          l2 = l1;
          l1 = test;
        }
      }

      if (i < limit) {
        futures[j] = _timeout.schedule(new Runnable() {
          @Override
          public void run() {
            // System.out.println("Incrementing: " + count.get());
            count.incrementAndGet();
          }
        }, 1, TimeUnit.SECONDS);
        primed++;
      }
    }

    assertEquals(count.get(), 0);

    LOG.debug("Created {} timeouts, cancelled {} of them", primed, cancelled);

    waitForStorm(count, primed - cancelled);

    assertEquals(count.get(), primed - cancelled);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testStormFib2(TimeoutProcessor processor) throws InterruptedException {

    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);
    TimeoutProcessor.TimeoutFuture[] futures = new TimeoutProcessor.TimeoutFuture[1000];
    int test = 0, l1 = 1, l2 = 0;
    int primed = 0, cancelled = 0;
    int limit = 100000;

    for (int i = 0, j = 0; i < limit + futures.length; i++, j++) {
      if (j >= futures.length)
        j = 0;

      if (futures[j] != null) {
        if (++test == l1 + l2) {
          l2 = l1;
          l1 = test;
        } else {
          if (futures[j].cancel())
            cancelled++;
        }
      }

      if (i < limit) {
        futures[j] = _timeout.schedule(new Runnable() {
          @Override
          public void run() {
            count.incrementAndGet();
          }
        }, 1, TimeUnit.SECONDS);
        primed++;
      }
    }

    assertEquals(count.get(), 0);

    LOG.info(String.format("Created %d timeouts, cancelled %d of them", primed, cancelled));

    waitForStorm(count, primed - cancelled);

    assertEquals(count.get(), primed - cancelled);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor")
  public void testStormFire(TimeoutProcessor processor) throws InterruptedException {
    _timeout = processor;
    final AtomicInteger count = new AtomicInteger(0);
    TimeoutProcessor.TimeoutFuture[] futures = new TimeoutProcessor.TimeoutFuture[1000];
    int primed = 0;

    for (int i = 0, j = 0; i < 100000; i++, j++) {
      if (j >= futures.length)
        j = 0;

      futures[j] = _timeout.schedule(new Runnable() {
        @Override
        public void run() {
          count.incrementAndGet();
        }
      }, 1, TimeUnit.SECONDS);
      primed++;
    }

    assertEquals(count.get(), 0);

    waitForStorm(count, primed);
    assertEquals(count.get(), primed);
  }

  private void waitForStorm(AtomicInteger count, long expected) throws InterruptedException {
    int i = 0;
    do {
      Thread.sleep(500L);
    } while (count.get() != expected && i++ < 24);
  }

  @Test(groups = { "unit" }, dataProvider = "TimeoutProcessor", successPercentage = 20, invocationCount = 5)
  void testFiringAccuracy(TimeoutProcessor processor) throws Exception {
    _timeout = processor;

    Map<Long, CompletableFuture<Long>> events = new HashMap<>();
    for (long expectedDelay: Arrays.asList(30, 90, 60)) {
      CompletableFuture future = new CompletableFuture();
      long startTime = System.nanoTime();
      _timeout.schedule(
          () -> future.complete(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)),
          expectedDelay,
          TimeUnit.MILLISECONDS);
      events.put(expectedDelay, future);
    }

    events.forEach((expectedDelay, future) -> {
      Long actualDelay = null;
      try {
        actualDelay = future.get(3, TimeUnit.SECONDS);
      } catch (Exception e) {
        Assert.fail("Event has not fired as expected within 3 seconds", e);
      }
      double errorTolerance = 0.50;
      double error = Math.abs(1. - (double) actualDelay / expectedDelay);
      Assert.assertTrue(
          error < errorTolerance,
          "Event fired after " + actualDelay + "ms (" + expectedDelay + "ms expected).");
    });
  }
}
