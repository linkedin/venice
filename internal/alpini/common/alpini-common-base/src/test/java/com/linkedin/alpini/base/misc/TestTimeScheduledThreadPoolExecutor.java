package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.concurrency.Executors;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@Test(singleThreaded = true)
public class TestTimeScheduledThreadPoolExecutor {
  @Test(groups = "unit")
  public void testQueueDrain() throws Exception {
    BlockingQueue<Runnable> queue = new TimeScheduledThreadPoolExecutor.DelayedWorkQueue();
    try {

      Time.freeze();
      Thread.sleep(1);

      class Task<V> extends TimeScheduledThreadPoolExecutor.AbstractFutureTask<V> {
        Task(Callable<V> callable, boolean cancellable, long ns, long period) {
          super(callable, cancellable, ns, period);
        }
      }

      Callable<?> mock = Mockito.mock(Callable.class);

      Task<?> task1 = new Task<>(mock, false, Time.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1), 0);
      Task<?> task2 = new Task<>(mock, false, Time.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1), 0);

      queue.put(task1);
      queue.put(task2);

      Runnable[] tasks = new Runnable[3];
      Assert.assertSame(queue.toArray(tasks), tasks);

      Assert.assertEquals(Arrays.stream(tasks).filter(Objects::nonNull).count(), 2);
      Assert.assertTrue(Arrays.asList(tasks).contains(task1));
      Assert.assertTrue(Arrays.asList(tasks).contains(task2));

      List<Runnable> list = new LinkedList<>();
      Assert.assertEquals(queue.drainTo(list, 1), 0);
      Iterator<Runnable> it = queue.iterator();

      Time.restore();

      Assert.assertEquals(queue.drainTo(list, 1), 1);

      Assert.assertEquals(queue.drainTo(list, 0), 0);

      Assert.assertEquals(list.size(), 1);
      Assert.assertEquals(queue.size(), 1);

      Assert.assertEquals(queue.drainTo(list, 2), 1);
      Assert.assertTrue(queue.isEmpty());

      // check iterator contains snapshot
      Consumer<Runnable> consumer = Mockito.mock(Consumer.class);
      it.forEachRemaining(consumer);
      Mockito.verify(consumer).accept(Mockito.eq(task1));
      Mockito.verify(consumer).accept(Mockito.eq(task2));
      Mockito.verifyNoMoreInteractions(consumer);

    } finally {
      Time.restore();
    }
  }

  @Test(groups = "unit")
  public void testQueueTimeSkip() throws Exception {
    BlockingQueue<Runnable> queue = new TimeScheduledThreadPoolExecutor.DelayedWorkQueue();
    try {

      Time.freeze();
      Thread.sleep(1);

      class Task<V> extends TimeScheduledThreadPoolExecutor.AbstractFutureTask<V> {
        Task(Callable<V> callable, boolean cancellable, long ns, long period) {
          super(callable, cancellable, ns, period);
        }
      }

      Callable<?> mock = Mockito.mock(Callable.class);

      queue.put(new Task<>(mock, false, Time.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1), 0));

      Assert.assertNull(queue.poll());

      Time.restore();

      Runnable task = queue.poll();
      Assert.assertNotNull(task);
      Mockito.verifyNoMoreInteractions(mock);
      task.run();
      Mockito.verify(mock).call();
      Mockito.verifyNoMoreInteractions(mock);

    } finally {
      Time.restore();
    }
  }

  @Test(groups = "unit")
  public void testQueueLongTimeSkip() throws Exception {
    BlockingQueue<Runnable> queue = new TimeScheduledThreadPoolExecutor.DelayedWorkQueue();
    AtomicBoolean shutdown = new AtomicBoolean();
    Thread timeWarp = new Thread(() -> {
      while (!shutdown.get()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
          // Ignore
        }
        Time.advance(10, TimeUnit.MINUTES);
      }
    });
    timeWarp.start();
    try {
      Callable<?> mock = Mockito.mock(Callable.class);

      class Task<V> extends TimeScheduledThreadPoolExecutor.AbstractFutureTask<V> {
        Task(Callable<V> callable, boolean cancellable, long ns, long period) {
          super(callable, cancellable, ns, period);
        }
      }

      Assert.assertNull(queue.poll(100L, TimeUnit.MILLISECONDS));

      Assert.assertTrue(
          queue
              .offer(new Task<>(mock, false, Time.nanoTime() + TimeUnit.HOURS.toNanos(2), 0), 1, TimeUnit.NANOSECONDS));

      Assert.assertNull(queue.poll(100L, TimeUnit.MILLISECONDS));

      Assert.assertNull(queue.poll(1, TimeUnit.HOURS));

      Runnable task = queue.poll(1, TimeUnit.HOURS);
      Assert.assertNotNull(task);

      Mockito.verifyNoMoreInteractions(mock);
      task.run();
      Mockito.verify(mock).call();
      Mockito.verifyNoMoreInteractions(mock);

    } finally {
      shutdown.set(true);
      timeWarp.join();
      Time.restore();
    }
  }

  @Test(groups = "unit")
  public void testShutdown() throws InterruptedException {
    TimeScheduledThreadPoolExecutor executor = new TimeScheduledThreadPoolExecutor(1);
    Runnable mock = Mockito.mock(Runnable.class);
    try {
      Assert.assertEquals(executor.getQueue().remainingCapacity(), Integer.MAX_VALUE);

      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

      executor.schedule(mock, 1, TimeUnit.HOURS);

      executor.shutdown();

      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

      Mockito.verifyNoMoreInteractions(mock);

      executor = new TimeScheduledThreadPoolExecutor(1);

      executor.schedule(mock, 1, TimeUnit.HOURS);

      List<Runnable> tasks = executor.shutdownNow();

      Mockito.verifyNoMoreInteractions(mock);

      Assert.assertEquals(tasks.size(), 1);

      tasks.forEach(Runnable::run);
      Mockito.verify(mock).run();
      Mockito.verifyNoMoreInteractions(mock);

      Mockito.reset(mock);

      executor = new TimeScheduledThreadPoolExecutor(1);

      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);

      executor.schedule(mock, 1, TimeUnit.SECONDS);

      executor.shutdown();

      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

      Mockito.verify(mock).run();
      Mockito.verifyNoMoreInteractions(mock);

    } finally {
      executor.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testBasicTimeSkip() throws InterruptedException {
    ScheduledExecutorService executor = new TimeScheduledThreadPoolExecutor(1);
    try {

      Time.freeze();

      CountDownLatch latch = new CountDownLatch(1);

      Runnable mock = Mockito.mock(Runnable.class);
      Mockito.doAnswer((invocation) -> {
        latch.countDown();
        return null;
      }).when(mock).run();

      executor.schedule(mock, 1, TimeUnit.MILLISECONDS);

      latch.await(1000L, TimeUnit.MILLISECONDS);
      Mockito.verifyNoMoreInteractions(mock);

      Time.restore();

      latch.await(1000L, TimeUnit.MILLISECONDS);
      Mockito.verify(mock).run();
      Mockito.verifyNoMoreInteractions(mock);

    } finally {
      executor.shutdown();
      Time.restore();
    }
  }

  @Test(groups = "unit")
  public void testLongTimeSkip() throws InterruptedException {
    ScheduledExecutorService executor = new TimeScheduledThreadPoolExecutor(1);
    Thread timeWarp = new Thread(() -> {
      while (!executor.isShutdown()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
          // Ignore
        }
        Time.advance(10, TimeUnit.MINUTES);
      }
    });
    timeWarp.start();
    try {
      CountDownLatch latch = new CountDownLatch(1);

      Runnable mock = Mockito.mock(Runnable.class);
      Mockito.doAnswer((invocation) -> {
        latch.countDown();
        return null;
      }).when(mock).run();

      executor.schedule(mock, 2, TimeUnit.HOURS);

      latch.await(100L, TimeUnit.MILLISECONDS);
      Mockito.verifyNoMoreInteractions(mock);

      Time.sleep(TimeUnit.HOURS.toMillis(1));

      Mockito.verifyNoMoreInteractions(mock);

      Time.sleep(TimeUnit.HOURS.toMillis(1));

      latch.await(1000L, TimeUnit.MILLISECONDS);
      Mockito.verify(mock).run();
      Mockito.verifyNoMoreInteractions(mock);

    } finally {
      executor.shutdown();
      timeWarp.join();
      Time.restore();
    }
  }

  static void waitForNanoTimeTick() {
    for (long t0 = System.nanoTime();;) {
      if (t0 != System.nanoTime()) {
        return;
      }
    }
  }

  @DataProvider
  public Object[][] testCombo1() {
    Object[][] foo = new Object[16][];
    int k = 0;
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; j++) {
        foo[k++] = new Object[] { i, j };
      }
    }
    return foo;
  }

  @DataProvider
  public Object[][] testCombo2() {
    Object[][] foo = new Object[4][];
    int k = 0;
    for (int i = 0; i < 4; i++) {
      foo[k++] = new Object[] { i };
    }
    return foo;
  }

  private void scheduleNow(TimeScheduledThreadPoolExecutor pool, Runnable r, int how) {
    switch (how) {
      case 0:
        pool.schedule(r, 0, TimeUnit.MILLISECONDS);
        break;
      case 1:
        pool.schedule(Executors.callable(r), 0, TimeUnit.DAYS);
        break;
      case 2:
        pool.scheduleWithFixedDelay(r, 0, 1000, TimeUnit.NANOSECONDS);
        break;
      case 3:
        pool.scheduleAtFixedRate(r, 0, 1000, TimeUnit.MILLISECONDS);
        break;
      default:
        Assert.fail(String.valueOf(how));
    }
  }

  private void scheduleAtTheEndOfTime(TimeScheduledThreadPoolExecutor pool, Runnable r, int how) {
    switch (how) {
      case 0:
        pool.schedule(r, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        break;
      case 1:
        pool.schedule(Executors.callable(r), Long.MAX_VALUE, TimeUnit.DAYS);
        break;
      case 2:
        pool.scheduleWithFixedDelay(r, Long.MAX_VALUE, 1000, TimeUnit.NANOSECONDS);
        break;
      case 3:
        pool.scheduleAtFixedRate(r, Long.MAX_VALUE, 1000, TimeUnit.MILLISECONDS);
        break;
      default:
        Assert.fail(String.valueOf(how));
    }
  }

  @Test(groups = "unit", dataProvider = "testCombo1")
  public void testScheduleTimeOverflow1(int nowHow, int thenHow) throws InterruptedException {
    final TimeScheduledThreadPoolExecutor pool = new TimeScheduledThreadPoolExecutor(1);
    try {
      final AsyncPromise<Void> fail = AsyncFuture.deferred(false);
      final CountDownLatch runLatch = new CountDownLatch(1);
      final CountDownLatch busyLatch = new CountDownLatch(1);
      final CountDownLatch proceedLatch = new CountDownLatch(1);
      final Runnable notifier = runLatch::countDown;
      final Runnable neverRuns = () -> fail.setSuccess(null);
      final Runnable keepPoolBusy = () -> {
        try {
          busyLatch.countDown();
          proceedLatch.await();
        } catch (Throwable t) {
          fail.setFailure(t);
        }
      };
      pool.schedule(keepPoolBusy, 0, TimeUnit.SECONDS);
      busyLatch.await();
      scheduleNow(pool, notifier, nowHow);
      waitForNanoTimeTick();
      scheduleAtTheEndOfTime(pool, neverRuns, thenHow);
      proceedLatch.countDown();

      Assert.assertTrue(runLatch.await(10L, TimeUnit.SECONDS));
      Assert.assertEquals(runLatch.getCount(), 0L);
      Assert.assertFalse(fail.isDone());

      pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    } finally {
      pool.shutdown();
    }
  }

  @Test(groups = "unit", dataProvider = "testCombo2")
  public void testScheduleTimeOverflow2(int nowHow) throws InterruptedException {
    final int nowHowCopy = nowHow;
    final TimeScheduledThreadPoolExecutor pool = new TimeScheduledThreadPoolExecutor(1);
    try {
      final AsyncPromise<Void> fail = AsyncFuture.deferred(false);
      final CountDownLatch runLatch = new CountDownLatch(1);
      final Runnable notifier = runLatch::countDown;
      final Runnable scheduleNowScheduler = () -> {
        try {
          scheduleNow(pool, notifier, nowHowCopy);
          waitForNanoTimeTick();
        } catch (Throwable t) {
          fail.setFailure(t);
        }
      };
      pool.scheduleWithFixedDelay(scheduleNowScheduler, 0, Long.MAX_VALUE, TimeUnit.NANOSECONDS);

      Assert.assertTrue(runLatch.await(10L, TimeUnit.SECONDS));
      Assert.assertEquals(runLatch.getCount(), 0L);
      Assert.assertFalse(fail.isDone());

      pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    } finally {
      pool.shutdown();
    }
  }
}
