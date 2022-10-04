/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.alpini.base.concurrency;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.linkedin.alpini.base.misc.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestBlockingQueues {
  @DataProvider
  public Object[][] blockingQueues() {
    return Stream.of(new LinkedBlockingQueue<>(), new ConcurrentLinkedBlockingQueue<>())
        .map(queue -> new Object[] { queue, queue.getClass().getSimpleName() })
        .toArray(Object[][]::new);
  }

  /*
   * We need to perform operations in a thread pool, even for simple cases, because the queue might
   * be a SynchronousQueue.
   */
  private ExecutorService threadPool;

  @BeforeMethod
  public void setUp() {
    threadPool = Executors.newCachedThreadPool();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws InterruptedException {
    if (threadPool == null) {
      return;
    }
    threadPool.shutdown();
    assertTrue("Some worker didn't finish in time", threadPool.awaitTermination(1, TimeUnit.SECONDS));
    threadPool = null;
  }

  private static <T> int drain(
      BlockingQueue q,
      Collection buffer,
      int maxElements,
      long timeout,
      TimeUnit unit,
      boolean interruptibly) throws InterruptedException {
    return interruptibly
        ? drain(q, buffer, maxElements, timeout, unit)
        : drainUninterruptibly(q, buffer, maxElements, timeout, unit);
  }

  /**
   * Drains the queue as {@link BlockingQueue#drainTo(Collection, int)}, but if the requested
   * {@code numElements} elements are not available, it will wait for them up to the specified
   * timeout.
   *
   * @param q the blocking queue to be drained
   * @param buffer where to add the transferred elements
   * @param numElements the number of elements to be waited for
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the timeout parameter
   * @return the number of elements transferred
   * @throws InterruptedException if interrupted while waiting
   */
  public static <E> int drain(
      BlockingQueue<E> q,
      Collection<? super E> buffer,
      int numElements,
      long timeout,
      TimeUnit unit) throws InterruptedException {
    Assert.assertNotNull(buffer);
    /*
     * This code performs one System.nanoTime() more than necessary, and in return, the time to
     * execute Queue#drainTo is not added *on top* of waiting for the timeout (which could make
     * the timeout arbitrarily inaccurate, given a queue that is slow to drain).
     */
    long deadline = Time.nanoTime() + unit.toNanos(timeout);
    int added = 0;
    while (added < numElements) {
      // we could rely solely on #poll, but #drainTo might be more efficient when there are multiple
      // elements already available (e.g. LinkedBlockingQueue#drainTo locks only once)
      added += q.drainTo(buffer, numElements - added);
      if (added < numElements) { // not enough elements immediately available; will have to poll
        E e = q.poll(deadline - Time.nanoTime(), TimeUnit.NANOSECONDS);
        if (e == null) {
          break; // we already waited enough, and there are no more elements in sight
        }
        buffer.add(e);
        added++;
      }
    }
    return added;
  }

  /**
   * Drains the queue as {@linkplain #drain(BlockingQueue, Collection, int, long, TimeUnit)},
   * but with a different behavior in case it is interrupted while waiting. In that case, the
   * operation will continue as usual, and in the end the thread's interruption status will be set
   * (no {@code InterruptedException} is thrown).
   *
   * @param q the blocking queue to be drained
   * @param buffer where to add the transferred elements
   * @param numElements the number of elements to be waited for
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the timeout parameter
   * @return the number of elements transferred
   */
  public static <E> int drainUninterruptibly(
      BlockingQueue<E> q,
      Collection<? super E> buffer,
      int numElements,
      long timeout,
      TimeUnit unit) {
    Assert.assertNotNull(buffer);
    long deadline = Time.nanoTime() + unit.toNanos(timeout);
    int added = 0;
    boolean interrupted = false;
    try {
      while (added < numElements) {
        // we could rely solely on #poll, but #drainTo might be more efficient when there are
        // multiple elements already available (e.g. LinkedBlockingQueue#drainTo locks only once)
        added += q.drainTo(buffer, numElements - added);
        if (added < numElements) { // not enough elements immediately available; will have to poll
          E e; // written exactly once, by a successful (uninterrupted) invocation of #poll
          while (true) {
            try {
              e = q.poll(deadline - Time.nanoTime(), TimeUnit.NANOSECONDS);
              break;
            } catch (InterruptedException ex) {
              interrupted = true; // note interruption and retry
            }
          }
          if (e == null) {
            break; // we already waited enough, and there are no more elements in sight
          }
          buffer.add(e);
          added++;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return added;
  }

  private static <E> List<E> newArrayList() {
    return new ArrayList<>();
  }

  @Test(dataProvider = "blockingQueues")
  public void testMultipleProducers(BlockingQueue<Object> q, String qName) throws InterruptedException {
    for (boolean interruptibly: new boolean[] { true, false }) {
      threadPool.submit(new Producer(q, 20));
      threadPool.submit(new Producer(q, 20));
      threadPool.submit(new Producer(q, 20));
      threadPool.submit(new Producer(q, 20));
      threadPool.submit(new Producer(q, 20));

      List<Object> buf = newArrayList();
      int elements = drain(q, buf, 100, Long.MAX_VALUE, TimeUnit.NANOSECONDS, interruptibly);
      assertEquals(100, elements);
      assertEquals(100, buf.size());
      assertDrained(q);
    }
  }

  @Test(dataProvider = "blockingQueues")
  public void testMultipleProducersIterator(BlockingQueue<Object> q, String qName)
      throws InterruptedException, TimeoutException, ExecutionException {
    for (boolean interruptibly: new boolean[] { true, false }) {
      List<Future<?>> futures = Arrays.asList(
          threadPool.submit(new Producer(q, 20)),
          threadPool.submit(new Producer(q, 20)),
          threadPool.submit(new Producer(q, 20)),
          threadPool.submit(new Producer(q, 20)),
          threadPool.submit(new Producer(q, 20)));

      for (Future<?> f: futures) {
        f.get(1, TimeUnit.SECONDS);
      }

      Assert.assertFalse(q.isEmpty());
      Assert.assertEquals(100, q.size());

      Object[] arr = new Object[100];
      int count = 0;
      Iterator<Object> it = q.iterator();
      while (it.hasNext()) {
        Object obj = arr[count++] = it.next();
        Assert.assertNotNull(obj);
      }
      Assert.assertEquals(100, count);
      Assert.assertThrows(NoSuchElementException.class, it::next);

      class Pred implements Predicate<Object> {
        int count = 0;

        @Override
        public boolean test(Object o) {
          Assert.assertNotNull(o);
          count++;
          return o == arr[50];
        }
      }

      Pred p = new Pred();
      Assert.assertTrue(q.removeIf(p));
      Assert.assertEquals(p.count, 100);
      Assert.assertEquals(q.size(), 99);

      Assert.assertTrue(q.remove(arr[75]));
      Assert.assertEquals(q.size(), 98);

      Assert.assertTrue(q.contains(arr[25]));
      Assert.assertFalse(q.contains(arr[75]));

      List<Object> buf = newArrayList();
      int elements = drain(q, buf, 98, Long.MAX_VALUE, TimeUnit.NANOSECONDS, interruptibly);
      assertEquals(98, elements);
      assertEquals(98, buf.size());
      assertDrained(q);

      Assert.assertFalse(buf.contains(arr[50]));
      Assert.assertFalse(buf.contains(arr[75]));
    }

    threadPool.shutdown();
    Assert.assertTrue(threadPool.awaitTermination(1, TimeUnit.SECONDS));
    System.gc();
    Time.sleep(1000);
    System.gc();
    Assert.assertNull(q.poll());
    q.clear();
    Assert.assertThrows(NullPointerException.class, () -> q.offer(null, 1, TimeUnit.SECONDS));
  }

  @Test(dataProvider = "blockingQueues")
  public void testDrainTimesOut(BlockingQueue<Object> q, String qName) throws Exception {
    for (boolean interruptibly: new boolean[] { true, false }) {
      assertEquals(0, drain(q, Collections.emptyList(), 1, 10, TimeUnit.MILLISECONDS));

      Producer producer = new Producer(q, 1);
      // producing one, will ask for two
      Future<?> producerThread = threadPool.submit(producer);

      // make sure we time out
      long startTime = System.nanoTime();

      int drained = drain(q, newArrayList(), 2, 10, TimeUnit.MILLISECONDS, interruptibly);
      assertTrue(drained <= 1);

      assertTrue((System.nanoTime() - startTime) >= TimeUnit.MILLISECONDS.toNanos(10));

      // If even the first one wasn't there, clean up so that the next test doesn't see an element.
      producerThread.cancel(true);
      producer.doneProducing.await();
      if (drained == 0) {
        q.poll(); // not necessarily there if producer was interrupted
      }
    }
  }

  @Test(dataProvider = "blockingQueues")
  public void testZeroElements(BlockingQueue<Object> q, String qName) throws InterruptedException {
    Assert.assertTrue(q.remainingCapacity() > 0);
    for (boolean interruptibly: new boolean[] { true, false }) {
      Assert.assertTrue(q.isEmpty());
      // asking to drain zero elements
      assertEquals(0, drain(q, Collections.emptyList(), 0, 10, TimeUnit.MILLISECONDS, interruptibly));
      Assert.assertEquals(q.size(), 0);
    }
    q.clear();
    q.drainTo(Collections.emptyList());
    Assert.assertFalse(q.remove(new Object()));
  }

  @Test(dataProvider = "blockingQueues")
  public void testEmpty(BlockingQueue<Object> q, String qName) {
    assertDrained(q);
  }

  @Test(dataProvider = "blockingQueues")
  public void testNegativeMaxElements(BlockingQueue<Object> q, String qName) throws InterruptedException {
    threadPool.submit(new Producer(q, 1));

    List<Object> buf = newArrayList();
    int elements = drain(q, buf, -1, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    assertEquals(elements, 0);
    assertTrue(buf.isEmpty());

    // Free the producer thread, and give subsequent tests a clean slate.
    q.take();
  }

  @Test(dataProvider = "blockingQueues")
  public void testDrain_throws(BlockingQueue<Object> q, String qName) {
    threadPool.submit(new Interrupter(Thread.currentThread()));
    try {
      drain(q, Collections.emptyList(), 100, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      fail();
    } catch (InterruptedException expected) {
    }
  }

  @Test(dataProvider = "blockingQueues")
  public void testDrainUninterruptibly_doesNotThrow(final BlockingQueue<Object> q, String qName) {
    final Thread mainThread = Thread.currentThread();
    threadPool.submit(new Callable<Void>() {
      public Void call() throws InterruptedException {
        new Producer(q, 50).call();
        new Interrupter(mainThread).run();
        new Producer(q, 50).call();
        return null;
      }
    });
    List<Object> buf = newArrayList();
    int elements = drainUninterruptibly(q, buf, 100, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    // so when this drains all elements, we know the thread has also been interrupted in between
    assertTrue(Thread.interrupted());
    assertEquals(100, elements);
    assertEquals(100, buf.size());
  }

  /**
   * Checks that #drain() invocations behave correctly for a drained (empty) queue.
   */
  private void assertDrained(BlockingQueue<Object> q) {
    assertNull(q.peek());
    assertInterruptibleDrained(q);
    assertUninterruptibleDrained(q);
  }

  private void assertInterruptibleDrained(BlockingQueue<Object> q) {
    // nothing to drain, thus this should wait doing nothing
    try {
      assertEquals(0, drain(q, Collections.emptyList(), 0, 10, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      throw new AssertionError();
    }

    // but does the wait actually occurs?
    threadPool.submit(new Interrupter(Thread.currentThread()));
    try {
      // if waiting works, this should get stuck
      drain(q, newArrayList(), 1, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      fail();
    } catch (InterruptedException expected) {
      // we indeed waited; a slow thread had enough time to interrupt us
    }
  }

  // same as above; uninterruptible version
  private void assertUninterruptibleDrained(BlockingQueue<Object> q) {
    assertEquals(0, drainUninterruptibly(q, Collections.emptyList(), 0, 10, TimeUnit.MILLISECONDS));

    // but does the wait actually occurs?
    threadPool.submit(new Interrupter(Thread.currentThread()));

    long startTime = System.nanoTime();
    drainUninterruptibly(q, newArrayList(), 1, 10, TimeUnit.MILLISECONDS);
    assertTrue((System.nanoTime() - startTime) >= TimeUnit.MILLISECONDS.toNanos(10));
    // wait for interrupted status and clear it
    while (!Thread.interrupted()) {
      Thread.yield();
    }
  }

  private static class Producer implements Callable<Void> {
    final BlockingQueue<Object> q;
    final int elements;
    final CountDownLatch doneProducing = new CountDownLatch(1);

    Producer(BlockingQueue<Object> q, int elements) {
      this.q = q;
      this.elements = elements;
    }

    @Override
    public Void call() throws InterruptedException {
      try {
        for (int i = 0; i < elements; i++) {
          q.put(new Object());
        }
        return null;
      } finally {
        doneProducing.countDown();
      }
    }
  }

  private static class Interrupter implements Runnable {
    final Thread threadToInterrupt;

    Interrupter(Thread threadToInterrupt) {
      this.threadToInterrupt = threadToInterrupt;
    }

    @Override
    public void run() {
      try {
        Time.sleep(100);
      } catch (InterruptedException e) {
        throw new AssertionError();
      } finally {
        threadToInterrupt.interrupt();
      }
    }
  }
}
