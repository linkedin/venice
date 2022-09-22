package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.consts.QOS;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestQOSPolicy {
  final Logger _log = LogManager.getLogger(getClass());

  @Test(groups = { "unit" })
  public void testQueueFactory() {
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION.name());
    SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());
    Assert.assertTrue(
        queue instanceof QOSBasedQueue<?>,
        "HIGHEST_PRIORITY_W_FAIR_ALLOCATION policy must return a QOSBasedQueue");

    conf.setQosPolicy(QOSPolicy.FCFS.name());
    queue = QOSPolicy.getQOSPolicy(conf.build());
    Assert.assertTrue(queue instanceof QOSFCFSQueue<?>, "FCFS policy must return a QOSFCFSQueue");

    conf.setQosPolicy(QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION_MULTI_QUEUE.name());
    queue = QOSPolicy.getQOSPolicy(conf.build());
    Assert.assertTrue(
        queue instanceof QOSBasedMultiQueue<?>,
        "HIGHEST_PRIORITY_W_FAIR_ALLOCATION_MULTI_QUEUE policy must return a QOSBasedMultiQueue");
  }

  @Test(groups = { "unit" })
  public void testStaticConfig() {
    // Set correct fair allocation
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION.name());
    conf.setFairAllocationRatio("5:15:80");
    SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());
    Assert.assertTrue(
        queue instanceof QOSBasedQueue<?>,
        "HIGHEST_PRIORITY_W_FAIR_ALLOCATION policy must return a QOSBasedQueue");

    // Set non-numeric fair allocation.
    conf.setFairAllocationRatio("5:15:ADKJ");
    try {
      queue = QOSPolicy.getQOSPolicy(conf.build());
      Assert.fail("Cannot reach here. Must throw a NumberFormatException while building config");
    } catch (NumberFormatException ex) {
    }

    conf.setFairAllocationRatio("5:80");
    try {
      queue = QOSPolicy.getQOSPolicy(conf.build());
      Assert.fail("Cannot reach here. Must throw an IllegalArgumentException while building config");
    } catch (IllegalArgumentException ex) {
    }
  }

  @DataProvider
  public Object[][] getQOSPolicies() {
    Object[][] values = { { QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION },
        { QOSPolicy.HIGHEST_PRIORITY_W_FAIR_ALLOCATION_MULTI_QUEUE }, { QOSPolicy.FCFS } };
    return values;
  }

  @Test(groups = { "unit" }, dataProvider = "getQOSPolicies")
  public void testQosBasedQueueBasic(QOSPolicy policy) {
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(policy.name());
    SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());

    QOSBasedRequestRunnable r1 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r2 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r3 = new QOSBasedRequestRunnable("queue1", QOS.NORMAL, null);
    QOSBasedRequestRunnable r4 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r5 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    queue.add(r1);
    queue.add(r2);
    queue.add(r3);
    queue.add(r4);
    queue.add(r5);

    Assert.assertEquals(queue.size(), 5);

    for (int i = 0; i < 5; i++)
      queue.poll();

    Assert.assertNull(queue.poll());
    Assert.assertEquals(queue.size(), 0);
  }

  @Test(groups = { "unit" }, dataProvider = "getQOSPolicies")
  public void testQosBasedQueueBasic2(QOSPolicy policy) {
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(policy.name());
    SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());

    QOSBasedRequestRunnable r1 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r2 = new QOSBasedRequestRunnable("queue2", QOS.LOW, null);
    QOSBasedRequestRunnable r3 = new QOSBasedRequestRunnable("queue3", QOS.NORMAL, null);
    QOSBasedRequestRunnable r4 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r5 = new QOSBasedRequestRunnable("queue2", QOS.HIGH, null);
    QOSBasedRequestRunnable r6 = new QOSBasedRequestRunnable("queue3", QOS.HIGH, null);
    QOSBasedRequestRunnable r7 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r8 = new QOSBasedRequestRunnable("queue2", QOS.NORMAL, null);
    QOSBasedRequestRunnable r9 = new QOSBasedRequestRunnable("queue3", QOS.LOW, null);
    queue.add(r1);
    queue.add(r2);
    queue.add(r3);
    queue.add(r4);
    queue.add(r5);
    queue.add(r6);
    queue.add(r7);
    queue.add(r8);
    queue.add(r9);

    Assert.assertEquals(queue.size(), 9);

    for (int i = 0; i < 9; i++)
      queue.poll();

    Assert.assertNull(queue.poll());
    Assert.assertEquals(queue.size(), 0);
  }

  @Test(groups = { "unit" }, dataProvider = "getQOSPolicies")
  public void testQosBasedQueuePolling(QOSPolicy policy) {
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(policy.name());
    SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());

    QOSBasedRequestRunnable r1 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r2 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r3 = new QOSBasedRequestRunnable("queue1", QOS.NORMAL, null);
    QOSBasedRequestRunnable r4 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r5 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r6 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r7 = new QOSBasedRequestRunnable("queue1", QOS.NORMAL, null);

    queue.add(r1);
    queue.add(r2);
    queue.add(r3);
    queue.add(r4);
    queue.add(r5);
    queue.add(r6);
    queue.add(r7);
    Assert.assertEquals(queue.size(), 7);

    if (QOSPolicy.FCFS.name().equals(policy.name())) {
      Assert.assertSame(queue.poll(), r1);
      Assert.assertSame(queue.poll(), r2);
      Assert.assertSame(queue.poll(), r3);
      Assert.assertSame(queue.poll(), r4);
      Assert.assertSame(queue.poll(), r5);
      Assert.assertSame(queue.poll(), r6);
      Assert.assertSame(queue.poll(), r7);
      return;
    } else {
      AbstractQOSBasedQueue<QOSBasedRequestRunnable> q = (AbstractQOSBasedQueue<QOSBasedRequestRunnable>) queue;
      // Test the above configuration for all Sort orders
      // Sort Order 1: {HIGH, NORMAL, LOW}
      List<QOS> sortOrder = Arrays.asList(QOS.HIGH, QOS.NORMAL, QOS.LOW);
      Assert.assertSame(q.getElement(sortOrder), r1);
      Assert.assertSame(q.getElement(sortOrder), r5);

      // Sort Order 2: {NORMAL, HIGH, LOW}
      sortOrder = Arrays.asList(QOS.NORMAL, QOS.HIGH, QOS.LOW);
      Assert.assertSame(q.getElement(sortOrder), r3);

      // Sort Order 3: {LOW, HIGH, NORMAL}
      sortOrder = Arrays.asList(QOS.LOW, QOS.HIGH, QOS.NORMAL);
      Assert.assertSame(q.getElement(sortOrder), r2);
      Assert.assertSame(q.getElement(sortOrder), r4);
      Assert.assertSame(q.getElement(sortOrder), r6);
      Assert.assertSame(q.getElement(sortOrder), r7);
    }

    // Empty queue
    Assert.assertNull(queue.poll());
    Assert.assertEquals(queue.size(), 0);

  }

  @Test(groups = { "unit" })
  public void testQosBasedMultiQueuePolling() {
    AbstractQOSBasedQueue<QOSBasedRequestRunnable> queue = new QOSBasedMultiQueue<QOSBasedRequestRunnable>();

    QOSBasedRequestRunnable r1 = new QOSBasedRequestRunnable("queue1", QOS.HIGH, null);
    QOSBasedRequestRunnable r2 = new QOSBasedRequestRunnable("queue1", QOS.LOW, null);
    QOSBasedRequestRunnable r3 = new QOSBasedRequestRunnable("queue2", QOS.NORMAL, null);
    QOSBasedRequestRunnable r4 = new QOSBasedRequestRunnable("queue2", QOS.LOW, null);
    QOSBasedRequestRunnable r5 = new QOSBasedRequestRunnable("queue2", QOS.HIGH, null);
    QOSBasedRequestRunnable r6 = new QOSBasedRequestRunnable("queue3", QOS.HIGH, null);
    QOSBasedRequestRunnable r7 = new QOSBasedRequestRunnable("queue3", QOS.NORMAL, null);

    queue.add(r1);
    queue.add(r2);
    queue.add(r3);
    queue.add(r4);
    queue.add(r5);
    queue.add(r6);
    queue.add(r7);
    Assert.assertEquals(queue.size(), 7);

    // Test the above configuration for all Sort orders
    // Sort Order 1: {HIGH, NORMAL, LOW}
    List<QOS> sortOrder = Arrays.asList(QOS.HIGH, QOS.NORMAL, QOS.LOW);

    // queue1
    Assert.assertSame(queue.getElement(sortOrder), r1);

    // queue2
    Assert.assertSame(queue.getElement(sortOrder), r5);

    // Sort Order 2: {NORMAL, HIGH, LOW}
    sortOrder = Arrays.asList(QOS.NORMAL, QOS.HIGH, QOS.LOW);

    // queue3
    Assert.assertSame(queue.getElement(sortOrder), r7);

    // Sort Order 3: {LOW, HIGH, NORMAL}
    sortOrder = Arrays.asList(QOS.LOW, QOS.HIGH, QOS.NORMAL);

    // queue1
    Assert.assertSame(queue.getElement(sortOrder), r2);

    // queue2
    Assert.assertSame(queue.getElement(sortOrder), r4);

    // queue3
    Assert.assertSame(queue.getElement(sortOrder), r6);

    // queue2
    Assert.assertSame(queue.getElement(sortOrder), r3);

    // Empty queue
    Assert.assertNull(queue.poll());
    Assert.assertEquals(queue.size(), 0);
  }

  @Test(groups = { "unit", "NoCoverage" }, dataProvider = "getQOSPolicies")
  public void testQosBasedQueueStress(QOSPolicy policy) throws InterruptedException {
    QOSPolicy.Config conf = new QOSPolicy.Config();
    conf.setQosPolicy(policy.name());

    final int runSize = 100000; // each enque thread generates this many items.
    final CountDownLatch doneLatch = new CountDownLatch(10); // this many enque threads
    // runSize = 100000, doneLatch = 10 results in around a 10 second test.

    // The use of sun.misc.Unsafe to enforce load/store barriers shaves nearly 10% off the time
    // without having a missed entry. ie, the new code, even though it introduces a semaphore,
    // is as fast or faster than the code before this change without unexpected nulls.

    final AtomicBoolean running = new AtomicBoolean(true);
    final Semaphore wakeup = new Semaphore(0);
    final SimpleQueue<QOSBasedRequestRunnable> queue = QOSPolicy.getQOSPolicy(conf.build());
    final Map<QOS, AtomicInteger> dequeCount = new EnumMap<>(QOS.class);
    final Map<QOS, AtomicInteger> enqueCount = new EnumMap<>(QOS.class);
    final ConcurrentHashMap<String, AtomicInteger> enqueQueueCount = new ConcurrentHashMap<String, AtomicInteger>();
    final ConcurrentHashMap<String, AtomicInteger> dequeQueueCount = new ConcurrentHashMap<String, AtomicInteger>();

    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch readyLatch = new CountDownLatch((int) doneLatch.getCount());

    final AtomicInteger missCount = new AtomicInteger();

    dequeCount.put(QOS.HIGH, new AtomicInteger());
    dequeCount.put(QOS.NORMAL, new AtomicInteger());
    dequeCount.put(QOS.LOW, new AtomicInteger());

    enqueCount.put(QOS.HIGH, new AtomicInteger());
    enqueCount.put(QOS.NORMAL, new AtomicInteger());
    enqueCount.put(QOS.LOW, new AtomicInteger());

    Runnable deque = new Runnable() {
      @Override
      public void run() {
        int miss = 0;
        try {
          while (running.get()) {
            if (!wakeup.tryAcquire(10, TimeUnit.MILLISECONDS)) {
              _log.debug("Sleep");
              Thread.sleep(10);
              continue;
            }
            QOSBasedRequestRunnable item = queue.poll();

            if (item == null) {
              missCount.incrementAndGet();

              _log.info("Didn't catch after wakeup, {} {}", miss++, Thread.currentThread());

              wakeup.release();
              continue;
            }

            miss = 0;

            dequeCount.get(item._qos).incrementAndGet();

            AtomicInteger q = dequeQueueCount.get(item._queueName);
            if (q == null) {
              AtomicInteger nq = new AtomicInteger();
              q = dequeQueueCount.putIfAbsent(item._queueName, nq);
              if (q == null) {
                q = nq;
              }
            }
            q.incrementAndGet();
            _log.debug("Dequeued {} entry for {}", item._qos, item._queueName);
          }

        } catch (Exception ex) {
          ex.printStackTrace();
        } finally {
          _log.debug("Deque thread finished: {}", Thread.currentThread());
        }
      }
    };

    Runnable enque = new Runnable() {
      private final String syllables = "..lexegezacebisousesarmaindirea.eratenberalavetiedorquanteisrion";

      private String makeQueueName(int[] seed) {
        StringBuilder name = new StringBuilder();
        for (;;) {
          boolean longName = 0 != (seed[0] & 0x40);

          for (int n = 0; n < 4; n++) {
            int d = ((seed[2] >>> 8) & 0x1f) << 1;
            int t = (seed[0] + seed[1] + seed[2]) & 0xffff;
            seed[0] = seed[1];
            seed[1] = seed[2];
            seed[2] = t;
            if (longName || n < 3) {
              name.append(syllables.subSequence(d, d + 2));
              for (int i = name.length() - 1; i >= Math.max(0, name.length() - 2); i--) {
                if (name.charAt(i) == '.') {
                  name.deleteCharAt(i);
                }
              }
            }
          }
          if (ThreadLocalRandom.current().nextInt(3) == 1) {
            name.insert(1, Character.toUpperCase(name.charAt(0)));
            return name.substring(1);
          }
          name.setLength(0);
        }
      }

      @Override
      public void run() {
        int[] seed = { 0x5a4a, 0x0248, 0xb753 };
        String[] queueNames = new String[50];
        for (int i = 0; i < queueNames.length; i++) {
          queueNames[i] = makeQueueName(seed);
          enqueQueueCount.put(queueNames[i], new AtomicInteger());
        }

        QOS[] qosList = { QOS.HIGH, QOS.NORMAL, QOS.NORMAL, QOS.NORMAL, QOS.NORMAL, QOS.NORMAL, QOS.NORMAL, QOS.NORMAL,
            QOS.LOW, QOS.LOW };

        try {
          readyLatch.countDown();
          startLatch.await();
          ThreadLocalRandom rnd = ThreadLocalRandom.current();

          for (int i = runSize; i > 0; i--) {
            int name = rnd.nextInt(queueNames.length);
            if (name == 42)
              continue;
            String queueName = queueNames[name];
            QOS qos = qosList[rnd.nextInt(qosList.length)];

            enqueQueueCount.get(queueName).incrementAndGet();
            enqueCount.get(qos).incrementAndGet();

            if (queue.add(new QOSBasedRequestRunnable(queueName, qos, null))) {
              _log.debug("Added {} entry for {}", qos, queueName);
              wakeup.release();
            } else {
              _log.warn("Failed to enque {} entry for {}", qos, queueName);
            }

            if (rnd.nextInt(10) == 0) {
              Thread.yield();
            } else if (rnd.nextInt(100) == 0) {
              Thread.sleep(10);
            }

          }
        } catch (Exception ex) {
          ex.printStackTrace();
        } finally {
          _log.debug("Enque thread finished: {}", Thread.currentThread());
          doneLatch.countDown();
        }
      }
    };

    Thread dequeThread[] = { new Thread(deque), new Thread(deque), new Thread(deque), new Thread(deque) };

    for (Thread aDequeThread: dequeThread) {
      aDequeThread.start();
    }

    Thread enqueThread[] = new Thread[(int) doneLatch.getCount()];
    for (int i = 0; i < enqueThread.length; i++) {
      enqueThread[i] = new Thread(enque);
      enqueThread[i].start();
    }

    _log.info("Waiting for enqueue threads to be ready");
    readyLatch.await();
    Thread.sleep(100);
    _log.info("Starting test...");
    startLatch.countDown();

    doneLatch.await();
    _log.info("Enqueue threads are done.");

    while (queue.size() > 0) {
      Thread.yield();
    }
    _log.info("Dequeue threads are done.");

    // wait a second so that if there are some lost queue entries, we have
    // gathered a lot of missing posted events, which will fail the assert
    // at the end of this test.
    Thread.sleep(1000);
    running.set(false);

    for (Thread aEnqueThread: enqueThread) {
      aEnqueThread.join();
    }

    for (Thread aDequeThread: dequeThread) {
      aDequeThread.join();
    }

    _log.info("Checking results.");

    for (Map.Entry<QOS, AtomicInteger> entry: enqueCount.entrySet()) {
      Assert
          .assertEquals(entry.getValue().get(), dequeCount.get(entry.getKey()).get(), "Mismatch for " + entry.getKey());
    }

    for (Map.Entry<String, AtomicInteger> entry: enqueQueueCount.entrySet()) {
      int hashCode = entry.getKey().hashCode() & 0xff;
      if (hashCode == 0x04 || hashCode == 0xbc || hashCode == 0x48) {
        // Print a few values
        _log.info("Testing for {} expect {}", entry.getKey(), entry.getValue());
      }

      if (entry.getValue().get() == 0) {
        Assert.assertNull(dequeQueueCount.get(entry.getKey()), "Expected null for " + entry.getKey());
      } else {
        Assert.assertEquals(
            dequeQueueCount.get(entry.getKey()).get(),
            entry.getValue().get(),
            "Mismatch for " + entry.getKey());
      }
    }

    Assert.assertEquals(
        missCount.get(),
        0,
        "Non-zero miss counts means something was dropped by queue: " + missCount.get());
  }

  @Test(groups = { "unit" })
  public void testQosBasedQueueSortOrder() {
    // Case 1: Only HIGH is set
    Map<QOS, Integer> qosAllocation = new EnumMap<>(QOSBasedQueue.getDefaultQOSAllocation());
    qosAllocation.put(QOS.LOW, 0);
    qosAllocation.put(QOS.NORMAL, 0);
    QOSBasedQueue<QOSBasedRequestRunnable> q = new QOSBasedQueue<>(qosAllocation);
    List<QOS> sortOrder = q.getQueuePollOrder();
    Assert.assertEquals(sortOrder, Arrays.asList(QOS.HIGH, QOS.NORMAL, QOS.LOW));

    // Case 1: Only NORMAL is set
    qosAllocation = new EnumMap<>(QOSBasedQueue.getDefaultQOSAllocation());
    qosAllocation.put(QOS.LOW, 0);
    qosAllocation.put(QOS.HIGH, 0);
    q = new QOSBasedQueue<>(qosAllocation);
    sortOrder = q.getQueuePollOrder();
    Assert.assertEquals(sortOrder, Arrays.asList(QOS.NORMAL, QOS.HIGH, QOS.LOW));

    // Case 1: Only LOW is set
    qosAllocation = new EnumMap<>(QOSBasedQueue.getDefaultQOSAllocation());
    qosAllocation.put(QOS.NORMAL, 0);
    qosAllocation.put(QOS.HIGH, 0);
    q = new QOSBasedQueue<>(qosAllocation);
    sortOrder = q.getQueuePollOrder();
    Assert.assertEquals(sortOrder, Arrays.asList(QOS.LOW, QOS.HIGH, QOS.NORMAL));

  }

}
