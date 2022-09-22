package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.misc.ThreadPoolExecutor;
import com.linkedin.alpini.base.misc.Time;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class TestShutdownableExecutors {
  @Test(groups = "unit")
  public void testFactoryObjectMethods() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    String value1 = reg.factory(ShutdownableExecutors.class).toString();
    int hash1 = reg.factory(ShutdownableExecutors.class).hashCode();

    Assert.assertNotNull(value1);

    String value2 = reg.factory(ShutdownableExecutors.class).toString();

    Assert.assertEquals(value2, value1);

    int hash2 = reg.factory(ShutdownableExecutors.class).hashCode();

    Assert.assertEquals(hash2, hash1);
  }

  @Test(groups = "unit")
  public void testNewSingleThreadExecutor() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor = reg.factory(ShutdownableExecutors.class).newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.execute(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewSingleThreadExecutorNamed() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor =
        reg.factory(ShutdownableExecutors.class).newSingleThreadExecutor(new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewFixedThreadPool() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor = reg.factory(ShutdownableExecutors.class).newFixedThreadPool(1);

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Callable() {
      @Override
      public Object call() {
        latch.countDown();
        return null;
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewFixedThreadPoolUnwrap() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ShutdownableExecutorService executor = reg.factory(ShutdownableExecutors.class).newFixedThreadPool(1);

    final CountDownLatch latch = new CountDownLatch(1);

    ThreadPoolExecutor pool = executor.unwrap(ThreadPoolExecutor.class);

    pool.submit(new Callable() {
      @Override
      public Object call() {
        latch.countDown();
        return null;
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(pool.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewFixedThreadPoolNamed() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor =
        reg.factory(ShutdownableExecutors.class).newFixedThreadPool(1, new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewCachedThreadPool() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor = reg.factory(ShutdownableExecutors.class).newCachedThreadPool();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewCachedThreadPoolNamed() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ExecutorService executor =
        reg.factory(ShutdownableExecutors.class).newCachedThreadPool(new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    }, 1);

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewSingleThreadScheduledExecutor() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ScheduledExecutorService executor = reg.factory(ShutdownableExecutors.class).newSingleThreadScheduledExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.schedule(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    }, 100, TimeUnit.MILLISECONDS);

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewSingleThreadScheduledExecutorNamed() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ScheduledExecutorService executor =
        reg.factory(ShutdownableExecutors.class).newSingleThreadScheduledExecutor(new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.schedule(new Callable() {
      @Override
      public Object call() throws Exception {
        latch.countDown();
        return null;
      }
    }, 100, TimeUnit.MILLISECONDS);

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewScheduledThreadPool() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ScheduledExecutorService executor = reg.factory(ShutdownableExecutors.class).newScheduledThreadPool(2);

    final CountDownLatch latch = new CountDownLatch(1);

    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    }, 100, 100, TimeUnit.MILLISECONDS);

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testNewScheduledThreadPoolNamed() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();

    ScheduledExecutorService executor =
        reg.factory(ShutdownableExecutors.class).newScheduledThreadPool(2, new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    }, 100, 100, TimeUnit.MILLISECONDS);

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.shutdown();

    Assert.assertTrue(reg.isShutdown());

    reg.waitForShutdown();

    Assert.assertTrue(reg.isTerminated());

    Assert.assertTrue(executor.isTerminated());
  }

  @Test(groups = "unit")
  public void testResourceGarbageCollection() throws InterruptedException {
    AtomicReference<ResourceRegistry> reg = new AtomicReference<ResourceRegistry>(new ResourceRegistry(true));
    ReferenceQueue<ResourceRegistry> referenceQueue = new ReferenceQueue<>();
    PhantomReference<ResourceRegistry> phantom = new PhantomReference<>(reg.get(), referenceQueue);

    ExecutorService executor =
        reg.get().factory(ShutdownableExecutors.class).newCachedThreadPool(new NamedThreadFactory("test"));

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(new Runnable() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    latch.await();

    Assert.assertFalse(executor.isShutdown());

    reg.set(null); // Oops! We lost it!
    Assert.assertNull(reg.get());

    // Wait for the ResourceRegistry to be GC.
    for (;;) {
      Reference<?> ref = referenceQueue.remove(100);
      if (ref == null) {
        System.gc();
        continue;
      }
      Assert.assertSame(ref, phantom);
      break;
    }

    // Wait for the ResourceRegistry to start shutdown of the executor
    long shutdownTimeout = Time.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (!executor.isShutdown() && Time.nanoTime() < shutdownTimeout) {
      Thread.yield();
    }
    Assert.assertTrue(executor.isShutdown());

    // Wait for the shutdown to complete
    Assert.assertTrue(executor.awaitTermination(100, TimeUnit.MILLISECONDS));

    Assert.assertTrue(executor.isTerminated());
  }

}
