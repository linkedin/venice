package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestDefaultAsyncFuture {
  private static final int TEST_RUNS = 5; // Too many runs and it bogs down a machine.

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(groups = { "unit" }, expectedExceptions = NullPointerException.class)
  public void testAddListenerNPE1() throws Exception {
    AsyncFuture<Void> asyncFuture = new DefaultAsyncFuture<Void>(false);
    AsyncFutureListener<Void> nullListener = null;
    asyncFuture.addListener(nullListener);
  }

  @Test(groups = { "unit" }, expectedExceptions = NullPointerException.class)
  public void testAddListenerNPE2() throws Exception {
    AsyncPromise<Void> asyncFuture = new DefaultAsyncFuture<Void>(false);
    AsyncPromise<Void> nullListener = null;
    asyncFuture.addListener(nullListener);
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowError() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++) {
      final int finalI = i;
      p[i] = new DefaultAsyncFuture<Void>(false);
      p[i].addListener(new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          if (finalI + 1 < p.length) {
            p[finalI + 1].setSuccess(null);
          }
        }
      });
    }
    p[0].setSuccess(null);
    for (AsyncFuture<Void> a: p) {
      Assert.assertTrue(a.isSuccess());
    }
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorWithAdapter() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    p[0].setSuccess(null);
    for (AsyncFuture<Void> a: p) {
      Assert.assertTrue(a.isSuccess());
    }
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorWithAdapter2() throws Exception {
    final AsyncPromise<Object>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Object>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Object o = new Object();
    p[0].setSuccess(o);
    for (AsyncFuture<Object> a: p) {
      Assert.assertTrue(a.isSuccess());
      Assert.assertSame(a.get(), o);
    }
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorWithAdapter3() throws Exception {
    final AsyncPromise<Object>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Object>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    final Object o = new Object();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        sleepQuietly(100);
        p[0].setSuccess(o);
      }
    });
    t.start();
    for (AsyncFuture<Object> a: p) {
      Assert.assertSame(a.get(1, TimeUnit.SECONDS), o);
      Assert.assertTrue(a.isSuccess());
    }
    t.join();
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorAsyncUninterruptable() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        sleepQuietly(100);
        p[0].setSuccess(null);
      }
    });
    t.start();
    for (AsyncFuture<Void> a: p) {
      a.awaitUninterruptibly();
      Assert.assertTrue(a.isSuccess());
    }
    t.join();
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorAsyncUninterruptable2() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        sleepQuietly(100);
        p[0].setSuccess(null);
      }
    });
    t.start();
    for (AsyncFuture<Void> a: p) {
      a.awaitUninterruptibly(1, TimeUnit.SECONDS);
      Assert.assertTrue(a.isSuccess());
    }
    t.join();
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorAsyncInterruptable() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        sleepQuietly(100);
        p[0].setSuccess(null);
      }
    });
    t.start();
    for (AsyncFuture<Void> a: p) {
      a.await();
      Assert.assertTrue(a.isSuccess());
    }
    t.join();
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorAsync() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        sleepQuietly(100);
        p[0].setSuccess(null);
      }
    });
    t.start();
    for (AsyncFuture<Void> a: p) {
      a.await(1, TimeUnit.SECONDS);
      Assert.assertTrue(a.isSuccess());
    }
    t.join();
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorOnFailure() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++) {
      final int finalI = i;
      p[i] = new DefaultAsyncFuture<Void>(false);
      p[i].addListener(new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          if (finalI + 1 < p.length) {
            p[finalI + 1].setFailure(future.getCause());
          }
        }
      });
    }
    Exception e = new Exception();
    p[0].setFailure(e);
    for (AsyncFuture<Void> a: p) {
      Assert.assertSame(a.getCause(), e);
    }
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorOnFailureWithAdapter() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(false);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    Exception e = new Exception();
    p[0].setFailure(e);
    for (AsyncFuture<Void> a: p) {
      Assert.assertSame(a.getCause(), e);
    }
  }

  @Test(groups = { "unit" })
  @SuppressWarnings("unchecked")
  public void testNoStackOverflowErrorOnCancelWithAdapter() throws Exception {
    final AsyncPromise<Void>[] p = new AsyncPromise[128];
    for (int i = 0; i < p.length; i++)
      p[i] = new DefaultAsyncFuture<Void>(true);
    for (int i = 0; i < p.length - 1; i++)
      p[i].addListener(p[i + 1]);
    p[0].cancel(false);
    for (AsyncFuture<Void> a: p) {
      Assert.assertTrue(a.isCancelled());
    }
  }

  @Test(groups = { "unit" }, enabled = false)
  public void testListenerNotifyOrder() throws Exception {
    final BlockingQueue<AsyncFutureListener<Void>> listeners = new LinkedBlockingQueue<AsyncFutureListener<Void>>();
    int runs = TEST_RUNS;
    for (int i = 0; i < runs; i++) {
      final AsyncPromise<Void> promise = new DefaultAsyncFuture<Void>(false);
      final AsyncFutureListener<Void> listener1 = new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          listeners.add(this);
        }
      };
      final AsyncFutureListener<Void> listener2 = new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          listeners.add(this);
        }
      };
      final AsyncFutureListener<Void> listener4 = new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          listeners.add(this);
        }
      };
      final AsyncFutureListener<Void> listener3 = new AsyncFutureListener<Void>() {
        @Override
        public void operationComplete(AsyncFuture<Void> future) throws Exception {
          listeners.add(this);
          // Ensure listener4 is notified *after* this method returns to maintain the order.
          future.addListener(listener4);
        }
      };
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          promise.setSuccess(null);
        }
      });
      t.start();
      promise.addListener(listener1);
      promise.addListener(listener2);
      promise.addListener(listener3);

      // LIFO execution, except that adding listener to already completed future immediately executes.
      Assert.assertSame(listeners.take(), listener3, "Fail during run " + i + " / " + runs);
      Assert.assertSame(listeners.take(), listener4, "Fail during run " + i + " / " + runs);
      Assert.assertSame(listeners.take(), listener2, "Fail during run " + i + " / " + runs);
      Assert.assertSame(listeners.take(), listener1, "Fail during run " + i + " / " + runs);
      Assert.assertTrue(listeners.isEmpty(), "Fail during run " + i + " / " + runs);
      t.join();
    }
  }

  @Test(groups = { "unit" })
  public void testListenerNotifyLater() throws Exception {
    // Testing first execution path in DefaultPromise
    testListenerNotifyLater(1);
    // Testing second execution path in DefaultPromise
    testListenerNotifyLater(2);
  }

  private static void testListenerNotifyLater(final int numListenersBefore) throws Exception {
    int expectedCount = numListenersBefore + 2;
    final CountDownLatch latch = new CountDownLatch(expectedCount);
    final AsyncFutureListener<Void> listener = new AsyncFutureListener<Void>() {
      @Override
      public void operationComplete(AsyncFuture<Void> future) throws Exception {
        latch.countDown();
      }
    };
    final AsyncPromise<Void> promise = new DefaultAsyncFuture<Void>(false);
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < numListenersBefore; i++) {
          promise.addListener(listener);
        }
        promise.setSuccess(null);
        Thread t2 = new Thread(new Runnable() {
          @Override
          public void run() {
            promise.addListener(listener);
          }
        });
        t2.setDaemon(true);
        t2.start();
        promise.addListener(listener);
      }
    });
    t1.start();
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS), "Should have notifed " + expectedCount + " listeners");
    t1.join();
  }
}
