package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import java.sql.SQLException;
import java.util.EmptyStackException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class TestResourceRegistry {
  public static interface MockResource extends ShutdownableResource {
  }

  public static interface MockResource2 extends ShutdownableShim {
  }

  public static interface MockResourceFirst extends MockResource, ResourceRegistry.ShutdownFirst {
  }

  public static interface MockResourceLast extends MockResource, ResourceRegistry.ShutdownLast {
  }

  public static interface BadFactory extends ResourceRegistry.Factory, Comparable<MockResource> {
  }

  public static class BadClass implements ResourceRegistry.Factory {
  }

  public static interface FactoryStaticOtherException extends ResourceRegistry.Factory<MockResource> {
    MockResource fooFactory();

    static final ResourceRegistry.Factory<MockResource> BAD_FACTORY =
        ResourceRegistry.registerFactory(FactoryStaticOtherException.class, new FactoryStaticOtherException() {
          {
            ExceptionUtil.throwException(new SQLException("Some checked exception"));
          }

          @Override
          public MockResource fooFactory() {
            Assert.fail("Never gets here");
            return null;
          }
        });
  }

  public static interface FactoryStaticError extends ResourceRegistry.Factory<MockResource> {
    MockResource fooFactory();

    static final ResourceRegistry.Factory<MockResource> BAD_FACTORY =
        ResourceRegistry.registerFactory(FactoryStaticError.class, new FactoryStaticError() {
          {
            if (Math.cos(0) > 0.9)
              throw new Error("Bad foo always happens");
          }

          @Override
          public MockResource fooFactory() {
            Assert.fail("Never gets here");
            return null;
          }
        });
  }

  public static interface FactoryStaticRuntimeException extends ResourceRegistry.Factory<MockResource> {
    MockResource fooFactory();

    static final ResourceRegistry.Factory<MockResource> BAD_FACTORY =
        ResourceRegistry.registerFactory(FactoryStaticRuntimeException.class, new FactoryStaticRuntimeException() {
          {
            Integer.valueOf("foobarite");
          }

          @Override
          public MockResource fooFactory() {
            Assert.fail("Never gets here");
            return null;
          }
        });
  }

  public static interface FactoryInitStaticRace extends ResourceRegistry.Factory<MockResource> {
    MockResource fooFactory();

    static final ResourceRegistry.Factory<MockResource> RACE_FACTORY =
        ResourceRegistry.registerFactory(FactoryInitStaticRace.class, new FactoryInitStaticRace() {
          {
            Time.sleepInterruptable(100);
          }

          @Override
          public MockResource fooFactory() {
            Assert.fail("Never gets here");
            return null;
          }
        });
  }

  public static interface EmptyFactory extends ResourceRegistry.Factory<MockResource> {
  }

  public static interface InvalidFactory extends ResourceRegistry.Factory<MockResource> {
    Runnable invalidFactory();
  }

  public static interface NotRegisteredFactory extends ResourceRegistry.Factory<MockResource> {
  }

  public static interface ReregisterFactory extends ResourceRegistry.Factory<MockResource> {
    MockResource fooFactory();
  }

  public static interface MockFactory1 extends ResourceRegistry.Factory<MockResource> {
    MockResource factory();
  }

  public static final MockFactory1 factory1 = Mockito.mock(MockFactory1.class);

  public static interface MockFactory2 extends ResourceRegistry.Factory<MockResource> {
    MockResource factory();
  }

  public static interface MockFactory3 extends ResourceRegistry.Factory<MockResource> {
    MockResource factory();
  }

  public static interface MockFactory4 extends ResourceRegistry.Factory<MockResource> {
    MockResource factory();
  }

  public interface ShutdownableShim extends ShutdownableResource {
  }

  public interface FactoryShim<T extends ShutdownableShim> extends ResourceRegistry.Factory<T> {
  }

  public interface MockFactory5 extends FactoryShim<MockResource2> {
    MockResource2 factory();
  }

  public static final MockFactory5 factory5 = Mockito.mock(MockFactory5.class);

  public static interface MockFactoryGC extends ResourceRegistry.Factory<MockResource> {
    MockResource factory();
  }

  @BeforeMethod
  public void beforeMethod() {
    Mockito.reset(factory1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Factory<E> missing generic type")
  public void testRegisterBadFactory() {
    Assert.assertNull(ResourceRegistry.registerFactory(BadFactory.class, Mockito.mock(BadFactory.class)));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*BadClass is not an interface.*")
  public void testRegisterBadClass() {
    Assert.assertNull(ResourceRegistry.registerFactory(BadClass.class, new BadClass()));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Factory should be an instance of the Factory class.")
  @SuppressWarnings("unchecked")
  public void testRegisterBadMismatchFactory() {
    Assert.assertNull(
        ResourceRegistry.registerFactory((Class) EmptyFactory.class, (ResourceRegistry.Factory) new BadClass()));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*EmptyFactory does not declare any methods")
  public void testRegisterEmptyFactory() {
    Assert.assertNull(ResourceRegistry.registerFactory(EmptyFactory.class, Mockito.mock(EmptyFactory.class)));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*InvalidFactory does not return a \\[.*MockResource\\] class")
  public void testRegisterFactory() {
    Assert.assertNull(ResourceRegistry.registerFactory(InvalidFactory.class, Mockito.mock(InvalidFactory.class)));
  }

  @Test(groups = "unit")
  public void testRegisterRace() throws InterruptedException {
    final ResourceRegistry reg = new ResourceRegistry();
    try {
      final ConcurrentLinkedQueue<FactoryInitStaticRace> queue = new ConcurrentLinkedQueue<FactoryInitStaticRace>();
      final CountDownLatch latch = new CountDownLatch(1);
      Runnable race = new Runnable() {
        @Override
        public void run() {
          try {
            latch.await();

            // The constructor has a sleep so that should make these race within the reg.factory method

            queue.add(reg.factory(FactoryInitStaticRace.class));
          } catch (InterruptedException e) {
            Assert.fail("Interrupted", e);
          }
        }
      };

      Thread t1 = new Thread(race);
      Thread t2 = new Thread(race);
      Thread t3 = new Thread(race);
      t1.start();
      t2.start();
      t3.start();
      Time.sleep(10);
      latch.countDown();
      t1.join();
      t2.join();
      t3.join();

      Assert.assertEquals(queue.size(), 3);

      FactoryInitStaticRace f1 = queue.poll();
      FactoryInitStaticRace f2 = queue.poll();
      FactoryInitStaticRace f3 = queue.poll();

      Assert.assertSame(f1, f2);
      Assert.assertSame(f1, f3);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit", expectedExceptions = Error.class, expectedExceptionsMessageRegExp = "Bad foo always happens")
  public void testFactoryStaticError() {
    ResourceRegistry reg = new ResourceRegistry();
    FactoryStaticError factory = reg.factory(FactoryStaticError.class);
    Assert.fail("Should not get here: " + factory);
  }

  @Test(groups = "unit", expectedExceptions = NumberFormatException.class)
  public void testFactoryStaticRuntimeException() {
    ResourceRegistry reg = new ResourceRegistry();
    FactoryStaticRuntimeException factory = reg.factory(FactoryStaticRuntimeException.class);
    Assert.fail("Should not get here: " + factory);
  }

  @Test(groups = "unit", expectedExceptions = ExceptionInInitializerError.class)
  public void testFactoryStaticOtherException() {
    ResourceRegistry reg = new ResourceRegistry();
    FactoryStaticOtherException factory = reg.factory(FactoryStaticOtherException.class);
    Assert.fail("Should not get here: " + factory);
  }

  @Test(groups = "unit", expectedExceptions = NullPointerException.class)
  public void testFactoryNull() {
    ResourceRegistry reg = new ResourceRegistry();
    ResourceRegistry.Factory factory = reg.factory(null);
    Assert.fail("Should not get here: " + factory);
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory1() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource mockResource = Mockito.mock(MockResource.class);

    Mockito.when(factory1.factory()).thenReturn(mockResource);

    final AtomicInteger shutdown = new AtomicInteger();
    final CountDownLatch waitingLatch = new CountDownLatch(1);
    final CountDownLatch latch = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.incrementAndGet();
        return null;
      }
    }).when(mockResource).shutdown();

    Mockito.when(mockResource.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.get() != 0;
      }
    });

    Mockito.when(mockResource.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return latch.getCount() == 0;
      }
    });

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          throw new IllegalStateException();
        waitingLatch.countDown();
        latch.await();
        return null;
      }
    }).when(mockResource).waitForShutdown();

    Mockito.doThrow(TimeoutException.class).when(mockResource).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res = reg.factory(MockFactory1.class).factory();

      Assert.assertSame(res, mockResource);

      Assert.assertEquals(shutdown.get(), 0);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      waitingLatch.await();

      Assert.assertEquals(shutdown.get(), 1);
      Assert.assertFalse(reg.isTerminated());

      latch.countDown();

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
      Assert.assertEquals(shutdown.get(), 1);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory5() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory5.class, factory5), factory5);
    MockResource2 mockResource2 = Mockito.mock(MockResource2.class);

    Mockito.when(factory5.factory()).thenReturn(mockResource2);

    final AtomicInteger shutdown = new AtomicInteger();
    final CountDownLatch waitingLatch = new CountDownLatch(1);
    final CountDownLatch latch = new CountDownLatch(1);

    Mockito.doAnswer(invocation -> {
      shutdown.incrementAndGet();
      return null;
    }).when(mockResource2).shutdown();

    Mockito.when(mockResource2.isShutdown()).thenAnswer((Answer<Boolean>) invocation -> shutdown.get() != 0);

    Mockito.when(mockResource2.isTerminated()).thenAnswer((Answer<Boolean>) invocation -> latch.getCount() == 0);

    Mockito.doAnswer(invocation -> {
      if (shutdown.get() == 0)
        throw new IllegalStateException();
      waitingLatch.countDown();
      latch.await();
      return null;
    }).when(mockResource2).waitForShutdown();

    Mockito.doThrow(TimeoutException.class).when(mockResource2).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource2 res = reg.factory(MockFactory5.class).factory();

      Assert.assertSame(res, mockResource2);

      Assert.assertEquals(shutdown.get(), 0);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      waitingLatch.await();

      Assert.assertEquals(shutdown.get(), 1);
      Assert.assertFalse(reg.isTerminated());

      latch.countDown();

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
      Assert.assertEquals(shutdown.get(), 1);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory1BadWaitForShutdown() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource mockResource = Mockito.mock(MockResource.class);

    Mockito.when(factory1.factory()).thenReturn(mockResource);

    final CountDownLatch shutdown = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.countDown();
        Time.sleep(1);
        return null;
      }
    }).when(mockResource).shutdown();

    Mockito.when(mockResource.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.getCount() == 0;
      }
    });

    Mockito.when(mockResource.isTerminated()).thenReturn(false);

    Mockito.doThrow(NullPointerException.class).when(mockResource).waitForShutdown();
    Mockito.doThrow(NullPointerException.class).when(mockResource).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res = reg.factory(MockFactory1.class).factory();

      Assert.assertSame(res, mockResource);

      Assert.assertEquals(shutdown.getCount(), 1);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      shutdown.await();

      Assert.assertFalse(reg.isTerminated());

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory1BadShutdown() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource mockResource = Mockito.mock(MockResource.class);

    Mockito.when(factory1.factory()).thenReturn(mockResource);

    final CountDownLatch shutdown = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.countDown();
        throw new NullPointerException();
      }
    }).when(mockResource).shutdown();

    Mockito.when(mockResource.isShutdown()).thenReturn(false);

    Mockito.when(mockResource.isTerminated()).thenReturn(false);

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res = reg.factory(MockFactory1.class).factory();

      Assert.assertSame(res, mockResource);

      Assert.assertEquals(shutdown.getCount(), 1);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      shutdown.await();

      Assert.assertFalse(reg.isTerminated());

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory1Dormouse() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource mockResource = Mockito.mock(MockResource.class);

    Mockito.when(factory1.factory()).thenReturn(mockResource);

    final CountDownLatch shutdown = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.countDown();
        return null;
      }
    }).when(mockResource).shutdown();

    Mockito.when(mockResource.isShutdown()).thenReturn(false);

    Mockito.when(mockResource.isTerminated()).thenReturn(false);

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res = reg.factory(MockFactory1.class).factory();

      Assert.assertSame(res, mockResource);

      Assert.assertEquals(shutdown.getCount(), 1);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      shutdown.await();

      Assert.assertFalse(reg.isTerminated());

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRegisterMockitoFactory1Timeout() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource mockResource = Mockito.mock(MockResource.class);

    Mockito.when(factory1.factory()).thenReturn(mockResource);

    final AtomicInteger shutdown = new AtomicInteger();
    final CountDownLatch waitingLatch = new CountDownLatch(1);
    final CountDownLatch latch = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.incrementAndGet();
        return null;
      }
    }).when(mockResource).shutdown();

    Mockito.when(mockResource.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.get() != 0;
      }
    });

    Mockito.when(mockResource.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return latch.getCount() == 0;
      }
    });

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          throw new IllegalStateException();
        waitingLatch.countDown();
        latch.await();
        return null;
      }
    }).when(mockResource).waitForShutdown();

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          throw new IllegalStateException();
        waitingLatch.countDown();
        if (!Time.await(latch, (Long) invocation.getArguments()[0], TimeUnit.MILLISECONDS))
          throw new TimeoutException();
        return null;
      }
    }).when(mockResource).waitForShutdown(Mockito.anyLong());
    Mockito.doThrow(TimeoutException.class).when(mockResource).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res = reg.factory(MockFactory1.class).factory();

      Assert.assertSame(res, mockResource);

      Assert.assertEquals(shutdown.get(), 0);

      reg.shutdown();
      Assert.assertTrue(reg.isShutdown());

      waitingLatch.await();

      Assert.assertEquals(shutdown.get(), 1);
      Assert.assertFalse(reg.isTerminated());

      try {
        reg.waitForShutdown(100);
        Assert.fail("Should not get here");
      } catch (TimeoutException e) {
      }

      latch.countDown();

      reg.waitForShutdown();
      Assert.assertTrue(reg.isTerminated());
      Assert.assertEquals(shutdown.get(), 1);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testRemoveShutdownableResource() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();
    ShutdownableResource[] res = new ShutdownableResource[5];
    final CountDownLatch[] latch = new CountDownLatch[5];
    Shutdownable res3 = Mockito.mock(Shutdownable.class);
    Shutdownable res4 = Mockito.mock(Shutdownable.class);

    for (int i = 0; i < res.length; i++) {
      res[i] = Mockito.mock(ShutdownableResource.class);
      final CountDownLatch specificLatch = latch[i] = new CountDownLatch(1);

      if (i == 3)
        res3 = res[3];
      if (i == 4)
        res4 = res[4];

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          specificLatch.countDown();
          return null;
        }
      }).when(res[i]).shutdown();

      Mockito.when(res[i].isShutdown()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return specificLatch.getCount() == 0;
        }
      });

      Mockito.when(res[i].isTerminated()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return specificLatch.getCount() == 0;
        }
      });
    }

    Assert.assertEquals(reg.register(res[1]), res[1]);
    Assert.assertEquals(reg.register(res[2]), res[2]);
    Assert.assertEquals(reg.register(res3), res3);
    Assert.assertEquals(reg.register(res4), res4);

    reg.remove(res[0]);
    reg.remove(res[2]);
    reg.remove(res4);

    reg.shutdown();
    reg.waitForShutdown();

    Assert.assertEquals(latch[0].getCount(), 1);
    Assert.assertEquals(latch[1].getCount(), 0);
    Assert.assertEquals(latch[2].getCount(), 1);
    Assert.assertEquals(latch[3].getCount(), 0);
    Assert.assertEquals(latch[4].getCount(), 1);
  }

  @Test(groups = "unit")
  public void testRemoveShutdownable() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();
    Shutdownable[] res = new Shutdownable[4];
    final CountDownLatch[] latch = new CountDownLatch[4];

    for (int i = 0; i < res.length; i++) {
      res[i] = Mockito.mock(Shutdownable.class);
      final CountDownLatch specificLatch = latch[i] = new CountDownLatch(1);

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          specificLatch.countDown();
          return null;
        }
      }).when(res[i]).shutdown();
    }

    Assert.assertEquals(reg.register(res[1]), res[1]);
    Assert.assertEquals(reg.register(res[2]), res[2]);
    Assert.assertEquals(reg.register(res[3]), res[3]);

    reg.remove(res[0]);
    reg.remove(res[2]);

    reg.shutdown();
    reg.waitForShutdown();

    Assert.assertEquals(latch[0].getCount(), 1);
    Assert.assertEquals(latch[1].getCount(), 0);
    Assert.assertEquals(latch[2].getCount(), 1);
    Assert.assertEquals(latch[3].getCount(), 0);
  }

  @Test(groups = "unit")
  public void testRemoveSyncShutdownable() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();
    SyncShutdownable[] res = new SyncShutdownable[4];
    final CountDownLatch[] latch = new CountDownLatch[4];

    for (int i = 0; i < res.length; i++) {
      res[i] = Mockito.mock(SyncShutdownable.class);
      final CountDownLatch specificLatch = latch[i] = new CountDownLatch(1);

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          specificLatch.countDown();
          return null;
        }
      }).when(res[i]).shutdownSynchronously();
    }

    Assert.assertEquals(reg.register(res[1]), res[1]);
    Assert.assertEquals(reg.register(res[2]), res[2]);
    Assert.assertEquals(reg.register(res[3]), res[3]);

    reg.remove(res[0]);
    reg.remove(res[2]);

    reg.shutdown();
    reg.waitForShutdown();

    Assert.assertEquals(latch[0].getCount(), 1);
    Assert.assertEquals(latch[1].getCount(), 0);
    Assert.assertEquals(latch[2].getCount(), 1);
    Assert.assertEquals(latch[3].getCount(), 0);
  }

  @Test(groups = "unit")
  public void testSortedShutdownCombinations() throws InterruptedException, TimeoutException {
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory1.class, factory1), factory1);
    MockResource[] mockResources = { Mockito.mock(MockResourceFirst.class), Mockito.mock(MockResource.class),
        Mockito.mock(MockResourceLast.class) };

    final Queue<MockResource> factoryQueue = new LinkedList<MockResource>();

    Mockito.when(factory1.factory()).thenAnswer(new Answer<MockResource>() {
      @Override
      public MockResource answer(InvocationOnMock invocation) throws Throwable {
        return factoryQueue.remove();
      }
    });

    final BlockingQueue<Integer> shutdownOrder = new LinkedBlockingQueue<Integer>();
    final AtomicInteger[] shutdown = { new AtomicInteger(), new AtomicInteger(), new AtomicInteger() };
    final CountDownLatch[] latch = { null, null, null };

    for (int i = 0; i < 3; i++) {
      final int index = i;
      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          shutdownOrder.add(index);
          shutdown[index].incrementAndGet();
          return null;
        }
      }).when(mockResources[index]).shutdown();

      Mockito.when(mockResources[index].isShutdown()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return shutdown[index].get() != 0;
        }
      });

      Mockito.when(mockResources[index].isTerminated()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return latch[index].getCount() == 0;
        }
      });

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          if (shutdown[index].get() == 0)
            throw new IllegalStateException();
          latch[index].await();
          return null;
        }
      }).when(mockResources[index]).waitForShutdown();

      Mockito.doThrow(TimeoutException.class).when(mockResources[index]).waitForShutdown(Mockito.anyLong());
    }

    // setup done... lets run!

    int[][] combinations = { { 0, 1, 2 }, { 0, 2, 1 }, { 1, 0, 2 }, { 2, 0, 1 }, { 1, 2, 0 }, { 2, 1, 0 } };

    for (int[] combination: combinations) {
      Assert.assertTrue(factoryQueue.isEmpty());

      for (int i: combination)
        factoryQueue.add(mockResources[i]);

      shutdown[0].set(0);
      shutdown[1].set(0);
      shutdown[2].set(0);

      latch[0] = new CountDownLatch(1);
      latch[1] = new CountDownLatch(1);
      latch[2] = new CountDownLatch(1);

      ResourceRegistry reg = new ResourceRegistry(false);
      try {
        MockResource res0 = reg.factory(MockFactory1.class).factory();
        Assert.assertSame(res0, mockResources[combination[0]]);

        MockResource res1 = reg.factory(MockFactory1.class).factory();
        Assert.assertSame(res1, mockResources[combination[1]]);

        MockResource res2 = reg.factory(MockFactory1.class).factory();
        Assert.assertSame(res2, mockResources[combination[2]]);

        Assert.assertEquals(shutdown[0].get(), 0);
        Assert.assertEquals(shutdown[1].get(), 0);
        Assert.assertEquals(shutdown[2].get(), 0);

        reg.shutdown();
        Assert.assertTrue(reg.isShutdown());

        Assert.assertEquals(shutdownOrder.take(), Integer.valueOf(0));
        Time.sleep(10);
        Assert.assertTrue(shutdownOrder.isEmpty());
        Assert.assertEquals(shutdown[0].get(), 1);
        Assert.assertEquals(shutdown[1].get(), 0);
        Assert.assertEquals(shutdown[2].get(), 0);
        Assert.assertFalse(reg.isTerminated());
        latch[0].countDown();

        Assert.assertEquals(shutdownOrder.take(), Integer.valueOf(1));
        Time.sleep(10);
        Assert.assertTrue(shutdownOrder.isEmpty());
        Assert.assertEquals(shutdown[0].get(), 1);
        Assert.assertEquals(shutdown[1].get(), 1);
        Assert.assertEquals(shutdown[2].get(), 0);
        Assert.assertFalse(reg.isTerminated());
        latch[1].countDown();

        Assert.assertEquals(shutdownOrder.take(), Integer.valueOf(2));
        Time.sleep(10);
        Assert.assertTrue(shutdownOrder.isEmpty());
        Assert.assertEquals(shutdown[0].get(), 1);
        Assert.assertEquals(shutdown[1].get(), 1);
        Assert.assertEquals(shutdown[2].get(), 1);
        Assert.assertFalse(reg.isTerminated());
        latch[2].countDown();

        reg.waitForShutdown();
        Assert.assertTrue(reg.isTerminated());
      } finally {
        reg.shutdown();
      }
    }
  }

  @Test(groups = "unit")
  public void testReRegisterInvalidFactory() {
    ReregisterFactory factory = Mockito.mock(ReregisterFactory.class);
    Assert.assertSame(ResourceRegistry.registerFactory(ReregisterFactory.class, factory), factory);
    Assert.assertSame(
        ResourceRegistry.registerFactory(ReregisterFactory.class, Mockito.mock(ReregisterFactory.class)),
        factory);
    Assert.assertSame(ResourceRegistry.registerFactory(ReregisterFactory.class, factory), factory);
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Factory not registered for interface.*")
  public void testRunNotRegistered() {
    ResourceRegistry reg = new ResourceRegistry(false);
    try {
      reg.factory(NotRegisteredFactory.class);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testShutdownQuick() throws TimeoutException, InterruptedException {
    ResourceRegistry reg = new ResourceRegistry(false);
    Assert.assertFalse(reg.isShutdown());
    reg.shutdown();
    if (!reg.isTerminated()) {
      Assert.assertTrue(reg.isShutdown());
      reg.waitForShutdown(100);
      Assert.assertTrue(reg.isTerminated());
    }
  }

  @Test(groups = "unit")
  public void testToString() {
    ResourceRegistry reg = new ResourceRegistry();
    String str;
    Assert.assertNotNull(str = reg.toString());
    Assert.assertEquals(reg.toString(), str);
    Assert.assertNotSame(reg.toString(), str);
    reg.shutdown();
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testPrematureWaitForShutdown1() throws InterruptedException {
    ResourceRegistry reg = new ResourceRegistry();
    try {
      reg.waitForShutdown();
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
  public void testPrematureWaitForShutdown2() throws InterruptedException, TimeoutException {
    ResourceRegistry reg = new ResourceRegistry();
    try {
      reg.waitForShutdown(1000);
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testUnilateralGlobalShutdown() throws InterruptedException, TimeoutException {
    MockFactory2 factory = Mockito.mock(MockFactory2.class);
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory2.class, factory), factory);

    final Stack<MockResource> resources = new Stack<MockResource>();
    MockResource mockResource1 = Mockito.mock(MockResource.class);
    resources.add(mockResource1);
    MockResource mockResource2 = Mockito.mock(MockResource.class);
    resources.add(mockResource2);

    Mockito.when(factory.factory()).thenAnswer(new Answer<MockResource>() {
      @Override
      public MockResource answer(InvocationOnMock invocation) throws Throwable {
        return resources.pop();
      }
    });

    final AtomicInteger shutdown = new AtomicInteger();

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.incrementAndGet();
        return null;
      }
    }).when(mockResource1).shutdown();

    Mockito.when(mockResource1.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          return false;
        return shutdown.getAndIncrement() > 0;
      }
    });

    Mockito.when(mockResource1.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.get() > 5;
      }
    });

    final AtomicBoolean mock2Shutdown = new AtomicBoolean();

    Mockito.when(mockResource2.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.when(mockResource2.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          throw new IllegalStateException();
        while (shutdown.getAndIncrement() <= 5)
          Thread.yield();
        return null;
      }
    }).when(mockResource1).waitForShutdown();

    Mockito.doThrow(TimeoutException.class).when(mockResource1).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res2 = reg.factory(MockFactory2.class).factory();
      MockResource res1 = reg.factory(MockFactory2.class).factory();

      Assert.assertSame(res1, mockResource1);
      Assert.assertSame(res2, mockResource2);

      Assert.assertEquals(shutdown.get(), 0);

      mock2Shutdown.set(true);

      ResourceRegistry.globalShutdown();

      Assert.assertTrue(reg.isShutdown());

      while (shutdown.get() < 5)
        Thread.yield();

      Assert.assertTrue(reg.isTerminated());
    } finally {
      reg.shutdown();
    }

    // Test that exceptions in the factory are passed out correctly

    reg = new ResourceRegistry();
    try {
      try {
        reg.factory(MockFactory2.class).factory();
        Assert.fail("Should not get here");
      } catch (Throwable ex) {
        Assert.assertSame(ex.getClass(), EmptyStackException.class);
      }
    } finally {
      reg.shutdown();
    }

  }

  @Test(groups = "unit")
  public void testUnilateralGlobalShutdownInterruptable() throws InterruptedException, TimeoutException {
    MockFactory3 factory = Mockito.mock(MockFactory3.class);
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory3.class, factory), factory);

    final Stack<MockResource> resources = new Stack<MockResource>();
    MockResource mockResource1 = Mockito.mock(MockResource.class);
    resources.add(mockResource1);
    MockResource mockResource2 = Mockito.mock(MockResource.class);
    resources.add(mockResource2);

    Mockito.when(factory.factory()).thenAnswer(new Answer<MockResource>() {
      @Override
      public MockResource answer(InvocationOnMock invocation) throws Throwable {
        return resources.pop();
      }
    });

    final CountDownLatch shutdown = new CountDownLatch(1);
    final CountDownLatch terminated = new CountDownLatch(1);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.countDown();
        return null;
      }
    }).when(mockResource1).shutdown();

    Mockito.when(mockResource1.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.getCount() == 0;
      }
    });

    Mockito.when(mockResource1.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return terminated.getCount() == 0;
      }
    });

    final AtomicBoolean mock2Shutdown = new AtomicBoolean();

    Mockito.when(mockResource2.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.when(mockResource2.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        terminated.await();
        return null;
      }
    }).when(mockResource1).waitForShutdown();

    Mockito.doThrow(TimeoutException.class).when(mockResource1).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res2 = reg.factory(MockFactory3.class).factory();
      MockResource res1 = reg.factory(MockFactory3.class).factory();

      Assert.assertSame(res1, mockResource1);
      Assert.assertSame(res2, mockResource2);

      mock2Shutdown.set(true);

      RunnableFuture<Void> future = new FutureTask<Void>(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          ResourceRegistry.globalShutdown();
          return null;
        }
      });

      Thread t = new Thread(future);
      t.start();

      shutdown.await();

      t.interrupt();

      try {
        future.get();
        Assert.fail("Should not get here");
      } catch (ExecutionException e) {
        Assert.assertEquals(e.getCause().getClass(), InterruptedException.class);
      }

      terminated.countDown();
    } finally {
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testUnilateralGlobalShutdownDelay() throws InterruptedException, TimeoutException {
    MockFactory4 factory = Mockito.mock(MockFactory4.class);
    Assert.assertSame(ResourceRegistry.registerFactory(MockFactory4.class, factory), factory);

    final Stack<MockResource> resources = new Stack<MockResource>();
    MockResource mockResource1 = Mockito.mock(MockResource.class);
    resources.add(mockResource1);
    MockResource mockResource2 = Mockito.mock(MockResource.class);
    resources.add(mockResource2);

    Mockito.when(factory.factory()).thenAnswer(new Answer<MockResource>() {
      @Override
      public MockResource answer(InvocationOnMock invocation) throws Throwable {
        return resources.pop();
      }
    });

    final AtomicInteger shutdown = new AtomicInteger();

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        shutdown.incrementAndGet();
        return null;
      }
    }).when(mockResource1).shutdown();

    Mockito.when(mockResource1.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          return false;
        return shutdown.getAndIncrement() > 0;
      }
    });

    Mockito.when(mockResource1.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return shutdown.get() > 5;
      }
    });

    final AtomicBoolean mock2Shutdown = new AtomicBoolean();

    Mockito.when(mockResource2.isShutdown()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.when(mockResource2.isTerminated()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return mock2Shutdown.get();
      }
    });

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (shutdown.get() == 0)
          throw new IllegalStateException();
        while (shutdown.getAndIncrement() <= 5)
          Thread.yield();
        return null;
      }
    }).when(mockResource1).waitForShutdown();

    Mockito.doThrow(TimeoutException.class).when(mockResource1).waitForShutdown(Mockito.anyLong());

    // setup done... lets run!

    ResourceRegistry reg = new ResourceRegistry();
    try {
      MockResource res2 = reg.factory(MockFactory4.class).factory();
      MockResource res1 = reg.factory(MockFactory4.class).factory();

      Assert.assertSame(res1, mockResource1);
      Assert.assertSame(res2, mockResource2);

      Assert.assertEquals(shutdown.get(), 0);

      mock2Shutdown.set(true);

      ResourceRegistry.setGlobalShutdownDelayMillis(1000);
      long shutdownStart = Time.currentTimeMillis();
      ResourceRegistry.globalShutdown();

      Assert.assertTrue(reg.isShutdown());

      while (shutdown.get() < 5)
        Thread.yield();

      Assert.assertTrue(
          Time.currentTimeMillis() - shutdownStart >= 1000,
          "globalShutdown did not wait _globalShutdownDelayMillis to shutdown.");
      Assert.assertTrue(reg.isTerminated());
    } finally {
      ResourceRegistry.setGlobalShutdownDelayMillis(0);
      reg.shutdown();
    }
  }

  @Test(groups = "unit")
  public void testResourceGarbageCollection() throws InterruptedException, TimeoutException {
    try {
      MockFactoryGC factoryGC = Mockito.mock(MockFactoryGC.class);
      Assert.assertSame(ResourceRegistry.registerFactory(MockFactoryGC.class, factoryGC), factoryGC);

      MockResource mockResource = Mockito.mock(MockResource.class);
      Mockito.when(factoryGC.factory()).thenReturn(mockResource);

      final CountDownLatch shutdown = new CountDownLatch(1);
      final CountDownLatch terminated = new CountDownLatch(1);

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          shutdown.countDown();
          return null;
        }
      }).when(mockResource).shutdown();

      Mockito.when(mockResource.isShutdown()).thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          return shutdown.getCount() == 0;
        }
      });

      Mockito.when(mockResource.isTerminated()).thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          return terminated.getCount() == 0;
        }
      });

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          terminated.await();
          return null;
        }
      }).when(mockResource).waitForShutdown();

      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          if (!terminated.await((Long) invocation.getArguments()[0], TimeUnit.MILLISECONDS))
            throw new TimeoutException();
          return null;
        }
      }).when(mockResource).waitForShutdown(Mockito.anyLong());

      // Test Simple GC

      AtomicReference<ResourceRegistry> reg = new AtomicReference<ResourceRegistry>(new ResourceRegistry(true));

      MockResource res = reg.get().factory(MockFactoryGC.class).factory();
      Assert.assertEquals(String.valueOf(res), String.valueOf(mockResource));

      reg.set(null); // Oops! We lost it!
      Assert.assertNull(reg.get());

      System.gc();
      Assert.assertTrue(shutdown.await(1000, TimeUnit.MILLISECONDS));

      terminated.countDown();
    } finally {
    }
  }

  @Test(groups = "unit")
  public void testWaitToStartShutdown() throws InterruptedException, TimeoutException, ExecutionException {
    final ResourceRegistry registry = new ResourceRegistry();
    final RunnableFuture<?> future = new FutureTask<Object>(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        registry.waitToStartShutdown();
        return null;
      }
    });

    new Thread(future).start();

    try {
      future.get(10, TimeUnit.MILLISECONDS);
      Assert.fail();
    } catch (TimeoutException ignored) {
      // expected
    }

    registry.shutdown();

    future.get(1, TimeUnit.MILLISECONDS);
  }

  @Test(groups = "unit")
  public void testWaitToStartShutdownTimeout() throws InterruptedException, TimeoutException, ExecutionException {
    final ResourceRegistry registry = new ResourceRegistry();
    final CountDownLatch startLatch = new CountDownLatch(2);
    final RunnableFuture<?> future1 = new FutureTask<Object>(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        startLatch.countDown();
        startLatch.await();
        registry.waitToStartShutdown(100);
        return null;
      }
    });
    final RunnableFuture<?> future2 = new FutureTask<Object>(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        startLatch.countDown();
        startLatch.await();
        registry.waitToStartShutdown(150);
        return null;
      }
    });

    new Thread(future1).start();
    new Thread(future2).start();
    startLatch.await();

    try {
      future1.get(20, TimeUnit.MILLISECONDS);
      Assert.fail();
    } catch (TimeoutException ignored) {
      // expected
    }

    try {
      future1.get(100, TimeUnit.MILLISECONDS);
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof TimeoutException);
    }

    registry.shutdown();

    future2.get(1, TimeUnit.MILLISECONDS);
  }
}
