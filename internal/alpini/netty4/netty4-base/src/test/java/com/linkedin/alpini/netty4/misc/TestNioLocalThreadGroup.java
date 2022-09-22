package com.linkedin.alpini.netty4.misc;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 12/12/16.
 */
public class TestNioLocalThreadGroup {
  private ConcurrentHashMap<Long, Data> _visitorMap = new ConcurrentHashMap<>();
  private CountDownLatch _latch;

  @Test(groups = "unit")
  public void testBasic1() throws InterruptedException {
    testBasic(1);
  }

  @Test(groups = "unit")
  public void testBasic8() throws InterruptedException {
    testBasic(8);
  }

  private void testBasic(int nThreads) throws InterruptedException {
    // org.apache.log4j.BasicConfigurator.configure();

    NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(nThreads);
    try {

      SingleThreadEventLoopGroupSupplier test = new LocalThreadEventLoopGroup<>(eventLoopGroup);

      _visitorMap.clear();
      _latch = new CountDownLatch(1000);
      executeTest1(test);
      _latch.await();

      Assert.assertEquals(_visitorMap.size(), nThreads);

      int success = 0;
      for (EventExecutor anEventLoopGroup: eventLoopGroup) {
        _visitorMap.clear();
        _latch = new CountDownLatch(1000);
        anEventLoopGroup.submit(() -> executeTest1(test));
        _latch.await();
        Assert.assertEquals(_visitorMap.size(), 1);
        Assert.assertEquals(_visitorMap.values().iterator().next()._count, 1000);
        success++;
      }
      Assert.assertEquals(success, nThreads);

      for (EventExecutor anEventLoopGroup: eventLoopGroup) {
        _visitorMap.clear();
        _latch = new CountDownLatch(1000);
        anEventLoopGroup.submit(() -> executeTest2(test, eventLoopGroup));
        _latch.await();
        Assert.assertEquals(_visitorMap.size(), 1);
        Assert.assertEquals(_visitorMap.values().iterator().next()._count, 3000);
        success++;
      }
      Assert.assertEquals(success, 2 * nThreads);
    } finally {
      eventLoopGroup.shutdownGracefully().await();
    }
  }

  private void executeTest2(SingleThreadEventLoopGroupSupplier test, NioEventLoopGroup actual) {
    _visitorMap.computeIfAbsent(Thread.currentThread().getId(), Data::new)._count = 2000;
    EventLoopGroup single = test.singleThreadGroup().getNow();
    for (long i = _latch.getCount(); i > 0; i--) {
      actual.next().execute(() -> single.next().submit(this::test1));
    }
  }

  private void executeTest1(EventLoopGroup test) {
    for (long i = _latch.getCount(); i > 0; i--) {
      test.next().submit(this::test1);
    }
  }

  private void test1() {
    _visitorMap.computeIfAbsent(Thread.currentThread().getId(), Data::new)._count++;
    _latch.countDown();
  }

  private static class Data {
    Data(Long threadId) {
    }

    int _count;
  }
}
