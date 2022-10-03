package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTimerTimeoutProcessor {
  @Test(groups = "unit")
  public void testExecute() throws InterruptedException {
    HashedWheelTimer timer = new HashedWheelTimer();
    RouterTimeoutProcessor timeoutProcessor = new TimerTimeoutProcessor(timer);

    CountDownLatch latch = new CountDownLatch(1);
    timeoutProcessor.execute(latch::countDown);

    // expect immediate but Timer has 100 ms granuality
    Assert.assertTrue(latch.await(205, TimeUnit.MILLISECONDS));

    timer.stop();
  }

  @Test(groups = "unit")
  public void testSchedule() throws InterruptedException {
    HashedWheelTimer timer = new HashedWheelTimer();
    RouterTimeoutProcessor timeoutProcessor = new TimerTimeoutProcessor(timer);

    {
      CountDownLatch latch = new CountDownLatch(1);

      long startTime = Time.nanoTime();
      RouterTimeoutProcessor.TimeoutFuture future = timeoutProcessor.schedule(latch::countDown, 1, TimeUnit.SECONDS);
      Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
      float totalTime = Time.nanoTime() - startTime;

      // expect error of up to 105 ms because default Timer has 100 ms granuality
      Assert.assertEquals(totalTime / TimeUnit.SECONDS.toNanos(1), 1.0f, 0.205f);

      Assert.assertFalse(future.cancel());
    }

    {
      CountDownLatch latch = new CountDownLatch(1);

      RouterTimeoutProcessor.TimeoutFuture future = timeoutProcessor.schedule(latch::countDown, 1, TimeUnit.SECONDS);
      Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
      Assert.assertTrue(future.cancel());
      Assert.assertFalse(latch.await(2, TimeUnit.SECONDS));
    }

    timer.stop();
  }

  @Test(groups = "unit")
  public void testUnwrap() {
    HashedWheelTimer timer = new HashedWheelTimer();
    RouterTimeoutProcessor timeoutProcessor = new TimerTimeoutProcessor(timer);
    Assert.assertSame(timeoutProcessor.unwrap(Timer.class), timer);
    timer.stop();
  }
}
