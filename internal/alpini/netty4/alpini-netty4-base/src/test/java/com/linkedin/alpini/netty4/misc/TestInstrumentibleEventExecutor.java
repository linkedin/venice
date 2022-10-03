package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.Time;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.StreamSupport;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestInstrumentibleEventExecutor {
  public void simpleTest() throws InterruptedException {

    EventExecutorGroup group = new DefaultEventExecutorGroup(4);
    try {
      LongAdder onSubmit = new LongAdder();
      LongAdder onSchedule = new LongAdder();
      LongAdder onExec = new LongAdder();
      LongAdder onComplete = new LongAdder();

      EventExecutorGroup test = new InstrumentibleEventExecutor(group) {
        @Override
        protected void onSubmit(Completion completion) {
          onSubmit.increment();
          super.onSubmit(completion);
        }

        @Override
        protected void onSchedule(Completion completion) {
          onSchedule.increment();
          super.onSchedule(completion);
        }

        @Override
        protected void onExec(Completion completion) {
          onExec.increment();
          super.onExec(completion);
        }

        @Override
        protected void onComplete(Completion completion, boolean isSuccess) {
          onComplete.increment();
          super.onComplete(completion, isSuccess);
        }
      };

      try {

        Assert.assertFalse(test.next().isShutdown());
        Assert.assertFalse(test.next().isShuttingDown());
        Assert.assertFalse(test.next().isTerminated());
        Assert.assertNotNull(test.next().terminationFuture());
        Assert.assertFalse(test.next().inEventLoop());

        test.execute(() -> {});
        Assert.assertEquals(onSubmit.intValue(), 1);
        Time.sleep(100);
        Assert.assertEquals(onExec.intValue(), 1);
        Assert.assertEquals(onComplete.intValue(), 1);

        Future<Integer> intFuture = test.submit(() -> {}, 42);
        Assert.assertEquals(onSubmit.intValue(), 2);
        intFuture.sync();
        Assert.assertEquals(onExec.intValue(), 2);
        Time.sleep(100);
        Assert.assertEquals(onComplete.intValue(), 2);

        intFuture = test.submit(() -> 42);
        Assert.assertEquals(onSubmit.intValue(), 3);
        intFuture.sync();
        Assert.assertEquals(onExec.intValue(), 3);
        Time.sleep(100);
        Assert.assertEquals(onComplete.intValue(), 3);

        ScheduledFuture<Integer> schedFuture = test.schedule(() -> 42, 100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onSchedule.intValue(), 1);
        Assert.assertEquals(onExec.intValue(), 3);
        schedFuture.sync();
        Assert.assertEquals(onExec.intValue(), 4);
        Time.sleep(100);
        Assert.assertEquals(onComplete.intValue(), 4);

        ScheduledFuture<?> schedFuture2 = test.schedule(() -> {}, 100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onSchedule.intValue(), 2);
        Assert.assertEquals(onExec.intValue(), 4);
        schedFuture2.sync();
        Assert.assertEquals(onExec.intValue(), 5);
        Time.sleep(100);
        Assert.assertEquals(onComplete.intValue(), 5);

        schedFuture2 = test.scheduleAtFixedRate(() -> {}, 10, 100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onSchedule.intValue(), 3);
        while (onComplete.intValue() < 10) {
          Thread.yield();
        }
        Assert.assertTrue(schedFuture2.cancel(false));
        Assert.assertEquals(onExec.intValue(), 10);
        Assert.assertEquals(onComplete.intValue(), 10);

        schedFuture2 = test.scheduleWithFixedDelay(() -> {}, 10, 100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onSchedule.intValue(), 4);
        while (onComplete.intValue() < 20) {
          Thread.yield();
        }
        Assert.assertTrue(schedFuture2.cancel(false));
        Assert.assertEquals(onExec.intValue(), 20);
        Assert.assertEquals(onComplete.intValue(), 20);

        Assert.assertEquals(StreamSupport.stream(test.spliterator(), false).count(), 4L);

      } finally {
        Assert.assertFalse(test.isShuttingDown());
        test.shutdownGracefully();
        Assert.assertTrue(test.isShuttingDown());
        Assert.assertTrue(test.awaitTermination(20, TimeUnit.SECONDS));
        test.terminationFuture().sync();
        Assert.assertTrue(test.isShutdown());
        Assert.assertTrue(test.isTerminated());
        test.shutdown();
      }

      for (EventExecutor executor: test) {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(executor.isTerminated());
      }
      ;

    } finally {
      group.shutdownGracefully().sync();
    }
  }

}
