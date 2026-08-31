package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;


public class NamedThreadFactoryTest {
  @Test
  public void testNamedThreadFactorySetsExplicitPriorityWithoutChangingDaemonBehavior() throws Exception {
    NamedThreadFactory factory = new NamedThreadFactory("TestNamedThread", Thread.NORM_PRIORITY - 1);
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);

    Thread thread = factory.newThread(() -> {
      executed.set(true);
      latch.countDown();
    });

    assertEquals(thread.getName(), "TestNamedThread-0");
    assertEquals(thread.getPriority(), Thread.NORM_PRIORITY - 1);
    assertFalse(thread.isDaemon());

    thread.start();
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertTrue(executed.get());
  }
}
