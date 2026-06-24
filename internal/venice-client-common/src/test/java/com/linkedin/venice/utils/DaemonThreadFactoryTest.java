package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class DaemonThreadFactoryTest {
  @Test
  public void testCreatesNamedDaemonThreads() {
    DaemonThreadFactory factory = new DaemonThreadFactory("TestPool");
    Thread first = factory.newThread(() -> {});
    Thread second = factory.newThread(() -> {});
    Assert.assertTrue(first.isDaemon(), "Threads should be daemons");
    Assert.assertEquals(first.getName(), "TestPool-t0");
    Assert.assertEquals(second.getName(), "TestPool-t1");
  }

  @Test
  public void testPriorityIsSetWhenSpecified() {
    int priority = Thread.NORM_PRIORITY - 1;
    DaemonThreadFactory factory = new DaemonThreadFactory("TestPool", priority, null);
    Thread t = factory.newThread(() -> {});
    Assert.assertEquals(t.getPriority(), priority, "Configured priority should be applied");
    Assert.assertTrue(t.isDaemon());
  }

  @Test
  public void testUnspecifiedPriorityInheritsFromCreatingThread() {
    DaemonThreadFactory factory = new DaemonThreadFactory("TestPool");
    Thread t = factory.newThread(() -> {});
    // With UNSPECIFIED_PRIORITY the factory does not call setPriority, so the thread inherits the creating thread's
    // priority (the JVM default behavior).
    Assert.assertEquals(t.getPriority(), Thread.currentThread().getPriority());
  }
}
