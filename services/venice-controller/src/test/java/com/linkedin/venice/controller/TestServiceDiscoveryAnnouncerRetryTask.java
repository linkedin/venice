package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.Utils.sleep;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceDiscoveryAnnouncerRetryTask {
  private static final Logger LOGGER = LogManager.getLogger(TestServiceDiscoveryAnnouncerRetryTask.class);

  @Test
  public void testOneFailedTask() {
    BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue = new LinkedBlockingQueue<>();
    ServiceDiscoveryAnnouncerRetryTask task = new ServiceDiscoveryAnnouncerRetryTask(retryQueue);
    Thread thread = new Thread(task);
    LOGGER.info("Starting thread");
    thread.start();
    LOGGER.info("Thread started");
    MockServiceDiscoveryAnnouncer announcer = new MockServiceDiscoveryAnnouncer("test", true);
    Assert.assertEquals(retryQueue.size(), 0);
    Assert.assertTrue(announcer.getShouldFail());
    if (announcer.getShouldFail()) {
      announcer.register();
      LOGGER.info("Service discovery \"{}\" failed so adding it to the retry queue", announcer.getName());
      retryQueue.add(announcer);
    }
    Assert.assertFalse(announcer.getShouldFail());
    Assert.assertEquals(retryQueue.size(), 1);
    LOGGER.info("Sleeping for 25 seconds");
    sleep(25000L);
    Assert.assertEquals(retryQueue.size(), 1);
    LOGGER.info("Sleeping for 10 seconds");
    sleep(10000L);
    Assert.assertEquals(retryQueue.size(), 0);
    LOGGER.info("Stopping thread");
    thread.interrupt();
    LOGGER.info("Thread stopped");
  }

  @Test
  public void testMultipleFailedTasks() {
    BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue = new LinkedBlockingQueue<>();
    ServiceDiscoveryAnnouncerRetryTask task = new ServiceDiscoveryAnnouncerRetryTask(retryQueue);
    Thread thread = new Thread(task);
    LOGGER.info("Starting thread");
    thread.start();
    LOGGER.info("Thread started");
    MockServiceDiscoveryAnnouncer announcer1 = new MockServiceDiscoveryAnnouncer("test1", true);
    MockServiceDiscoveryAnnouncer announcer2 = new MockServiceDiscoveryAnnouncer("test2", true);
    MockServiceDiscoveryAnnouncer announcer3 = new MockServiceDiscoveryAnnouncer("test3", true);
    List<MockServiceDiscoveryAnnouncer> announcers = Arrays.asList(announcer1, announcer2, announcer3);
    Assert.assertEquals(retryQueue.size(), 0);
    Assert.assertTrue(announcer1.getShouldFail());
    Assert.assertTrue(announcer2.getShouldFail());
    Assert.assertTrue(announcer3.getShouldFail());
    for (MockServiceDiscoveryAnnouncer announcer: announcers) {
      if (announcer.getShouldFail()) {
        announcer.register();
        LOGGER.info("Service discovery \"{}\" failed so adding it to the retry queue", announcer.getName());
        retryQueue.add(announcer);
      }
    }
    Assert.assertFalse(announcer1.getShouldFail());
    Assert.assertFalse(announcer2.getShouldFail());
    Assert.assertFalse(announcer3.getShouldFail());
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 3);
    LOGGER.info("Sleeping for 25 seconds");
    sleep(25000L);
    Assert.assertEquals(retryQueue.size(), 3);
    LOGGER.info("Sleeping for 10 seconds");
    sleep(10000L);
    Assert.assertEquals(retryQueue.peek(), announcer2);
    Assert.assertEquals(retryQueue.size(), 2);
    LOGGER.info("Sleeping for 20 seconds");
    sleep(20000L);
    Assert.assertEquals(retryQueue.size(), 2);
    LOGGER.info("Sleeping for 10 seconds");
    sleep(10000L);
    Assert.assertEquals(retryQueue.peek(), announcer3);
    Assert.assertEquals(retryQueue.size(), 1);
    LOGGER.info("Sleeping for 20 seconds");
    sleep(20000L);
    Assert.assertEquals(retryQueue.size(), 1);
    LOGGER.info("Sleeping for 10 seconds");
    sleep(10000L);
    Assert.assertEquals(retryQueue.size(), 0);
    LOGGER.info("Stopping thread");
    thread.interrupt();
    LOGGER.info("Thread stopped");
  }

  /**
   * This class mocks {@link ServiceDiscoveryAnnouncer} for testing purposes.
   */
  static class MockServiceDiscoveryAnnouncer implements ServiceDiscoveryAnnouncer {
    private String name;
    private boolean shouldFail;

    public MockServiceDiscoveryAnnouncer(String name, boolean shouldFail) {
      this.name = name;
      this.shouldFail = shouldFail;
    }

    @Override
    public void register() {
      if (shouldFail) {
        shouldFail = false;
        LOGGER.info("Failed to register to service discovery: \"{}\"", name);
      }
    }

    @Override
    public void unregister() {
      // no-op
    }

    public String getName() {
      return name;
    }

    public boolean getShouldFail() {
      return shouldFail;
    }
  }
}
