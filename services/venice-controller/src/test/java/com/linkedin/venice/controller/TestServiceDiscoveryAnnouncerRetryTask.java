package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.Utils.sleep;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceDiscoveryAnnouncerRetryTask {
  private static final Logger LOGGER = LogManager.getLogger(TestServiceDiscoveryAnnouncerRetryTask.class);
  private ServiceDiscoveryAnnouncer announcer1 = mock(ServiceDiscoveryAnnouncer.class);
  private ServiceDiscoveryAnnouncer announcer2 = mock(ServiceDiscoveryAnnouncer.class);
  private ServiceDiscoveryAnnouncer announcer3 = mock(ServiceDiscoveryAnnouncer.class);
  private VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);

  /**
   * Below is the expected workflow of the test: <br>
   * 1) {@link ServiceDiscoveryAnnouncerHelper#registerServiceDiscoveryAnnouncers(List, BlockingQueue)} is called.
   * The result of the call is that announcer3 successfully registers, and announcer1 and announcer2 fail to register,
   * so retryQueue is [announcer1, announcer2] <br>
   * 2) Retry thread starts and retries registering the announcers in retryQueue <br>
   * 3) After 30 seconds, announcer1 retries registering and fails, so retryQueue is [announcer2, announcer1] <br>
   * 4) After 30 seconds, announcer2 retries registering and successfully registers, so retryQueue is [announcer1] <br>
   * 5) After 30 seconds, announcer1 retries registering and successfully registers, so retryQueue is []
   */
  @Test
  public void testRegisterServiceDiscoveryAnnouncers() {
    reset(announcer1, announcer2, announcer3);
    doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(announcer1).register();
    doThrow(new RuntimeException()).doNothing().when(announcer2).register();
    doNothing().when(announcer3).register();
    doReturn(30000L).when(config).getRetryRegisterServiceDiscoveryAnnouncerMS();
    long retryRegisterServiceDiscoveryAnnouncerMS = config.getRetryRegisterServiceDiscoveryAnnouncerMS();
    List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers = Arrays.asList(announcer1, announcer2, announcer3);
    BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue = new LinkedBlockingQueue<>();
    ServiceDiscoveryAnnouncerHelper.registerServiceDiscoveryAnnouncers(serviceDiscoveryAnnouncers, retryQueue);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 2);

    ServiceDiscoveryAnnouncerRetryTask retryTask =
        new ServiceDiscoveryAnnouncerRetryTask(retryQueue, retryRegisterServiceDiscoveryAnnouncerMS);
    Thread retryThread = new Thread(retryTask);
    LOGGER.info("Starting retry thread");
    retryThread.start();
    LOGGER.info("Retry thread started");

    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 2);

    putTestToSleep(retryRegisterServiceDiscoveryAnnouncerMS + 1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertEquals(retryQueue.peek(), announcer2);
    Assert.assertEquals(retryQueue.size(), 2);

    putTestToSleep(retryRegisterServiceDiscoveryAnnouncerMS + 1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertFalse(retryQueue.contains(announcer2));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 1);

    putTestToSleep(retryRegisterServiceDiscoveryAnnouncerMS + 1000);
    Assert.assertFalse(retryQueue.contains(announcer1));
    Assert.assertEquals(retryQueue.size(), 0);

    LOGGER.info("Stopping retry thread");
    retryThread.interrupt();
    LOGGER.info("Retry thread stopped");
  }

  @Test
  public void testUnregisterServiceDiscoveryAnnouncers() {
    doThrow(new RuntimeException()).when(announcer1).unregister();
    doNothing().when(announcer2).unregister();
    doNothing().when(announcer3).unregister();
    List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers = Arrays.asList(announcer1, announcer2, announcer3);
    Map<ServiceDiscoveryAnnouncer, String> map =
        ServiceDiscoveryAnnouncerHelper.unregisterServiceDiscoveryAnnouncers(serviceDiscoveryAnnouncers);
    Assert.assertEquals(map.get(announcer1), "Failed to unregister from service discovery");
    Assert.assertEquals(map.get(announcer2), "Unregistered from service discovery");
    Assert.assertEquals(map.get(announcer3), "Unregistered from service discovery");
  }

  private void putTestToSleep(long ms) {
    LOGGER.info("Test sleeping for {} ms", ms);
    sleep(ms);
    LOGGER.info("Test woke up");
  }
}
