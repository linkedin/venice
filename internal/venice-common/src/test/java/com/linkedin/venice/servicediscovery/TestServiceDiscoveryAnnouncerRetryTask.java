package com.linkedin.venice.servicediscovery;

import static com.linkedin.venice.utils.Utils.sleep;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.utils.Time;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
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
   * 1) {@link AsyncRetryingServiceDiscoveryAnnouncer#register()} is called. The result of the call is that announcer3
   * successfully registers, and announcer1 and announcer2 fail to register, so retryQueue is [announcer1, announcer2] <br>
   * 2) Retry thread starts and retries registering the announcers in retryQueue <br>
   * 3) After 30 seconds, announcer1 retries registering and fails, so retryQueue is [announcer2, announcer1] <br>
   * 4) After 30 seconds, announcer2 retries registering and successfully registers, so retryQueue is [announcer1] <br>
   * 5) After 30 seconds, announcer1 retries registering and successfully registers, so retryQueue is []
   */
  @Test
  public void testRegisterServiceDiscoveryAnnouncers() {
    reset(announcer1, announcer2, announcer3, config);
    doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(announcer1).register();
    doThrow(new RuntimeException()).doNothing().when(announcer2).register();
    doNothing().when(announcer3).register();
    doReturn(30L * Time.MS_PER_SECOND).when(config).getServiceDiscoveryRegistrationRetryMS();
    long serviceDiscoveryRegistrationRetryMS = config.getServiceDiscoveryRegistrationRetryMS();
    List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers = Arrays.asList(announcer1, announcer2, announcer3);
    AsyncRetryingServiceDiscoveryAnnouncer asyncRetryingServiceDiscoveryAnnouncer =
        new AsyncRetryingServiceDiscoveryAnnouncer(serviceDiscoveryAnnouncers, serviceDiscoveryRegistrationRetryMS);
    BlockingQueue<ServiceDiscoveryAnnouncer> retryQueue =
        asyncRetryingServiceDiscoveryAnnouncer.getServiceDiscoveryAnnouncerRetryQueue();
    try {
      asyncRetryingServiceDiscoveryAnnouncer.register();
    } catch (Exception e) {
      Assert.fail("The method call should not throw an exception.");
    }

    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 2);

    putTestToSleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertEquals(retryQueue.peek(), announcer2);
    Assert.assertEquals(retryQueue.size(), 2);

    putTestToSleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertFalse(retryQueue.contains(announcer2));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 1);

    putTestToSleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(retryQueue.contains(announcer1));
    Assert.assertEquals(retryQueue.size(), 0);
  }

  @Test
  public void testUnregisterServiceDiscoveryAnnouncers() {
    reset(announcer1, announcer2, announcer3, config);
    doThrow(new RuntimeException()).when(announcer1).unregister();
    doNothing().when(announcer2).unregister();
    doNothing().when(announcer3).unregister();
    doReturn(30L * Time.MS_PER_SECOND).when(config).getServiceDiscoveryRegistrationRetryMS();
    long serviceDiscoveryRegistrationRetryMS = config.getServiceDiscoveryRegistrationRetryMS();
    List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers = Arrays.asList(announcer1, announcer2, announcer3);
    AsyncRetryingServiceDiscoveryAnnouncer asyncRetryingServiceDiscoveryAnnouncer =
        new AsyncRetryingServiceDiscoveryAnnouncer(serviceDiscoveryAnnouncers, serviceDiscoveryRegistrationRetryMS);
    try {
      asyncRetryingServiceDiscoveryAnnouncer.unregister();
    } catch (Exception e) {
      Assert.fail("The method call should not throw an exception.");
    }
  }

  private void putTestToSleep(long ms) {
    LOGGER.info("Test sleeping for {} ms", ms);
    sleep(ms);
    LOGGER.info("Test woke up");
  }
}
