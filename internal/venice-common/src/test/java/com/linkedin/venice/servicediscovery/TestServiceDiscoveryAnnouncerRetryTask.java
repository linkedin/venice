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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceDiscoveryAnnouncerRetryTask {
  private ServiceDiscoveryAnnouncer announcer1 = mock(ServiceDiscoveryAnnouncer.class);
  private ServiceDiscoveryAnnouncer announcer2 = mock(ServiceDiscoveryAnnouncer.class);
  private ServiceDiscoveryAnnouncer announcer3 = mock(ServiceDiscoveryAnnouncer.class);
  private VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);

  @Test
  public void testThreadMethodsDoNotRunIfRetryQueueEmpty() {
    reset(announcer1, announcer2, announcer3, config);
    doNothing().when(announcer1).register();
    doNothing().when(announcer2).register();
    doNothing().when(announcer3).register();
    doNothing().when(announcer1).unregister();
    doNothing().when(announcer2).unregister();
    doNothing().when(announcer3).unregister();
    doReturn(30L * Time.MS_PER_SECOND).when(config).getServiceDiscoveryRegistrationRetryMS();
    long serviceDiscoveryRegistrationRetryMS = config.getServiceDiscoveryRegistrationRetryMS();
    List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers = Arrays.asList(announcer1, announcer2, announcer3);
    AsyncRetryingServiceDiscoveryAnnouncer asyncRetryingServiceDiscoveryAnnouncer =
        new AsyncRetryingServiceDiscoveryAnnouncer(serviceDiscoveryAnnouncers, serviceDiscoveryRegistrationRetryMS);
    Thread serviceDiscoveryAnnouncerRetryThread =
        asyncRetryingServiceDiscoveryAnnouncer.getServiceDiscoveryAnnouncerRetryThread();
    try {
      asyncRetryingServiceDiscoveryAnnouncer.register();
    } catch (Exception e) {
      Assert.fail("The method call should not throw an exception.");
    }
    Assert.assertFalse(serviceDiscoveryAnnouncerRetryThread.isAlive());
    try {
      asyncRetryingServiceDiscoveryAnnouncer.unregister();
    } catch (Exception e) {
      Assert.fail("The method call should not throw an exception.");
    }
    Assert.assertFalse(serviceDiscoveryAnnouncerRetryThread.isAlive());
    Assert.assertFalse(serviceDiscoveryAnnouncerRetryThread.isInterrupted());
  }

  /**
   * Below is the expected workflow of the test: <br>
   * 1) {@link AsyncRetryingServiceDiscoveryAnnouncer#register()} is called. The result of the call is that announcer3
   * successfully registers, and announcer1 and announcer2 fail to register, so {@code retryQueue=[announcer1, announcer2]} <br>
   * 2) Retry thread starts and retries registering the announcers in the queue <br>
   * 3) announcer1 retries registering and fails, and announcer2 retries registering and succeeds, so {@code retryQueue=[announcer1]} <br>
   * 4) The thread sleeps for 30 seconds <br>
   * 5) announcer1 retries registering and succeeds, so {@code retryQueue=[]} <br>
   * 6) The thread sleeps for 30 seconds and queue is empty, so the thread exits
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

    sleep(1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertFalse(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 1);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(retryQueue.contains(announcer1));
    Assert.assertFalse(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.size(), 0);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(asyncRetryingServiceDiscoveryAnnouncer.getServiceDiscoveryAnnouncerRetryThread().isAlive());
  }

  /**
   * Below is the expected workflow of the test: <br>
   * 1) {@link AsyncRetryingServiceDiscoveryAnnouncer#register()} is called. The result of the call is that all announcers
   * fail to register, so {@code retryQueue=[announcer1, announcer2, announcer3]} <br>
   * 2) Retry thread starts and retries registering the announcers in the queue <br>
   * 3) All announcers fail to register again, so {@code retryQueue=[announcer1, announcer2, announcer3]} <br>
   * 4) The thread sleeps for 30 seconds <br>
   * 5) announcer1 and announcer2 retry registering and fail, and announcer3 retries registering and succeeds, so
   * {@code retryQueue=[announcer1, announcer2]} <br>
   * 6) The thread sleeps for 30 seconds <br>
   * 7) announcer1 retries registering and succeeds, and announcer2 retries registering and fails, so {@code retryQueue=[announcer2]} <br>
   * 8) The thread sleeps for 30 seconds <br>
   * 9) announcer2 retries registering and succeeds, so {@code retryQueue=[]} <br>
   * 10) The thread sleeps for 30 seconds and queue is empty, so the thread exits
   */
  @Test
  public void testAddToRetryQueueMultipleTimes() {
    reset(announcer1, announcer2, announcer3, config);
    doThrow(new RuntimeException()).doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doNothing()
        .when(announcer1)
        .register();
    doThrow(new RuntimeException()).doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doNothing()
        .when(announcer2)
        .register();
    doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(announcer3).register();
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

    sleep(1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertTrue(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 3);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertTrue(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer1);
    Assert.assertEquals(retryQueue.size(), 2);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(retryQueue.contains(announcer1));
    Assert.assertTrue(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.peek(), announcer2);
    Assert.assertEquals(retryQueue.size(), 1);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(retryQueue.contains(announcer1));
    Assert.assertFalse(retryQueue.contains(announcer2));
    Assert.assertFalse(retryQueue.contains(announcer3));
    Assert.assertEquals(retryQueue.size(), 0);

    sleep(serviceDiscoveryRegistrationRetryMS + 1000);
    Assert.assertFalse(asyncRetryingServiceDiscoveryAnnouncer.getServiceDiscoveryAnnouncerRetryThread().isAlive());
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
}
