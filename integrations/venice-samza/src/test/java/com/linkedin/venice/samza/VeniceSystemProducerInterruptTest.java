package com.linkedin.venice.samza;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * A writer that starts while the controller is slow must stop when its thread is interrupted, the way a Flink task
 * is cancelled, and must keep the interrupt flag set; ordinary controller failures must still be retried.
 */
public class VeniceSystemProducerInterruptTest {
  private static final String STORE_NAME = "test_store";

  @AfterMethod(alwaysRun = true)
  public void clearInterrupt() {
    // Never leak an interrupt flag onto the shared TestNG worker thread.
    Thread.interrupted();
  }

  @Test
  public void testControllerRequestStopsOnAnInterruptedErrorResponse() throws Exception {
    Time time = mock(Time.class);
    VeniceSystemProducer producer = newProducer(time, "http://localhost:1");
    ControllerResponse errorResponse = new ControllerResponse();
    errorResponse.setError("An error occurred during controller request, aborted because the thread was interrupted");
    AtomicInteger attempts = new AtomicInteger();

    VeniceException e = expectThrows(VeniceException.class, () -> producer.controllerRequestWithRetry(() -> {
      attempts.incrementAndGet();
      Thread.currentThread().interrupt();
      return errorResponse;
    }, 2));

    assertEquals(attempts.get(), 1, "No attempt may follow an interrupted one");
    verify(time, never()).sleep(anyLong());
    assertTrue(e.getMessage().contains("Interrupted"), e.getMessage());
    assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  @Test
  public void testControllerRequestStopsOnAnInterruptedException() throws Exception {
    Time time = mock(Time.class);
    VeniceSystemProducer producer = newProducer(time, "http://localhost:1");
    VeniceException attemptFailure = new VeniceException("Failed to discover leader controller with D2 client");
    AtomicInteger attempts = new AtomicInteger();

    VeniceException e = expectThrows(VeniceException.class, () -> producer.controllerRequestWithRetry(() -> {
      attempts.incrementAndGet();
      Thread.currentThread().interrupt();
      throw attemptFailure;
    }, 10));

    assertEquals(attempts.get(), 1, "No attempt may follow an interrupted one");
    verify(time, never()).sleep(anyLong());
    assertSame(e.getCause(), attemptFailure);
    assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  @Test
  public void testInterruptedBackoffKeepsTheInterruptFlag() throws Exception {
    Time time = mock(Time.class);
    doThrow(new InterruptedException()).when(time).sleep(anyLong());
    VeniceSystemProducer producer = newProducer(time, "http://localhost:1");
    ControllerResponse errorResponse = new ControllerResponse();
    errorResponse.setError("controller unavailable");

    VeniceException e =
        expectThrows(VeniceException.class, () -> producer.controllerRequestWithRetry(() -> errorResponse, 2));

    assertTrue(e.getCause() instanceof InterruptedException, "Unexpected cause: " + e.getCause());
    assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  /**
   * Stopping must key on the interrupt only: without one, every attempt and every backoff still happens.
   */
  @Test
  public void testControllerRequestStillRetriesWhenNotInterrupted() throws Exception {
    Time time = mock(Time.class);
    VeniceSystemProducer producer = newProducer(time, "http://localhost:1");
    ControllerResponse errorResponse = new ControllerResponse();
    errorResponse.setError("controller unavailable");
    AtomicInteger attempts = new AtomicInteger();

    SamzaException e = expectThrows(SamzaException.class, () -> producer.controllerRequestWithRetry(() -> {
      attempts.incrementAndGet();
      return errorResponse;
    }, 3));

    assertEquals(attempts.get(), 3);
    InOrder backoffs = Mockito.inOrder(time);
    backoffs.verify(time).sleep(1000L);
    backoffs.verify(time).sleep(2000L);
    backoffs.verify(time).sleep(3000L);
    assertTrue(e.getMessage().contains("controller unavailable"), e.getMessage());
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /**
   * A writer's start finds the controller leader through D2 and sends /request_topic to it, and the leader accepts the
   * request but never answers; then the writer's thread is interrupted, the way Flink cancels a task. The start must
   * end within seconds, without a second /request_topic, and with the interrupt flag still set.
   */
  @Test(timeOut = 120_000)
  public void testStartExitsPromptlyWhenInterruptedWhileTheControllerStalls() throws Exception {
    try (StalledController controller = new StalledController()) {
      VeniceSystemProducer producer = spy(newProducer(SystemTime.INSTANCE, controller.getUrl()));
      doNothing().when(producer).setupClientsAndReInitProvider();
      producer.setControllerClient(
          new D2ControllerClient("VeniceController", "test-cluster", leaderDiscoveryD2Client(controller.getUrl())));

      AtomicReference<Throwable> startFailure = new AtomicReference<>();
      AtomicBoolean interruptedWhenStartFailed = new AtomicBoolean();
      Thread writer = new Thread(() -> {
        try {
          producer.start();
        } catch (Throwable t) {
          interruptedWhenStartFailed.set(Thread.currentThread().isInterrupted());
          startFailure.set(t);
        }
      }, "writer-start");
      // Without the fix this thread stays blocked on the controller; it must not keep the test JVM alive.
      writer.setDaemon(true);
      writer.start();
      assertTrue(controller.awaitRequestTopic(60, TimeUnit.SECONDS), "The writer never reached /request_topic");

      long interruptMs = System.currentTimeMillis();
      writer.interrupt();
      writer.join(TimeUnit.SECONDS.toMillis(10));
      boolean exited = !writer.isAlive();
      try {
        assertTrue(
            exited,
            "The start must end within seconds of the interrupt, it was still running after "
                + (System.currentTimeMillis() - interruptMs) + " ms");
        assertEquals(controller.getRequestTopicCount(), 1, "/request_topic must not be retried after the interrupt");
        assertNotNull(startFailure.get(), "The interrupted start must fail");
        assertTrue(startFailure.get() instanceof VeniceException, "Unexpected failure: " + startFailure.get());
        assertTrue(startFailure.get().getMessage().contains("Interrupted"), startFailure.get().getMessage());
        assertTrue(interruptedWhenStartFailed.get(), "The interrupt flag must still be set when the start fails");
      } finally {
        // stop() and start() share the producer's lock, so stop() would wait for a start that never ends.
        if (exited) {
          producer.stop();
        }
      }
    }
  }

  /**
   * A controller-colo D2 client that answers leader discovery with the given URL, like VeniceController in D2.
   */
  private static D2Client leaderDiscoveryD2Client(String leaderUrl) throws Exception {
    LeaderControllerResponse leader = new LeaderControllerResponse();
    leader.setCluster("test-cluster");
    leader.setUrl(leaderUrl);
    RestResponse response = new RestResponseBuilder().setStatus(200)
        .setEntity(ObjectMapperFactory.getInstance().writeValueAsBytes(leader))
        .build();
    D2Client d2Client = mock(D2Client.class);
    doReturn(CompletableFuture.completedFuture(response)).when(d2Client).restRequest(any(RestRequest.class));
    return d2Client;
  }

  private static VeniceSystemProducer newProducer(Time time, String discoveryUrl) {
    return new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName(STORE_NAME)
            .setPushType(Version.PushType.STREAM)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setDiscoveryUrl(discoveryUrl)
            .setTime(time)
            .build());
  }

  /**
   * Accepts /request_topic without ever answering it, like an unresponsive controller leader.
   */
  private static class StalledController implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch requestTopicReceived = new CountDownLatch(1);
    private final AtomicInteger requestTopicCount = new AtomicInteger();
    private final String url;

    StalledController() throws IOException {
      this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
      this.url = "http://127.0.0.1:" + server.getAddress().getPort();
      server.createContext("/request_topic", exchange -> {
        requestTopicCount.incrementAndGet();
        requestTopicReceived.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          exchange.close();
        }
      });
      server.setExecutor(executor);
      server.start();
    }

    String getUrl() {
      return url;
    }

    boolean awaitRequestTopic(long timeout, TimeUnit unit) throws InterruptedException {
      return requestTopicReceived.await(timeout, unit);
    }

    int getRequestTopicCount() {
      return requestTopicCount.get();
    }

    @Override
    public void close() {
      release.countDown();
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
