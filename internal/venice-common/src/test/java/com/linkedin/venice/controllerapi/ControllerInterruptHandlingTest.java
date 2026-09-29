package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.methods.HttpGet;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Interruption of the calling thread must unwind the controller client rather than being retried.
 *
 * <p>A retry loop that treats interruption as a transient controller failure consumes the caller's
 * cancellation signal, which makes the calling thread effectively uncancellable: every interrupt
 * delivered to stop it simply buys the loop another iteration. Stream processing frameworks cancel
 * tasks by interrupting them repeatedly and then kill the process if the task does not exit, so this
 * turns a routine cancellation into a lost container.
 */
public class ControllerInterruptHandlingTest {
  private static final String CONTROLLER_D2_SERVICE = "ChildController";

  @AfterMethod(alwaysRun = true)
  public void clearInterrupt() {
    // Never leak an interrupt flag onto the shared TestNG worker thread.
    Thread.interrupted();
  }

  /**
   * {@link ControllerTransport#executeRequest} wraps {@link InterruptedException} in an
   * {@link ExecutionException} for signature reasons. It must still re-assert the interrupt flag,
   * otherwise the interruption is invisible to every caller above it.
   */
  @Test
  public void testExecuteRequestPreservesInterruptFlag() throws Exception {
    try (ControllerTransport transport = new ControllerTransport(Optional.empty())) {
      // A pre-set interrupt makes the in-flight Future#get abort immediately, with no network needed.
      Thread.currentThread().interrupt();

      ExecutionException e = Assert.expectThrows(
          ExecutionException.class,
          () -> transport
              .executeRequest(new HttpGet("http://localhost:1/leader_controller"), ControllerResponse.class, 30_000));

      Assert.assertTrue(
          e.getCause() instanceof InterruptedException,
          "Expected the InterruptedException to be carried as the cause, but got: " + e.getCause());
      Assert.assertTrue(
          Thread.currentThread().isInterrupted(),
          "The interrupt flag must survive executeRequest, otherwise callers cannot tell an interrupted "
              + "request apart from an unreachable controller");
    }
  }

  /**
   * Closing the transport waits for the async client's I/O reactor, and httpcore-nio swallows the
   * InterruptedException of that wait. The transport must restore the flag, since every request closes its
   * transport before returning to the caller that has to see the interrupt.
   */
  @Test(timeOut = 30_000)
  public void testCloseKeepsTheCallerInterruptFlag() throws Exception {
    try (StalledServer server = new StalledServer()) {
      ControllerTransport transport = new ControllerTransport(Optional.empty());
      // A request that reaches the server leaves the client's I/O reactor running; closing a client whose reactor
      // never started returns without waiting, so it would not exercise the interrupted wait.
      Assert.expectThrows(
          TimeoutException.class,
          () -> transport.executeRequest(
              new HttpGet(server.getUrl() + "/leader_controller"),
              ControllerResponse.class,
              (int) TimeUnit.SECONDS.toMillis(1)));
      Assert.assertTrue(server.getConnectionCount() >= 1, "The request should have reached the stalled server");

      Thread.currentThread().interrupt();
      transport.close();

      Assert.assertTrue(Thread.currentThread().isInterrupted(), "Closing the transport must not clear the interrupt");
    }
  }

  /**
   * The retry loop must stop on the first interrupted attempt instead of consuming the interrupt
   * {@code maxAttempts} times.
   */
  @Test
  public void testRequestStopsRetryingOnceInterrupted() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("test-cluster", "http://localhost:1"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn("http://localhost:1").when(client).getLeaderControllerUrl();

    AtomicInteger attempts = new AtomicInteger();
    Mockito.when(
        transport.request(
            Mockito.anyString(),
            Mockito.any(ControllerRoute.class),
            Mockito.any(QueryParams.class),
            Mockito.<Class<StoreResponse>>any(),
            Mockito.anyInt(),
            Mockito.any()))
        .thenAnswer(invocation -> {
          attempts.incrementAndGet();
          // Reproduce a transport that correctly preserved the interrupt.
          Thread.currentThread().interrupt();
          throw new ExecutionException(new InterruptedException());
        });

    StoreResponse response = client.getStore("test-store");

    Assert.assertEquals(
        attempts.get(),
        1,
        "The retry loop must abort on the first interrupted attempt rather than absorbing the interrupt "
            + "on every attempt");
    Assert.assertTrue(response.isError(), "An aborted request should surface as an error response");
    Assert.assertTrue(
        response.getError().contains("interrupted"),
        "The error should say the request was interrupted so it is not misread as a controller outage, but was: "
            + response.getError());
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  /**
   * A request that fails for a non-interrupt reason must still use its full retry budget: the abort
   * above must key on interruption, not on failure in general.
   */
  @Test
  public void testRequestStillRetriesWhenNotInterrupted() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("test-cluster", "http://localhost:1"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn("http://localhost:1").when(client).getLeaderControllerUrl();

    AtomicInteger attempts = new AtomicInteger();
    StoreResponse success = new StoreResponse();
    Mockito.when(
        transport.request(
            Mockito.anyString(),
            Mockito.any(ControllerRoute.class),
            Mockito.any(QueryParams.class),
            Mockito.<Class<StoreResponse>>any(),
            Mockito.anyInt(),
            Mockito.any()))
        .thenAnswer(invocation -> {
          if (attempts.incrementAndGet() == 1) {
            throw new ExecutionException(new RuntimeException("controller genuinely unreachable"));
          }
          return success;
        });

    client.getStore("test-store");

    Assert.assertEquals(attempts.get(), 2, "A non-interrupt failure must still be retried");
    Assert.assertFalse(
        Thread.currentThread().isInterrupted(),
        "A non-interrupt failure must not leave the thread interrupted");
  }

  /**
   * An interrupt that arrives while the request loop is backing off after an ordinary failure must end the loop
   * as well, instead of being cleared by the sleep and followed by the next attempt.
   */
  @Test(timeOut = 30_000)
  public void testRequestStopsWhenInterruptedDuringBackoff() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("test-cluster", "http://localhost:1"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn("http://localhost:1").when(client).getLeaderControllerUrl();

    AtomicInteger attempts = new AtomicInteger();
    Mockito.when(
        transport.request(
            Mockito.anyString(),
            Mockito.any(ControllerRoute.class),
            Mockito.any(QueryParams.class),
            Mockito.<Class<StoreResponse>>any(),
            Mockito.anyInt(),
            Mockito.any()))
        .thenAnswer(invocation -> {
          attempts.incrementAndGet();
          throw new ExecutionException(new RuntimeException("controller genuinely unreachable"));
        });

    ScheduledExecutorService interrupter = Executors.newSingleThreadScheduledExecutor();
    try {
      Thread caller = Thread.currentThread();
      // The first attempt fails at once, so this interrupt lands inside the 5-second backoff that follows it.
      interrupter.schedule(caller::interrupt, 500, TimeUnit.MILLISECONDS);
      long startMs = System.currentTimeMillis();
      StoreResponse response = client.getStore("test-store");
      long elapsedMs = System.currentTimeMillis() - startMs;

      Assert.assertEquals(attempts.get(), 1, "No attempt may follow an interrupted backoff");
      Assert.assertTrue(elapsedMs < 4_000, "The backoff must end at the interrupt, but the request took " + elapsedMs);
      Assert.assertTrue(response.isError());
      Assert.assertTrue(response.getError().contains("interrupted"), response.getError());
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
    } finally {
      interrupter.shutdownNow();
    }
  }

  /**
   * Leader discovery walks the discovery URLs; once interrupted it must stop instead of issuing a request to the next
   * URL, which would wait out its full timeout with the interrupt already consumed.
   */
  @Test(timeOut = 30_000)
  public void testLeaderDiscoveryStopsWalkingUrlsOnceInterrupted() throws Exception {
    try (StalledServer first = new StalledServer(); StalledServer second = new StalledServer()) {
      ControllerClient client = new ControllerClient("test-cluster", first.getUrl() + "," + second.getUrl());

      Thread.currentThread().interrupt();
      VeniceException e = Assert.expectThrows(VeniceException.class, client::getLeaderControllerUrl);

      Assert.assertTrue(e.getMessage().contains("interrupted"), e.getMessage());
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
      Assert.assertTrue(
          first.getConnectionCount() + second.getConnectionCount() <= 1,
          "At most the first discovery URL may be contacted after the interrupt");
    }
  }

  @Test(timeOut = 30_000)
  public void testClusterDiscoveryStopsWalkingUrlsOnceInterrupted() throws Exception {
    try (StalledServer first = new StalledServer(); StalledServer second = new StalledServer()) {
      ControllerClient client = new ControllerClient("test-cluster", first.getUrl() + "," + second.getUrl());

      Thread.currentThread().interrupt();
      D2ServiceDiscoveryResponse response = client.discoverCluster("test-store");

      Assert.assertTrue(response.isError());
      Assert.assertTrue(response.getError().contains("interrupted"), response.getError());
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
      Assert.assertTrue(
          first.getConnectionCount() + second.getConnectionCount() <= 1,
          "At most the first discovery URL may be contacted after the interrupt");
    }
  }

  @Test
  public void testRetryableRequestStopsWhenAnAttemptIsInterrupted() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    AtomicInteger attempts = new AtomicInteger();

    VeniceException e =
        Assert.expectThrows(VeniceException.class, () -> ControllerClient.retryableRequest(client, 3, 0, c -> {
          attempts.incrementAndGet();
          Thread.currentThread().interrupt();
          throw new VeniceException("interrupted while waiting for the controller");
        }, r -> false));

    Assert.assertEquals(attempts.get(), 1, "No attempt may follow an interrupted one");
    Assert.assertTrue(e.getMessage().contains("interrupted"), e.getMessage());
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  @Test
  public void testRetryableRequestReturnsTheInterruptedErrorResponse() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    ControllerResponse errorResponse = new ControllerResponse();
    errorResponse.setError("controller request was interrupted");
    AtomicInteger attempts = new AtomicInteger();

    ControllerResponse result = ControllerClient.retryableRequest(client, 3, 0, c -> {
      attempts.incrementAndGet();
      Thread.currentThread().interrupt();
      return errorResponse;
    }, r -> false);

    Assert.assertSame(result, errorResponse);
    Assert.assertEquals(attempts.get(), 1, "No attempt may follow an interrupted one");
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  @Test(timeOut = 30_000)
  public void testRetryableRequestStopsWhenInterruptedDuringBackoff() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    AtomicInteger attempts = new AtomicInteger();
    ScheduledExecutorService interrupter = Executors.newSingleThreadScheduledExecutor();
    try {
      Thread caller = Thread.currentThread();
      interrupter.schedule(caller::interrupt, 500, TimeUnit.MILLISECONDS);
      long startMs = System.currentTimeMillis();

      VeniceException e =
          Assert.expectThrows(VeniceException.class, () -> ControllerClient.retryableRequest(client, 3, 10_000, c -> {
            attempts.incrementAndGet();
            throw new VeniceException("controller genuinely unreachable");
          }, r -> false));
      long elapsedMs = System.currentTimeMillis() - startMs;

      Assert.assertEquals(attempts.get(), 1, "No attempt may follow an interrupted backoff");
      Assert.assertTrue(elapsedMs < 5_000, "The backoff must end at the interrupt, but it took " + elapsedMs);
      Assert.assertTrue(e.getMessage().contains("interrupted"), e.getMessage());
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
    } finally {
      interrupter.shutdownNow();
    }
  }

  /**
   * Leader discovery through D2 must stop at the first interrupted D2 client instead of moving on to the next one.
   */
  @Test(timeOut = 30_000)
  public void testD2LeaderDiscoveryStopsAtTheInterruptedD2Client() {
    D2Client stalledD2Client = Mockito.mock(D2Client.class);
    Mockito.doReturn(new CompletableFuture<RestResponse>())
        .when(stalledD2Client)
        .restRequest(Mockito.any(RestRequest.class));
    D2Client nextD2Client = Mockito.mock(D2Client.class);
    AtomicInteger nextD2ClientCalls = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      nextD2ClientCalls.incrementAndGet();
      CompletableFuture<RestResponse> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("must not be called after the interrupt"));
      return failed;
    }).when(nextD2Client).restRequest(Mockito.any(RestRequest.class));

    try (D2ControllerClient client = new D2ControllerClient(
        CONTROLLER_D2_SERVICE,
        "test-cluster",
        Arrays.asList(stalledD2Client, nextD2Client),
        Optional.empty())) {
      Thread.currentThread().interrupt();
      VeniceException e = Assert.expectThrows(VeniceException.class, client::getLeaderControllerUrl);

      Assert.assertTrue(e.getMessage().contains("interrupted"), e.getMessage());
      Assert.assertEquals(nextD2ClientCalls.get(), 0, "The next D2 client must not be tried after the interrupt");
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
      Thread.interrupted();
    }
  }

  /**
   * The path Samza and Flink writers take: a {@link D2ControllerClient} request whose D2 leader discovery is
   * interrupted must end the request after one attempt, not retry it.
   */
  @Test(timeOut = 30_000)
  public void testD2ControllerRequestStopsWhenLeaderDiscoveryIsInterrupted() {
    D2Client d2Client = Mockito.mock(D2Client.class);
    AtomicInteger d2Calls = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      d2Calls.incrementAndGet();
      return new CompletableFuture<RestResponse>();
    }).when(d2Client).restRequest(Mockito.any(RestRequest.class));

    try (D2ControllerClient client = new D2ControllerClient(CONTROLLER_D2_SERVICE, "test-cluster", d2Client)) {
      Thread.currentThread().interrupt();
      StoreResponse response = client.getStore("test-store");

      Assert.assertTrue(response.isError());
      Assert.assertTrue(response.getError().contains("interrupted"), response.getError());
      Assert.assertEquals(d2Calls.get(), 1, "Leader discovery must not be retried after the interrupt");
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
      Thread.interrupted();
    }
  }

  @Test(timeOut = 30_000)
  public void testD2ClusterDiscoveryStopsAtTheInterruptedD2Client() {
    D2Client stalledD2Client = Mockito.mock(D2Client.class);
    Mockito.doReturn(new CompletableFuture<RestResponse>())
        .when(stalledD2Client)
        .restRequest(Mockito.any(RestRequest.class));
    D2Client nextD2Client = Mockito.mock(D2Client.class);
    AtomicInteger nextD2ClientCalls = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      nextD2ClientCalls.incrementAndGet();
      CompletableFuture<RestResponse> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("must not be called after the interrupt"));
      return failed;
    }).when(nextD2Client).restRequest(Mockito.any(RestRequest.class));

    try (D2ControllerClient client = new D2ControllerClient(
        CONTROLLER_D2_SERVICE,
        "test-cluster",
        Arrays.asList(stalledD2Client, nextD2Client),
        Optional.empty())) {
      Thread.currentThread().interrupt();
      VeniceException e = Assert.expectThrows(VeniceException.class, () -> client.discoverCluster("test-store"));

      Assert.assertTrue(e.getMessage().contains("interrupted"), e.getMessage());
      Assert.assertEquals(nextD2ClientCalls.get(), 0, "The next D2 client must not be tried after the interrupt");
      Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
      Thread.interrupted();
    }
  }

  /**
   * Accepts connections and never answers, like a controller that stopped responding.
   */
  private static class StalledServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final List<Socket> acceptedSockets = Collections.synchronizedList(new ArrayList<>());

    StalledServer() throws IOException {
      this.serverSocket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
      Thread acceptor = new Thread(() -> {
        while (!serverSocket.isClosed()) {
          try {
            acceptedSockets.add(serverSocket.accept());
          } catch (IOException e) {
            return;
          }
        }
      }, "stalled-controller-" + serverSocket.getLocalPort());
      acceptor.setDaemon(true);
      acceptor.start();
    }

    String getUrl() {
      return "http://127.0.0.1:" + serverSocket.getLocalPort();
    }

    int getConnectionCount() {
      return acceptedSockets.size();
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      synchronized (acceptedSockets) {
        for (Socket socket: acceptedSockets) {
          socket.close();
        }
      }
    }
  }
}
