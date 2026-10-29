package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.VersionStorageModeUpdateReason;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ControllerClientTest {
  @DataProvider
  public Object[][] discoveryDurations() {
    return new Object[][] { { false, 15_000, new int[] { 5000, 5000, 5000 } },
        { false, 15_000, new int[] { 2000, 5000, 5000, 3000 } }, { true, 8000, new int[] { 5000, 3000 } },
        { true, 8000, new int[] { 2000, 5000, 1000 } } };
  }

  @Test(dataProvider = "discoveryDurations")
  public void testDiscoverySharesDeadlineAcrossCandidates(boolean fanout, int budgetMs, int[] durationsMs)
      throws Exception {
    ControllerClient client = Mockito
        .spy(new ControllerClient("cluster", "http://first,http://second,http://third,http://fourth,http://fifth"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    AtomicLong clock = new AtomicLong();
    Mockito.doAnswer(invocation -> clock.get()).when(client).requestTimeNs();
    AtomicInteger attempts = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      int remainingMs = budgetMs - (int) TimeUnit.NANOSECONDS.toMillis(clock.get());
      Assert.assertEquals(invocation.<Integer>getArgument(4).intValue(), Math.min(5000, remainingMs));
      clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(durationsMs[attempts.getAndIncrement()]));
      throw new TimeoutException("candidate timed out");
    })
        .when(transport)
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    if (fanout) {
      Assert.assertTrue(client.getStoreWithDeadline("store", TimeUnit.MILLISECONDS.toNanos(budgetMs)).isError());
      Mockito.verify(transport, Mockito.never())
          .request(
              Mockito.anyString(),
              Mockito.eq(ControllerRoute.STORE),
              Mockito.any(),
              Mockito.any(),
              Mockito.anyInt(),
              Mockito.isNull());
    } else {
      Assert.assertThrows(VeniceException.class, client::discoverLeaderController);
    }
    Assert.assertEquals(attempts.get(), durationsMs.length);
    Assert.assertEquals(clock.get(), TimeUnit.MILLISECONDS.toNanos(budgetMs));
    Mockito.verify(transport).close();
  }

  @Test
  public void testFanoutFallsBackWithCandidateCapAndRemainingStoreBudget() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://first,http://second"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    // Nine seconds have already elapsed in submission/queueing; discovery must not start a new 15 seconds.
    AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(9));
    Mockito.doAnswer(invocation -> clock.get()).when(client).requestTimeNs();
    AtomicInteger attempts = new AtomicInteger();
    LeaderControllerResponse leader = new LeaderControllerResponse();
    leader.setUrl("http://leader");
    Mockito.doAnswer(invocation -> {
      if (attempts.getAndIncrement() == 0) {
        Assert.assertEquals(invocation.<Integer>getArgument(4).intValue(), 5000);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
        throw new TimeoutException("first candidate stalled");
      }
      Assert.assertEquals(invocation.<Integer>getArgument(4).intValue(), 1000);
      clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(400));
      return leader;
    })
        .when(transport)
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    StoreResponse success = new StoreResponse();
    Mockito.doReturn(success)
        .when(transport)
        .request(
            Mockito.eq("http://leader"),
            Mockito.eq(ControllerRoute.STORE),
            Mockito.any(),
            Mockito.eq(StoreResponse.class),
            Mockito.eq(600),
            Mockito.isNull());
    Assert.assertSame(client.getStoreWithDeadline("store", TimeUnit.SECONDS.toNanos(15)), success);
    Assert.assertEquals(attempts.get(), 2);
    Mockito.verify(transport).close();
  }

  @DataProvider
  public Object[][] discoveryModes() {
    return new Object[][] { { false }, { true } };
  }

  @Test
  public void testDiscoveryFallsBackAfterCandidateFailure() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://first,http://second"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn(0L).when(client).requestTimeNs();
    LeaderControllerResponse leader = new LeaderControllerResponse();
    leader.setUrl("http://leader");
    Mockito.when(
        transport.request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.eq(5000),
            Mockito.isNull()))
        .thenThrow(new TimeoutException())
        .thenReturn(leader);
    Assert.assertEquals(client.discoverLeaderController(), "http://leader");
    Mockito.verify(transport, Mockito.times(2))
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.eq(5000),
            Mockito.isNull());
  }

  @Test
  public void testDiscoveryPreservesActionableFailure() throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://first,http://second"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn(0L).when(client).requestTimeNs();
    VeniceException actionable = new VeniceException("Invalid discovery response");
    Mockito.when(
        transport.request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull()))
        .thenThrow(actionable)
        .thenThrow(new VeniceException(new ConnectException()));
    VeniceException failure = Assert.expectThrows(VeniceException.class, client::discoverLeaderController);
    Assert.assertSame(failure.getCause(), actionable);
  }

  @Test(dataProvider = "discoveryModes")
  public void testDiscoveryStopsOnInterruption(boolean fanout) throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://first,http://second"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    })
        .when(transport)
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    try {
      if (fanout) {
        Assert.assertTrue(
            client.getStoreWithDeadline("store", System.nanoTime() + TimeUnit.SECONDS.toNanos(15)).isError());
      } else {
        Assert.assertThrows(VeniceException.class, client::discoverLeaderController);
      }
      Assert.assertTrue(Thread.currentThread().isInterrupted());
      Mockito.verify(transport)
          .request(
              Mockito.anyString(),
              Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
              Mockito.any(),
              Mockito.eq(LeaderControllerResponse.class),
              Mockito.anyInt(),
              Mockito.isNull());
    } finally {
      Thread.interrupted();
    }
  }

  @DataProvider
  public Object[][] boundedQueries() {
    return new Object[][] { { ControllerRoute.STORE }, { ControllerRoute.GET_ADMIN_TOPIC_METADATA } };
  }

  private ControllerResponse boundedQuery(ControllerClient client, ControllerRoute route) {
    if (route == ControllerRoute.STORE) {
      return client.getStoreWithTimeout("store", 1000);
    }
    return client.getAdminTopicMetadataWithTimeout(Optional.empty(), 1000);
  }

  @Test(dataProvider = "boundedQueries")
  public void testBoundedQueriesShareDiscoveryBudget(ControllerRoute route) throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://discovery"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    AtomicLong clock = new AtomicLong();
    Mockito.doAnswer(invocation -> clock.get()).when(client).requestTimeNs();
    LeaderControllerResponse leader = new LeaderControllerResponse();
    leader.setUrl("http://leader");
    Mockito.doAnswer(invocation -> {
      Assert.assertEquals(invocation.<Integer>getArgument(4).intValue(), 1000);
      clock.set(TimeUnit.MILLISECONDS.toNanos(400));
      return leader;
    })
        .when(transport)
        .request(
            Mockito.eq("http://discovery"),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    boundedQuery(client, route);
    Mockito.verify(transport)
        .request(
            Mockito.eq("http://leader"),
            Mockito.eq(route),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(600),
            Mockito.isNull());
    Mockito.verify(transport).close();
  }

  @Test(dataProvider = "boundedQueries")
  public void testBoundedQueriesDoNotRetryDiscovery(ControllerRoute route) throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://discovery,http://other"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doThrow(new TimeoutException())
        .when(transport)
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    Assert.assertTrue(boundedQuery(client, route).isError());
    Mockito.verify(transport)
        .request(
            Mockito.anyString(),
            Mockito.eq(ControllerRoute.LEADER_CONTROLLER),
            Mockito.any(),
            Mockito.eq(LeaderControllerResponse.class),
            Mockito.anyInt(),
            Mockito.isNull());
    Mockito.verify(transport, Mockito.never())
        .request(
            Mockito.anyString(),
            Mockito.eq(route),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.isNull());
  }

  @Test(dataProvider = "boundedQueries")
  public void testBoundedQueriesDoNotRetryRequest(ControllerRoute route) throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://discovery"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Mockito.doReturn("http://leader").when(client).discoverLeaderController(Mockito.eq(transport), Mockito.anyLong());
    Mockito.doThrow(new TimeoutException())
        .when(transport)
        .request(
            Mockito.eq("http://leader"),
            Mockito.eq(route),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.isNull());
    Assert.assertTrue(boundedQuery(client, route).isError());
    Mockito.verify(transport)
        .request(
            Mockito.eq("http://leader"),
            Mockito.eq(route),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.isNull());
  }

  @Test(dataProvider = "boundedQueries")
  public void testBoundedQueriesSkipRequestAfterDiscoveryUsesBudget(ControllerRoute route) throws Exception {
    ControllerClient client = Mockito.spy(new ControllerClient("cluster", "http://discovery"));
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    AtomicLong clock = new AtomicLong();
    Mockito.doAnswer(invocation -> clock.get()).when(client).requestTimeNs();
    Mockito.doAnswer(invocation -> {
      clock.set(TimeUnit.SECONDS.toNanos(1));
      return "http://leader";
    }).when(client).discoverLeaderController(Mockito.eq(transport), Mockito.anyLong());
    Assert.assertTrue(boundedQuery(client, route).isError());
    Mockito.verify(transport, Mockito.never())
        .request(
            Mockito.anyString(),
            Mockito.eq(route),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.isNull());
  }

  @Test
  public void testBoundedD2DiscoveryDoesNotRetryAndCancelsTimedOutRead() throws Exception {
    D2Client first = Mockito.mock(D2Client.class);
    D2Client second = Mockito.mock(D2Client.class);
    D2ControllerClient client =
        Mockito.spy(new D2ControllerClient("service", "cluster", Arrays.asList(first, second), Optional.empty()));
    Mockito.doReturn(0L).when(client).requestTimeNs();
    ControllerTransport transport = Mockito.mock(ControllerTransport.class);
    Mockito.doReturn(transport).when(client).getNewControllerTransport();
    Future<RestResponse> future = Mockito.mock(Future.class);
    Mockito.doReturn(future).when(first).restRequest(Mockito.any());
    Mockito.doThrow(new TimeoutException()).when(future).get(1000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(client.getStoreWithTimeout("store", 1000).isError());
    Mockito.verify(future).get(1000, TimeUnit.MILLISECONDS);
    Mockito.verify(future).cancel(true);
    Mockito.verify(second, Mockito.never()).restRequest(Mockito.any());
    Mockito.verify(transport, Mockito.never())
        .request(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.isNull());
  }

  /**
   * A transient failure on an earlier attempt must not mask a later successful attempt: the retry state is cleared
   * at the start of every attempt, so the first success is returned rather than the run throwing on a stale failure.
   */
  @Test
  public void testRetryableRequestReturnsSuccessAfterTransientFailure() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    ControllerResponse success = new ControllerResponse();
    AtomicInteger attempts = new AtomicInteger(0);

    // Zero backoff keeps the retry loop instant under test.
    ControllerResponse result = ControllerClient.retryableRequest(client, 3, 0, c -> {
      if (attempts.getAndIncrement() == 0) {
        throw new VeniceException("transient failure on first attempt");
      }
      return success;
    }, r -> false);

    Assert.assertSame(result, success);
    Assert.assertFalse(result.isError());
    Assert.assertEquals(attempts.get(), 2, "Should stop retrying as soon as an attempt succeeds");
  }

  @Test
  public void testRetryableRequestThrowsAfterAllAttemptsFail() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    AtomicInteger attempts = new AtomicInteger(0);

    Assert.assertThrows(VeniceException.class, () -> ControllerClient.retryableRequest(client, 3, 0, c -> {
      attempts.incrementAndGet();
      throw new VeniceException("persistent failure");
    }, r -> false));

    Assert.assertEquals(attempts.get(), 3, "Should exhaust all attempts before throwing");
  }

  /**
   * Callers written against the pre-existing overloads must keep compiling and must land on the same request the
   * controller already understands, with the reason defaulted rather than invented.
   */
  @Test
  public void testUpdateStoreVersionStorageModeWithoutReasonDefaultsToUnspecified() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    Mockito.doCallRealMethod()
        .when(client)
        .updateStoreVersionStorageMode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
    Mockito.doCallRealMethod()
        .when(client)
        .updateStoreVersionStorageMode(Mockito.anyString(), Mockito.anyInt(), Mockito.any(), Mockito.any());

    client.updateStoreVersionStorageMode("store", 1, StorageMode.INTERNAL);
    client.updateStoreVersionStorageMode("store", 1, StorageMode.INTERNAL, "dc-1");

    Mockito.verify(client)
        .updateStoreVersionStorageMode(
            "store",
            1,
            StorageMode.INTERNAL,
            null,
            VersionStorageModeUpdateReason.UNSPECIFIED);
    Mockito.verify(client)
        .updateStoreVersionStorageMode(
            "store",
            1,
            StorageMode.INTERNAL,
            "dc-1",
            VersionStorageModeUpdateReason.UNSPECIFIED);
  }

  @Test
  public void testVersionStorageModeUpdateReasonParsing() {
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("EXTERNAL_WRITE_FAILURE"),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("external_write_failure"),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
    // Absent, blank or unknown values must never fail the request; they simply are not alertable.
    Assert
        .assertEquals(VersionStorageModeUpdateReason.parseOrDefault(null), VersionStorageModeUpdateReason.UNSPECIFIED);
    Assert
        .assertEquals(VersionStorageModeUpdateReason.parseOrDefault("  "), VersionStorageModeUpdateReason.UNSPECIFIED);
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("SOMETHING_FROM_A_NEWER_CLIENT"),
        VersionStorageModeUpdateReason.UNSPECIFIED);
  }
}
