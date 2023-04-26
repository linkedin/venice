package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class DualReadAvroGenericStoreClientTest {
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final static String fastClientResponse = "fast_client_response";
  private final static String thinClientResponse = "thin_client_response";

  @AfterClass
  public void tearDown() throws InterruptedException {
    TestUtils.shutdownExecutorNow(scheduledExecutor);
  }

  private DualReadAvroGenericStoreClient<String, String> prepareClients(
      boolean fastClientThrowExceptionWhenSending,
      boolean fastClientSucceed,
      long fastClientDelayMS,
      boolean thinClientThrowExceptionWhenSending,
      boolean thinClientSucceed,
      long thinClientDelayMS,
      FastClientStats clientStatsForSingleGet) {
    InternalAvroStoreClient<String, String> fastClient = mock(InternalAvroStoreClient.class);
    AvroGenericStoreClient<String, String> thinClient = mock(AvroGenericStoreClient.class);
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(clientStatsForSingleGet).when(clientConfig).getStats(RequestType.SINGLE_GET);
    doReturn(thinClient).when(clientConfig).getGenericThinClient();

    if (fastClientThrowExceptionWhenSending) {
      doThrow(new VeniceClientException("Mocked VeniceClientException for fast-client while sending out request"))
          .when(fastClient)
          .get(any(GetRequestContext.class), any());
    } else {
      if (fastClientSucceed) {
        if (fastClientDelayMS == 0) {
          doReturn(CompletableFuture.completedFuture(fastClientResponse)).when(fastClient)
              .get(any(GetRequestContext.class), any());
        } else {
          CompletableFuture<String> fastClientFuture = new CompletableFuture<>();
          doReturn(fastClientFuture).when(fastClient).get(any(GetRequestContext.class), any());
          scheduledExecutor
              .schedule(() -> fastClientFuture.complete(fastClientResponse), fastClientDelayMS, TimeUnit.MILLISECONDS);
        }
      } else {
        CompletableFuture<String> fastClientFuture = new CompletableFuture<>();
        doReturn(fastClientFuture).when(fastClient).get(any(GetRequestContext.class), any());
        if (fastClientDelayMS == 0) {
          fastClientFuture
              .completeExceptionally(new VeniceClientException("Mocked VeniceClientException for fast-client"));
        } else {
          scheduledExecutor.schedule(
              () -> fastClientFuture
                  .completeExceptionally(new VeniceClientException("Mocked VeniceClientException for fast-client")),
              fastClientDelayMS,
              TimeUnit.MILLISECONDS);
        }
      }
    }

    if (thinClientThrowExceptionWhenSending) {
      doThrow(new VeniceClientException("Mocked VeniceClientException for thin-client while sending out request"))
          .when(thinClient)
          .get(any());
    } else {
      if (thinClientSucceed) {
        if (thinClientDelayMS == 0) {
          doReturn(CompletableFuture.completedFuture(thinClientResponse)).when(thinClient).get(any());
        } else {
          CompletableFuture<String> thinClientFuture = new CompletableFuture<>();
          doReturn(thinClientFuture).when(thinClient).get(any());
          scheduledExecutor
              .schedule(() -> thinClientFuture.complete(thinClientResponse), fastClientDelayMS, TimeUnit.MILLISECONDS);
        }
      } else {
        CompletableFuture<String> thinClientFuture = new CompletableFuture<>();
        doReturn(thinClientFuture).when(thinClient).get(any());
        if (thinClientDelayMS == 0) {
          thinClientFuture
              .completeExceptionally(new VeniceClientException("Mocked VeniceClientException for thin-client"));
        } else {
          scheduledExecutor.schedule(
              () -> thinClientFuture
                  .completeExceptionally(new VeniceClientException("Mocked VeniceClientException for thin-client")),
              thinClientDelayMS,
              TimeUnit.MILLISECONDS);
        }
      }
    }

    return new DualReadAvroGenericStoreClient<>(fastClient, clientConfig);
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    // Both returns, but fast-client is faster
    FastClientStats clientStatsForSingleGet = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClients(false, true, 0, false, true, 1000, clientStatsForSingleGet);
    String res = dualReadClient.get("test_key").get();
    assertEquals(res, fastClientResponse, "Fast client response should be returned since it is faster");
    verify(clientStatsForSingleGet, never()).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, never()).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet, timeout(2000)).recordThinClientFastClientLatencyDelta(anyDouble());

    // Both returns, but thin-client is faster
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(false, true, 1000, false, true, 0, clientStatsForSingleGet);
    res = dualReadClient.get("test_key").get();
    assertEquals(res, thinClientResponse, "Thin client response should be returned since it is faster");
    verify(clientStatsForSingleGet, never()).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, timeout(2000)).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet).recordThinClientFastClientLatencyDelta(anyDouble());

    // Fast-client returns ok, but thin-client returns error
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(false, true, 1000, false, false, 0, clientStatsForSingleGet);
    res = dualReadClient.get("test_key").get();
    assertEquals(res, fastClientResponse, "Fast client response should be returned since it succeeds");
    verify(clientStatsForSingleGet, never()).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, never()).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet, never()).recordThinClientFastClientLatencyDelta(anyDouble());

    // Fast-client returns error, but thin-client returns ok
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(false, false, 0, false, true, 1000, clientStatsForSingleGet);
    res = dualReadClient.get("test_key").get();
    assertEquals(res, thinClientResponse, "Thin client response should be returned since it succeeds");
    verify(clientStatsForSingleGet).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, never()).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet, never()).recordThinClientFastClientLatencyDelta(anyDouble());

    // Both return error
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(false, false, 1000, false, false, 0, clientStatsForSingleGet);
    try {
      dualReadClient.get("test_key").get();
      fail("Exception is expected here when both clients return error");
    } catch (Exception e) {
      // expected
      Assert.assertEquals(e.getClass(), ExecutionException.class);
    }

    // Fast-client returns ok, but thin-client fails to send out request
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(false, true, 1000, true, true, 0, clientStatsForSingleGet);
    res = dualReadClient.get("test_key").get();
    assertEquals(res, fastClientResponse, "Fast client response should be returned since it succeeds");
    verify(clientStatsForSingleGet, never()).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, never()).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet, never()).recordThinClientFastClientLatencyDelta(anyDouble());

    // Fast-client fails to send out request, but thin-client returns ok
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(true, true, 0, false, true, 1000, clientStatsForSingleGet);
    res = dualReadClient.get("test_key").get();
    assertEquals(res, thinClientResponse, "Thin client response should be returned since it succeeds");
    verify(clientStatsForSingleGet).recordFastClientErrorThinClientSucceedRequest();
    verify(clientStatsForSingleGet, never()).recordFastClientSlowerRequest();
    verify(clientStatsForSingleGet, never()).recordThinClientFastClientLatencyDelta(anyDouble());

    // Both fail to send out request
    clientStatsForSingleGet = mock(FastClientStats.class);
    dualReadClient = prepareClients(true, true, 1000, true, true, 0, clientStatsForSingleGet);
    try {
      dualReadClient.get("test_key").get();
      fail("Exception is expected here when both clients return error");
    } catch (Exception e) {
      // expected
      Assert.assertEquals(e.getClass(), ExecutionException.class);
    }
  }
}
