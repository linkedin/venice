package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.doAnswer;
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
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DualReadAvroGenericStoreClientTest {
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private static final String FAST_CLIENT_SINGLE_GET_RESPONSE = "fast_client_response";
  private static final Map<String, String> FAST_CLIENT_BATCH_GET_RESPONSE = new HashMap<>();

  private static final String THIN_CLIENT_SINGLE_GET_RESPONSE = "thin_client_response";
  private static final Map<String, String> THIN_CLIENT_BATCH_GET_RESPONSE = new HashMap<>();
  private static final String SINGLE_GET_KEY = "test_key";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();

  @BeforeClass
  public void setUp() {
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    FAST_CLIENT_BATCH_GET_RESPONSE.put("test_key_1", "fast_client_response1");
    FAST_CLIENT_BATCH_GET_RESPONSE.put("test_key_2", "fast_client_response2");
    THIN_CLIENT_BATCH_GET_RESPONSE.put("test_key_1", "fast_client_response1");
    THIN_CLIENT_BATCH_GET_RESPONSE.put("test_key_2", "fast_client_response2");
  }

  @AfterClass
  public void tearDown() throws InterruptedException {
    TestUtils.shutdownExecutor(scheduledExecutor);
  }

  private DualReadAvroGenericStoreClient<String, String> prepareClient(
      boolean batchGet,
      boolean fastClientThrowExceptionWhenSending,
      boolean fastClientSucceed,
      long fastClientDelayMS,
      boolean thinClientThrowExceptionWhenSending,
      boolean thinClientSucceed,
      long thinClientDelayMS,
      FastClientStats dualClientStats,
      boolean useStreamingBatchGetAsDefault) {
    InternalAvroStoreClient<String, String> fastClient = mock(DispatchingAvroGenericStoreClient.class);
    AvroGenericStoreClient<String, String> thinClient = mock(AvroGenericStoreClient.class);
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(dualClientStats).when(clientConfig).getStats(RequestType.MULTI_GET);
    doReturn(dualClientStats).when(clientConfig).getStats(RequestType.SINGLE_GET);
    doReturn(useStreamingBatchGetAsDefault).when(clientConfig).useStreamingBatchGetAsDefault();
    doReturn(thinClient).when(clientConfig).getGenericThinClient();
    doReturn(2).when(clientConfig).getMaxAllowedKeyCntInBatchGetReq();

    if (fastClientThrowExceptionWhenSending) {
      String fcRequestFailException = "Mocked VeniceClientException for fast-client while sending out request";
      if (batchGet) {
        if (useStreamingBatchGetAsDefault) {
          doAnswer(invocation -> {
            throw new VeniceClientException(fcRequestFailException);
          }).when(fastClient).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
        } else {
          doThrow(new VeniceClientException(fcRequestFailException)).when(fastClient)
              .get(any(GetRequestContext.class), any());
        }
      } else {
        doThrow(new VeniceClientException(fcRequestFailException)).when(fastClient)
            .get(any(GetRequestContext.class), any());
      }
    } else {
      if (fastClientSucceed) {
        if (fastClientDelayMS == 0) {
          if (batchGet) {
            if (useStreamingBatchGetAsDefault) {
              doAnswer(invocation -> {
                StreamingCallback callback = invocation.getArgument(2);
                FAST_CLIENT_BATCH_GET_RESPONSE.forEach((k, v) -> callback.onRecordReceived(k, v));
                callback.onCompletion(Optional.empty());
                return null;
              }).when(fastClient).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
            } else {
              doAnswer(invocation -> {
                String key = invocation.getArgument(1);
                return CompletableFuture.completedFuture(FAST_CLIENT_BATCH_GET_RESPONSE.get(key));
              }).when(fastClient).get(any(GetRequestContext.class), any());
            }
          } else {
            doReturn(CompletableFuture.completedFuture(FAST_CLIENT_SINGLE_GET_RESPONSE)).when(fastClient)
                .get(any(GetRequestContext.class), any());
          }
        } else {
          if (batchGet) {
            if (useStreamingBatchGetAsDefault) {
              doAnswer(invocation -> {
                maybeDelayExecute(() -> {
                  StreamingCallback callback = invocation.getArgument(2);
                  FAST_CLIENT_BATCH_GET_RESPONSE.forEach(callback::onRecordReceived);
                  callback.onCompletion(Optional.empty());
                }, fastClientDelayMS);
                return null;
              }).when(fastClient).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
            } else {
              doAnswer(invocation -> {
                CompletableFuture<String> fastClientFuture = new CompletableFuture<>();
                maybeDelayExecute(() -> {
                  String key = invocation.getArgument(1);
                  fastClientFuture.complete(FAST_CLIENT_BATCH_GET_RESPONSE.get(key));
                }, fastClientDelayMS);
                return fastClientFuture;
              }).when(fastClient).get(any(GetRequestContext.class), any());
            }
          } else {
            doAnswer(invocation -> {
              CompletableFuture<String> fastClientFuture = new CompletableFuture<>();
              maybeDelayExecute(() -> {
                fastClientFuture.complete(FAST_CLIENT_SINGLE_GET_RESPONSE);
              }, fastClientDelayMS);
              return fastClientFuture;
            }).when(fastClient).get(any(GetRequestContext.class), any());
          }
        }
      } else {
        String fcException = "Mocked VeniceClientException for fast-client";
        if (batchGet) {
          if (useStreamingBatchGetAsDefault) {
            doAnswer(invocation -> {
              maybeDelayExecute(() -> {
                StreamingCallback callback = invocation.getArgument(2);
                callback.onCompletion(Optional.of(new VeniceClientException(fcException)));
              }, fastClientDelayMS);
              return null;
            }).when(fastClient).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
          } else {
            doAnswer(invocation -> {
              CompletableFuture<Map<String, String>> fastClientFuture = new CompletableFuture<>();
              maybeDelayExecute(() -> {
                fastClientFuture.completeExceptionally(new VeniceClientException(fcException));
              }, fastClientDelayMS);
              return fastClientFuture;
            }).when(fastClient).get(any(GetRequestContext.class), any());
          }
        } else {
          CompletableFuture<String> fastClientFuture = new CompletableFuture<>();
          doAnswer(invocation -> {
            maybeDelayExecute(() -> {
              fastClientFuture.completeExceptionally(new VeniceClientException(fcException));
            }, fastClientDelayMS);
            return fastClientFuture;
          }).when(fastClient).get(any(GetRequestContext.class), any());
        }
      }
    }

    if (thinClientThrowExceptionWhenSending) {
      String tcRequestFailException = "Mocked VeniceClientException for thin-client while sending out request";
      if (batchGet) {
        doThrow(new VeniceClientException(tcRequestFailException)).when(thinClient).batchGet(any());
      } else {
        doThrow(new VeniceClientException(tcRequestFailException)).when(thinClient).get(any());
      }
    } else {
      if (thinClientSucceed) {
        if (thinClientDelayMS == 0) {
          if (batchGet) {
            doReturn(CompletableFuture.completedFuture(THIN_CLIENT_BATCH_GET_RESPONSE)).when(thinClient)
                .batchGet(any());
          } else {
            doReturn(CompletableFuture.completedFuture(THIN_CLIENT_SINGLE_GET_RESPONSE)).when(thinClient).get(any());
          }
        } else {
          if (batchGet) {
            doAnswer(invocation -> {
              CompletableFuture<Map<String, String>> thinClientFuture = new CompletableFuture<>();
              maybeDelayExecute(() -> {
                thinClientFuture.complete(THIN_CLIENT_BATCH_GET_RESPONSE);
              }, thinClientDelayMS);
              return thinClientFuture;
            }).when(thinClient).batchGet(any());
          } else {
            doAnswer(invocation -> {
              CompletableFuture<String> thinClientFuture = new CompletableFuture<>();
              maybeDelayExecute(() -> {
                thinClientFuture.complete(THIN_CLIENT_SINGLE_GET_RESPONSE);
              }, thinClientDelayMS);
              return thinClientFuture;
            }).when(thinClient).get(any());
          }
        }
      } else {
        String tcException = "Mocked VeniceClientException for thin-client";
        if (batchGet) {
          CompletableFuture<Map<String, String>> thinClientFuture = new CompletableFuture<>();
          doAnswer(invocation -> {
            maybeDelayExecute(() -> {
              thinClientFuture.completeExceptionally(new VeniceClientException(tcException));
            }, thinClientDelayMS);
            return thinClientFuture;
          }).when(thinClient).batchGet(any());
        } else {
          CompletableFuture<String> thinClientFuture = new CompletableFuture<>();
          doAnswer(invocation -> {
            maybeDelayExecute(() -> {
              thinClientFuture.completeExceptionally(new VeniceClientException(tcException));
            }, thinClientDelayMS);
            return thinClientFuture;
          }).when(thinClient).get(any());
        }
      }
    }

    return new DualReadAvroGenericStoreClient<>(fastClient, clientConfig);
  }

  @DataProvider(name = "FastClient-Two-Booleans")
  public Object[][] twoBooleans() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean batchGet = (boolean) permutation[0];
      boolean useStreamingBatchGetAsDefault = (boolean) permutation[1];
      if (!batchGet) {
        if (useStreamingBatchGetAsDefault) {
          // avoid duplicate tests for batchGet cases
          return false;
        }
      }
      return true;
    },
        DataProviderUtils.BOOLEAN, // batchGet
        DataProviderUtils.BOOLEAN); // useStreamingBatchGetAsDefault
  }

  // Both returns, but fast-client is faster
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithFastClientBeingFaster(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, true, 0, false, true, 1000, dualClientStats, useStreamingBatchGetAsDefault);

    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          FAST_CLIENT_BATCH_GET_RESPONSE,
          "Fast client response should be returned since it is faster");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          FAST_CLIENT_SINGLE_GET_RESPONSE,
          "Fast client response should be returned since it is faster");
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats, never()).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, never()).recordFastClientSlowerRequest();
      verify(dualClientStats, timeout(2000)).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Both returns, but thin-client is faster
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithThinClientBeingFaster(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, true, 1000, false, true, 0, dualClientStats, useStreamingBatchGetAsDefault);
    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          THIN_CLIENT_BATCH_GET_RESPONSE,
          "Thin client response should be returned since it is faster");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          THIN_CLIENT_SINGLE_GET_RESPONSE,
          "Thin client response should be returned since it is faster");
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats, never()).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, timeout(2000)).recordFastClientSlowerRequest();
      verify(dualClientStats).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Fast-client returns ok, but thin-client returns error
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithThinClientReturnError(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, true, 1000, false, false, 0, dualClientStats, useStreamingBatchGetAsDefault);
    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          FAST_CLIENT_BATCH_GET_RESPONSE,
          "Fast client response should be returned since it succeeds");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          FAST_CLIENT_SINGLE_GET_RESPONSE,
          "Fast client response should be returned since it succeeds");
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats, never()).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, never()).recordFastClientSlowerRequest();
      verify(dualClientStats, never()).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Fast-client returns error, but thin-client returns ok
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithFastClientReturnError(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, false, 0, false, true, 1000, dualClientStats, useStreamingBatchGetAsDefault);
    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          THIN_CLIENT_BATCH_GET_RESPONSE,
          "Thin client response should be returned since it succeeds");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          THIN_CLIENT_SINGLE_GET_RESPONSE,
          "Thin client response should be returned since it succeeds");
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, never()).recordFastClientSlowerRequest();
      verify(dualClientStats, never()).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Both return error
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithBothClientsReturnError(boolean batchGet, boolean useStreamingBatchGetAsDefault) {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, false, 1000, false, false, 0, dualClientStats, useStreamingBatchGetAsDefault);
    try {
      if (batchGet) {
        dualReadClient.batchGet(BATCH_GET_KEYS).get();
      } else {
        dualReadClient.get(SINGLE_GET_KEY).get();
      }
      fail("Exception is expected here when both clients return error");
    } catch (Exception e) {
      // expected
      assertEquals(e.getClass(), ExecutionException.class);
    }
  }

  // Fast-client returns ok, but thin-client fails to send out request
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithThinClientFailsToSendOutRequest(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, false, true, 1000, true, true, 0, dualClientStats, useStreamingBatchGetAsDefault);
    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          FAST_CLIENT_BATCH_GET_RESPONSE,
          "Fast client response should be returned since it succeeds");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          FAST_CLIENT_SINGLE_GET_RESPONSE,
          "Fast client response should be returned since it succeeds");
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats, never()).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, never()).recordFastClientSlowerRequest();
      verify(dualClientStats, never()).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Fast-client fails to send out request, but thin-client returns ok
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithFastClientFailsToSendOutRequest(boolean batchGet, boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, true, true, 0, false, true, 1000, dualClientStats, useStreamingBatchGetAsDefault);
    if (batchGet) {
      Map<String, String> batchGetRes = dualReadClient.batchGet(BATCH_GET_KEYS).get();
      assertEquals(
          batchGetRes,
          THIN_CLIENT_BATCH_GET_RESPONSE,
          "Thin client response should be returned since it succeeds");
    } else {
      String singleGetRes = dualReadClient.get(SINGLE_GET_KEY).get();
      assertEquals(
          singleGetRes,
          THIN_CLIENT_SINGLE_GET_RESPONSE,
          "Thin client response should be returned since it succeeds");
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(dualClientStats).recordFastClientErrorThinClientSucceedRequest();
      verify(dualClientStats, never()).recordFastClientSlowerRequest();
      verify(dualClientStats, never()).recordThinClientFastClientLatencyDelta(anyDouble());
    });
  }

  // Both fail to send out request
  @Test(dataProvider = "FastClient-Two-Booleans")
  public void testGetWithBothClientsFailsToSendOutRequest(boolean batchGet, boolean useStreamingBatchGetAsDefault) {
    FastClientStats dualClientStats = mock(FastClientStats.class);
    AvroGenericStoreClient<String, String> dualReadClient =
        prepareClient(batchGet, true, true, 1000, true, true, 0, dualClientStats, useStreamingBatchGetAsDefault);
    try {
      if (batchGet) {
        dualReadClient.batchGet(BATCH_GET_KEYS).get();
      } else {
        dualReadClient.get(SINGLE_GET_KEY).get();
      }
      fail("Exception is expected here when both clients return error");
    } catch (Exception e) {
      // expected
      assertEquals(e.getClass(), ExecutionException.class);
    }
  }

  private void maybeDelayExecute(Runnable runnable, long delayMS) {
    if (delayMS > 0) {
      scheduledExecutor.schedule(runnable, delayMS, java.util.concurrent.TimeUnit.MILLISECONDS);
    } else {
      runnable.run();
    }
  }
}
