package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.fastclient.utils.TestClientSimulator;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


/**
 * Focused tests for the fast-client optimization where a {@code streamingBatchGet} containing a single key is served
 * via a single-GET request instead of a multi-get request (see
 * {@link DispatchingAvroGenericStoreClient#streamingBatchGet} and
 * {@link RetriableAvroGenericStoreClient#streamingBatchGet}).
 *
 * <p>{@link TestClientSimulator} asserts the request shape on the wire: a single-key request must produce a single-GET
 * request ({@link TestClientSimulator#expectSingleGetRequestWithKeyForPartitionOnRoute}) and increment the
 * {@code batch_get_routed_to_single_get_request_count} metric, while a multi-key request must produce a multi-get
 * request and leave that metric at zero.
 */
public class SingleKeyBatchGetAsSingleGetTest {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final long CLIENT_TIME_OUT_IN_SECONDS = 10;

  /**
   * A single-key batch-get is served via a single-GET request and is counted by the reroute metric.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleKeyBatchGetRoutedToSingleGet()
      throws InterruptedException, ExecutionException, TimeoutException {
    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectSingleGetRequestWithKeyForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    Map<String, String> results = streamingBatchGetAndCollect(client, false);
    assertEquals(results, client.getRequestedKeyValues());

    assertTrue(rerouteMetricValue(client) > 0, "Single-key batch-get should be counted as routed to single-GET");
  }

  /**
   * A single-key batch-get propagates a single-GET failure to the streaming callback's {@code onCompletion}. A 429 is
   * used so the single-GET path does not trigger an error retry (which would require a second simulated request).
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleKeyBatchGetErrorPropagated() throws InterruptedException, ExecutionException, TimeoutException {
    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectSingleGetRequestWithKeyForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithError(5, 1, 429)
        .simulate();

    // Expecting the streaming callback to complete exceptionally.
    streamingBatchGetAndCollect(client, true);
  }

  /**
   * Contrast: a multi-key batch-get is NOT rerouted; it is served via a multi-get request and the reroute metric
   * stays at zero.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiKeyBatchGetNotRoutedToSingleGet()
      throws InterruptedException, ExecutionException, TimeoutException {
    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 2)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    Map<String, String> results = streamingBatchGetAndCollect(client, false);
    assertEquals(results, client.getRequestedKeyValues());

    assertFalse(rerouteMetricValue(client) > 0, "Multi-key batch-get must not be counted as routed to single-GET");
  }

  /**
   * Issues a {@code streamingBatchGet} for all of the simulator's keys, collecting the received values. Blocks until
   * both the record stream and the simulation complete (or fail).
   */
  private Map<String, String> streamingBatchGetAndCollect(TestClientSimulator client, boolean expectError)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, String> results = new ConcurrentHashMap<>();
    CompletableFuture<Void> recordCompletion = new CompletableFuture<>();
    client.getFastClient()
        .streamingBatchGet(client.getRequestedKeyValues().keySet(), new StreamingCallback<String, Utf8>() {
          @Override
          public void onRecordReceived(String key, Utf8 value) {
            if (value != null) {
              results.put(key, value.toString());
            }
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            if (exception.isPresent()) {
              recordCompletion.completeExceptionally(exception.get());
            } else {
              recordCompletion.complete(null);
            }
          }
        });

    try {
      CompletableFuture.allOf(recordCompletion, client.getSimulatorCompleteFuture())
          .get(CLIENT_TIME_OUT_IN_SECONDS, TimeUnit.SECONDS);
      if (expectError) {
        fail("Expected the single-key batch-get to fail");
      }
    } catch (ExecutionException e) {
      if (!expectError) {
        throw e;
      }
    }
    return results;
  }

  private double rerouteMetricValue(TestClientSimulator client) {
    Map<String, ? extends Metric> metrics =
        client.getClientConfig().getStats(RequestType.MULTI_GET_STREAMING).getMetricsRepository().metrics();
    return metrics
        .get(
            "." + client.UNIT_TEST_STORE_NAME
                + "--multiget_streaming_batch_get_routed_to_single_get_request_count.OccurrenceRate")
        .value();
  }
}
