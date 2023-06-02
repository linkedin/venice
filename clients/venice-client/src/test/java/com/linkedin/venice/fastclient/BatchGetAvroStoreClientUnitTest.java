package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.beust.jcommander.internal.Lists;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.utils.TestClientSimulator;
import com.linkedin.venice.read.RequestType;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


/**
 * TODO:
 * Success scenarios , original complete , second complete scenarios and combinations
 * We need to make sure all responses are received and processed before claiming test is done
 * When neither the original or the second completes
 * Failure scenarios - First or the second request sends a error or a timeout
 * Different timeout scenarios
 * Multithreaded scenario and testing
 */
public class BatchGetAvroStoreClientUnitTest {
  private static final Logger LOGGER = LogManager.getLogger(BatchGetAvroStoreClientUnitTest.class);
  private static final long TIME_OUT_IN_SECONDS = 10;
  private static final int RETRY_THRESHOLD_IN_MS = 50;
  private static final int NUM_KEYS = 12;
  private static final int NUM_PARTITIONS = 3;

  /**
   * Basic test with 1 partition, 1 replica and 1000 keys
   */
  @Test
  public void testSimpleStreamingBatchGet() throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Error case: Timeout due to response taking a longer time than the simulator's allowed timeout.
   */
  @Test
  public void testSimpleStreamingBatchGettingTimeout()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 2)
        .partitionKeys(2)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues((int) TIME_OUT_IN_SECONDS * 2 * 1000, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture(),
        true);

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    // Timed out before incrementing any counters
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Error case: Setting up Response without setting up request.
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Must have a corresponding request")
  public void testSimpleStreamingBatchGetWithoutRequest()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 2)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(1, 1)
        .simulate();
  }

  /**
   * Error case: Bad Timeline: Response occurs before request.
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Request should happen before response")
  public void testSimpleStreamingBatchGetWithBadTimeLine()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 2)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(2, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(1, 1)
        .simulate();
  }

  /**
   * Error case: Multiple requests on the same route but with different partitions in a separate
   * {@link TestClientSimulator#expectRequestWithKeysForPartitionOnRoute} call. Current implementation
   * of {@link TestClientSimulator} expects all the keys from the same route to be requested together,
   * if not it will fail. so this test will fail.
   */
  @Test(expectedExceptions = AssertionError.class, expectedExceptionsMessageRegExp = "Unexpected key.*")
  public void testSimpleStreamingBatchGetWithBadTimeLineWithSameRoute()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 10)
        .partitionKeys(2)
        .assignRouteToPartitions("https://host1.linkedin.com", 0, 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Similar to {@link #testSimpleStreamingBatchGet} but enables long tail Retry for single get,
   * so client will be an instance of {@link RetriableAvroGenericStoreClient}, but multiGet should not support retry.
   * This test case will fail if we retry multiGet when retry for multiGet is not enabled.
   */
  @Test
  public void testSimpleStreamingBatchGetAndLongTailRetryEnabledForSingleGet()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .setLongTailRetryEnabledForSingleGet(true) // Enable Retry for single get alone
        .setLongTailRetryThresholdForSingleGetInMicroseconds(RETRY_THRESHOLD_IN_MS)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Similar to {@link #testSimpleStreamingBatchGet} but with multiple partitions
   */
  @Test
  public void testSimpleStreamingBatchGetMultiplePartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .partitionKeys(5)
        .assignRouteToPartitions("https://host1.linkedin.com", 0, 1, 2, 3, 4)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0, 1, 2, 3, 4)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Adding multiple routes on top of {@link #testSimpleStreamingBatchGetMultiplePartitions}
   */
  @Test
  public void testStreamingBatchGetMultipleRoutesAndPartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 12)
        .partitionKeys(3)
        .assignRouteToPartitions("https://host0.linkedin.com", 0)
        .assignRouteToPartitions("https://host1.linkedin.com", 1)
        .assignRouteToPartitions("https://host2.linkedin.com", 2)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .respondToRequestWithKeyValues(7, 3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Same as {@link #testStreamingBatchGetMultipleRoutesAndPartitions} but with sequential execution of requests but
   * parallel execution of response: By changing the input timeticks of requests. Just an additional check to make
   * sure the timeticks are working as expected.
   */
  @Test
  public void testStreamingBatchGetMultipleRoutesAndPartitionsV2()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 12)
        .partitionKeys(3)
        .assignRouteToPartitions("https://host0.linkedin.com", 0)
        .assignRouteToPartitions("https://host1.linkedin.com", 1)
        .assignRouteToPartitions("https://host2.linkedin.com", 2)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(2, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(3, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(5, 2)
        .respondToRequestWithKeyValues(5, 3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Introducing >1 replicas: By setting multiple routes to each partitions
   */
  @Test
  public void testStreamingBatchGetMultiplePartitionsPerRoute()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 12)
        .partitionKeys(3)
        .assignRouteToPartitions("https://host0.linkedin.com", 0, 1)
        .assignRouteToPartitions("https://host1.linkedin.com", 1, 2)
        .assignRouteToPartitions("https://host2.linkedin.com", 2, 0)
        // For a request for a partition and numberOfReplicas, respond with
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            0,
            Lists.newArrayList("https://host0.linkedin.com", "https://host2.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            1,
            Lists.newArrayList("https://host1.linkedin.com", "https://host0.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            2,
            Lists.newArrayList("https://host2.linkedin.com", "https://host1.linkedin.com"))
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .respondToRequestWithKeyValues(7, 3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * For all the tests below which will explore the RETRY cases: <br>
   * 1. {@link #setupLongTailRetryWithMultiplePartitions} sets up {@link #RETRY_THRESHOLD_IN_MS} as threshold for retry. <br>
   * 2. 1 timetick is equivalent to 1 ms (check {@link TestClientSimulator#simulate})<br><br>
   *
   * So a request sent and didn't receive reply after {@link #RETRY_THRESHOLD_IN_MS} or that many timeticks will result in a retry.
   */

  /**
   * In this test: retry is not triggered as all the responses are received within 10 timeticks.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsNoRetryTriggered()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .respondToRequestWithKeyValues(10, 3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertFalse(metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Retry succeeds as response for Original request never comes back.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsNoOrigResponse()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Simulate slow route which never actually comes back
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithKeyValues(55, 4)
        .simulate();
    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = NUM_KEYS / NUM_PARTITIONS; // get for 1 partition is retried
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Retry succeeds as response for Original request came after response for retry.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsOrigResponseLate()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithKeyValues(55, 4)
        .respondToRequestWithKeyValues(70, 3) // Response from original request was late
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = NUM_KEYS / NUM_PARTITIONS; // get for 1 partition is retried
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Original succeeds as response for Original request came before response for retry.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsOrigResponseEarly()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithKeyValues(55, 3) // Response from original request came earlier
        .respondToRequestWithKeyValues(70, 4)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = NUM_KEYS / NUM_PARTITIONS; // get for 1 partition is retried
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Retry for requestId 2 is triggered after 50 timeticks (requestId 4) => Response for 2 is returned before response for 4 (which never returns)
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 5) => Response for 5 is returned before response for 3 (which never returns)
   */
  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Route is very slow: never receives reply
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host0.linkedin.com", 1) // retry for requestId 2
        .expectRequestWithKeysForPartitionOnRoute(50, 5, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithKeyValues(55, 2)
        .respondToRequestWithKeyValues(60, 5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = (NUM_KEYS / NUM_PARTITIONS) * 2; // get for 2 partitions are retried
    int expectedNumberOfKeysToBeRetriedSuccessfully = NUM_KEYS / NUM_PARTITIONS; // retry for 1 partition wins
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetriedSuccessfully);
  }

  /**
   * Same as {@link #testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse} but requestId 3 returns Error
   * before the retry for it (requestId 5) starts. The behavior of original request not returning vs returning error is
   * the same as the error is handled in {@link RetriableAvroGenericStoreClient#getStreamingCallback} such that the
   * exception will be thrown further up only when both the original request and the retry fails.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        // Route returns error before even retry starts
        .respondToRequestWithError(40, 3, 500)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host0.linkedin.com", 1) // retry for requestId 2
        .expectRequestWithKeysForPartitionOnRoute(50, 5, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithKeyValues(55, 2)
        .respondToRequestWithKeyValues(60, 5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    /** same as {@link #testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse} */
    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = (NUM_KEYS / NUM_PARTITIONS) * 2; // get for 2 partitions are retried
    int expectedNumberOfKeysToBeRetriedSuccessfully = NUM_KEYS / NUM_PARTITIONS; // retry for 1 partition wins
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetriedSuccessfully);
  }

  /**
   * same as {@link #testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry} but requestId 3 returns Error
   * after the retry for it (requestId 5) starts. It works the same way.
   */
  @Test
  public void testStreamingBatchGetLongTailRetryOriginalRequestErrorAfterRetry()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host0.linkedin.com", 1) // retry for requestId 2
        .expectRequestWithKeysForPartitionOnRoute(50, 5, "https://host1.linkedin.com", 2) // retry for requestId 3
        // Route returns error before even retry starts
        .respondToRequestWithError(53, 3, 500)
        .respondToRequestWithKeyValues(55, 2)
        .respondToRequestWithKeyValues(60, 5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    /** same as {@link #testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse} */
    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = (NUM_KEYS / NUM_PARTITIONS) * 2; // get for 2 partitions are retried
    int expectedNumberOfKeysToBeRetriedSuccessfully = NUM_KEYS / NUM_PARTITIONS; // retry for 1 partition wins
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetriedSuccessfully);
  }

  /**
   * Similar to the above cases but the retry errors out while the original succeeds
   */
  @Test
  public void testStreamingBatchGetLongTailRetryRequestError()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithError(55, 4, 500)
        .respondToRequestWithKeyValues(70, 3) // Response from original request was late
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = NUM_KEYS / NUM_PARTITIONS; // get for 1 partitions are retried
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  /**
   * Both Original request and retry errors out
   */
  @Test
  public void testStreamingBatchGetLongTailBothError()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2) // retry for requestId 3
        .respondToRequestWithError(55, 4, 500) // Error response for retry
        .respondToRequestWithError(70, 3, 500) // Error response for original
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture(),
        true);

    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    int expectedNumberOfKeysToBeRetried = NUM_KEYS / NUM_PARTITIONS; // get for 1 partitions are retried
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertTrue(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_unhealthy_request.OccurrenceRate").value() > 0);

    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true here.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    // The 1 following assert should have been true but counters are not incremented as mentioned above
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate")
            .value() > 0);
    // The 1 following assert should have been true but counters are not incremented as mentioned above
    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_key_count.Max")
            .value() == expectedNumberOfKeysToBeRetried);

    assertFalse(
        metrics.get("." + client.UNIT_TEST_STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
  }

  private TestClientSimulator setupLongTailRetryWithMultiplePartitions(TestClientSimulator client) {
    return client.generateKeyValues(0, NUM_KEYS) // generate NUM_KEYS keys
        .partitionKeys(NUM_PARTITIONS) // partition into NUM_PARTITIONS partitions
        .setLongTailRetryEnabledForBatchGet(true) // enable retry
        .setLongTailRetryThresholdForBatchGetInMicroSeconds(RETRY_THRESHOLD_IN_MS * 1000)

        .assignRouteToPartitions("https://host0.linkedin.com", 0, 1)
        .assignRouteToPartitions("https://host1.linkedin.com", 1, 2)
        .assignRouteToPartitions("https://host2.linkedin.com", 2, 0)

        // For a request for a partition and numOfReplicas, respond with the priority order as specified
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            0,
            Lists.newArrayList("https://host0.linkedin.com", "https://host2.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            1,
            Lists.newArrayList("https://host1.linkedin.com", "https://host0.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(
            2,
            Lists.newArrayList("https://host2.linkedin.com", "https://host1.linkedin.com"));
  }

  private void callStreamingBatchGetAndVerifyResults(
      AvroGenericStoreClient<String, Utf8> fastClient,
      Map<String, String> keyValues,
      CompletableFuture<Integer> simulatorCompletion)
      throws ExecutionException, InterruptedException, TimeoutException {
    callStreamingBatchGetAndVerifyResults(fastClient, keyValues, simulatorCompletion, false);
  }

  private void callStreamingBatchGetAndVerifyResults(
      AvroGenericStoreClient<String, Utf8> fastClient,
      Map<String, String> keyValues,
      CompletableFuture<Integer> simulatorCompletionFuture,
      boolean expectedError) throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, String> results = new ConcurrentHashMap<>();
    AtomicBoolean isComplete = new AtomicBoolean();
    CompletableFuture<Integer> recordCompletionFuture = new CompletableFuture<>();
    fastClient.streamingBatchGet(keyValues.keySet(), new StreamingCallback<String, Utf8>() {
      @Override
      public void onRecordReceived(String key, Utf8 value) {
        LOGGER.info("Record received {}:{}", key, value);
        if ("nonExisting".equals(key)) {
          assertNull(value);
        } else {
          if (results.containsKey(key)) {
            fail("Duplicate value received for key " + key);
          }
          results.put(key, value.toString());
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        LOGGER.info("OnCompletion called . Exception: {} isComplete: {} ", exception, isComplete.get());
        if (!exception.isPresent()) {
          assertEquals(exception, Optional.empty());
          assertTrue(isComplete.compareAndSet(false, true));
          recordCompletionFuture.complete(0);
        } else {
          recordCompletionFuture.completeExceptionally(exception.get());
        }
      }
    });

    CompletableFuture<Void> allCompletionFuture =
        CompletableFuture.allOf(recordCompletionFuture, simulatorCompletionFuture);
    allCompletionFuture.whenComplete((v, e) -> {
      if (e != null) {
        LOGGER.error("Exception received", e);
        if (expectedError) {
          assertFalse(isComplete.get());
        } else {
          fail("Exception received");
        }
      } else {
        LOGGER.info("Test completed successfully");
        assertTrue(isComplete.get());
      }
    });

    /**
     * Below get() on {@link allCompletionFuture} makes the test wait until both the simulation
     * and record completion is done and validated within {@link TIME_OUT_IN_SECONDS}
     */
    try {
      allCompletionFuture.get(TIME_OUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (Exception exception) {
      if (expectedError) {
        LOGGER.info("Test completed successfully because was expecting an exception");
      } else
        throw exception;
    }
  }

  private Map<String, ? extends Metric> getStats(ClientConfig clientConfig) {
    FastClientStats stats = clientConfig.getStats(RequestType.MULTI_GET_STREAMING);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    return metrics;
  }
}
