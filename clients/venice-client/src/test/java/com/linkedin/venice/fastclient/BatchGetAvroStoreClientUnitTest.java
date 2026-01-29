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
import com.linkedin.venice.utils.Time;
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
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final Logger LOGGER = LogManager.getLogger(BatchGetAvroStoreClientUnitTest.class);
  private static final long CLIENT_TIME_OUT_IN_SECONDS = 10;
  private static final int RETRY_THRESHOLD_IN_MS = 50;
  private static final int NUM_KEYS = 12;
  private static final int NUM_PARTITIONS = 3;

  /**
   * Basic test with 1 partition, 1 replica and 1000 keys. No retries.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSimpleStreamingBatchGet() throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .setExpectedValueSchemaId(5)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    validateMetrics(client, 1000, 1000, 0, 0);
  }

  /**
   * Error case: Timeout due to response taking a longer time than the simulator's allowed timeout.
   */
  @Test(timeOut = CLIENT_TIME_OUT_IN_SECONDS * 5 * Time.MS_PER_SECOND)
  public void testSimpleStreamingBatchGettingTimeout()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 2)
        .partitionKeys(2)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues((int) CLIENT_TIME_OUT_IN_SECONDS * 2 * 1000, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture(),
        true);

    // Timed out before incrementing any counters, so pass in all 0s
    validateMetrics(client, 0, 0, 0, 0);
  }

  /**
   * Error case: Setting up Response without setting up request.
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Must have a corresponding request")
  public void testSimpleStreamingBatchGetWithoutRequest() {
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
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Request should happen before response")
  public void testSimpleStreamingBatchGetWithBadTimeLine() {
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
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = AssertionError.class, expectedExceptionsMessageRegExp = "Unexpected key.*")
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

    fail("Expected to throw an exception");
  }

  /**
   * Similar to {@link #testSimpleStreamingBatchGet} but enables long tail Retry for single get.
   * With the new implementation, batch get retry is always enabled with dynamic thresholds.
   * For 1000 keys, the dynamic threshold from "501-:500" is 500ms, which is much higher than the
   * response time (5ms), so no retry should be triggered.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSimpleStreamingBatchGetAndLongTailRetryEnabledForSingleGet()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .setLongTailRetryEnabledForSingleGet(true) // Enable Retry for single get
        .setLongTailRetryThresholdForSingleGetInMicroseconds(RETRY_THRESHOLD_IN_MS)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5, 1) // Fast response (5ms) - no retry triggered
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    validateMetrics(client, 1000, 1000, 0, 0);
  }

  /**
   * Similar to {@link #testSimpleStreamingBatchGet} but with multiple partitions
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testSimpleStreamingBatchGetMultiplePartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, 1000)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(5)
        .assignRouteToPartitions("https://host1.linkedin.com", 0, 1, 2, 3, 4)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host1.linkedin.com", 0, 1, 2, 3, 4)
        .respondToRequestWithKeyValues(5, 1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    validateMetrics(client, 1000, 1000, 0, 0);
  }

  /**
   * Adding multiple routes on top of {@link #testSimpleStreamingBatchGetMultiplePartitions}
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetMultipleRoutesAndPartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, NUM_KEYS)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(NUM_PARTITIONS)
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

    validateMetrics(client, NUM_KEYS, NUM_KEYS, 0, 0);
  }

  /**
   * Same as {@link #testStreamingBatchGetMultipleRoutesAndPartitions} but with sequential execution of requests but
   * parallel execution of response: By changing the input timeticks of requests. Just an additional check to make
   * sure the timeticks are working as expected.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetMultipleRoutesAndPartitionsV2()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, NUM_KEYS)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
        .partitionKeys(NUM_PARTITIONS)
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

    validateMetrics(client, NUM_KEYS, NUM_KEYS, 0, 0);
  }

  /**
   * Introducing >1 replicas: By setting multiple routes to each partitions
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
  public void testStreamingBatchGetMultiplePartitionsPerRoute()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client.generateKeyValues(0, NUM_KEYS)
        .partitionKeys(NUM_PARTITIONS)
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

    validateMetrics(client, NUM_KEYS, NUM_KEYS, 0, 0);
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
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(client, NUM_KEYS, NUM_KEYS, 0, 0);
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Retry succeeds as response for Original request never comes back.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        NUM_KEYS / NUM_PARTITIONS, // get for 1 partition is retried
        NUM_KEYS / NUM_PARTITIONS); // all retries are successful
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Retry succeeds as response for Original request came after response for retry.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        NUM_KEYS / NUM_PARTITIONS, // get for 1 partition is retried
        NUM_KEYS / NUM_PARTITIONS); // all retries are successful
  }

  /**
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 4).
   * Original succeeds as response for Original request came before response for retry.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        NUM_KEYS / NUM_PARTITIONS, // get for 1 partition is retried
        0); // none of the retries succeed
  }

  /**
   * Retry for requestId 2 is triggered after 50 timeticks (requestId 4) => Response for 2 is returned before response for 4 (which never returns)
   * Retry for requestId 3 is triggered after 50 timeticks (requestId 5) => Response for 5 is returned before response for 3 (which never returns)
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        (NUM_KEYS / NUM_PARTITIONS) * 2, // get for 2 partitions are retried
        NUM_KEYS / NUM_PARTITIONS); // retry for 1 partition wins
  }

  /**
   * Same as {@link #testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse} but requestId 3 returns Error
   * before the retry for it (requestId 5) starts. The behavior of original request not returning vs returning error is
   * the same as the error is handled in {@link RetriableAvroGenericStoreClient#getStreamingCallback} such that the
   * exception will be thrown further up only when both the original request and the retry fails.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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
    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        (NUM_KEYS / NUM_PARTITIONS) * 2, // get for 2 partitions are retried
        NUM_KEYS / NUM_PARTITIONS); // retry for 1 partition wins
  }

  /**
   * same as {@link #testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry} but requestId 3 returns Error
   * after the retry for it (requestId 5) starts. It works the same way.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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
    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        (NUM_KEYS / NUM_PARTITIONS) * 2, // get for 2 partitions are retried
        NUM_KEYS / NUM_PARTITIONS); // retry for 1 partition wins
  }

  /**
   * Similar to the above cases but the retry errors out while the original succeeds
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        NUM_KEYS,
        NUM_KEYS / NUM_PARTITIONS, // get for 1 partitions are retried
        0); // no retries are successful
  }

  /**
   * Both Original request and retry errors out
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
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

    validateMetrics(
        client,
        NUM_KEYS,
        0, // ideally its (NUM_KEYS/3) * 2, but we aren't incrementing successful metrics in case of request marked as
           // failed
        (NUM_KEYS / 3),
        0); // no retries are successful. Not counted regardless
  }

  /**
   *  same as {@link #testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry} but with multiple routes
   *  throwing errors.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
  public void testStreamingBatchGetLongTailRetryWithMultipleErrors()
      throws InterruptedException, ExecutionException, TimeoutException {

    int expectedRetryKeyCount = (NUM_KEYS / NUM_PARTITIONS) * 2;
    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithError(6, 2, 500)
        .respondToRequestWithError(7, 3, 500)
        .expectRequestWithKeysForPartitionOnRoute(50, 4, "https://host1.linkedin.com", 2)
        .expectRequestWithKeysForPartitionOnRoute(51, 5, "https://host0.linkedin.com", 1)
        .respondToRequestWithKeyValues(55, 4)
        .respondToRequestWithKeyValues(56, 5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture());

    validateMetrics(client, NUM_KEYS, NUM_KEYS, expectedRetryKeyCount, expectedRetryKeyCount);
  }

  /**
   *  same as {@link #testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry} but request id 3 throws 429 too
   *  many request error. Long tail retry should be skipped and propagate the quota exceeded exception to the caller.
   */
  /**
   * The updated least-loaded routing strategy will return a random replica to guarantee even distribution,
   * so these deterministic assertions won't work anymore.
   * TODO: we need to evaluate whether these test cases are still useful or not.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = false)
  public void testStreamingBatchGetNoLongTailRetryWithTooManyRequest()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1, 1, "https://host0.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1, 2, "https://host1.linkedin.com", 1)
        .expectRequestWithKeysForPartitionOnRoute(1, 3, "https://host2.linkedin.com", 2)
        .respondToRequestWithKeyValues(5, 1)
        .respondToRequestWithKeyValues(6, 2)
        .respondToRequestWithError(7, 3, 429)
        .simulate();

    callStreamingBatchGetAndVerifyResults(
        client.getFastClient(),
        client.getRequestedKeyValues(),
        client.getSimulatorCompleteFuture(),
        true);

    // Technically its (NUM_KEYS/3) * 2 for successful keys, but we aren't incrementing successful metrics in case
    // failed request
    validateMetrics(client, NUM_KEYS, 0, 0, 0);
  }

  private TestClientSimulator setupLongTailRetryWithMultiplePartitions(TestClientSimulator client) {
    return client.generateKeyValues(0, NUM_KEYS) // generate NUM_KEYS keys
        .partitionKeys(NUM_PARTITIONS) // partition into NUM_PARTITIONS partitions

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
        LOGGER.info("OnCompletion called. Exception: {} isComplete: {} ", exception, isComplete.get());
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
     * and record completion is done and validated within {@link CLIENT_TIME_OUT_IN_SECONDS}
     */
    try {
      allCompletionFuture.get(CLIENT_TIME_OUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (Exception exception) {
      LOGGER.info(
          "Record completion future is done: {}, simulator completion future is done: {}",
          recordCompletionFuture.isDone(),
          simulatorCompletionFuture.isDone());
      if (expectedError) {
        LOGGER.info("Test completed successfully because was expecting an exception");
      } else
        throw exception;
    }
  }

  private void validateMetrics(
      TestClientSimulator client,
      double totalNumberOfKeys,
      double totalNumberOfSuccessfulKeys,
      double expectedNumberOfKeysToBeRetried,
      double expectedNumberOfKeysToBeRetriedSuccessfully) {
    Map<String, ? extends Metric> metrics = getStats(client.getClientConfig());
    String metricPrefix = "." + client.UNIT_TEST_STORE_NAME + "--multiget_streaming_";
    if (totalNumberOfKeys > 0) {
      assertTrue(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(metricPrefix + "request_key_count.Max").value(), totalNumberOfKeys);
    } else {
      assertFalse(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "request_key_count.Max").value() > 0);
    }

    if (totalNumberOfSuccessfulKeys > 0) {
      assertTrue(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(metricPrefix + "success_request_key_count.Max").value(), totalNumberOfSuccessfulKeys);
      assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
    } else {
      assertFalse(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "success_request_key_count.Max").value() > 0);
      if (totalNumberOfKeys > 0) {
        assertTrue(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      } else {
        assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      }
    }

    if (expectedNumberOfKeysToBeRetried > 0) {
      assertTrue(metrics.get(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(metricPrefix + "retry_request_key_count.Max").value(), expectedNumberOfKeysToBeRetried);
    } else {
      assertFalse(metrics.get(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "retry_request_key_count.Max").value() > 0);
    }

    if (expectedNumberOfKeysToBeRetriedSuccessfully > 0) {
      assertEquals(
          metrics.get(metricPrefix + "retry_request_success_key_count.Max").value(),
          expectedNumberOfKeysToBeRetriedSuccessfully);
    } else {
      assertFalse(metrics.get(metricPrefix + "retry_request_success_key_count.Max").value() > 0);
    }
  }

  private Map<String, ? extends Metric> getStats(ClientConfig clientConfig) {
    FastClientStats stats = clientConfig.getStats(RequestType.MULTI_GET_STREAMING);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    return metrics;
  }
}
