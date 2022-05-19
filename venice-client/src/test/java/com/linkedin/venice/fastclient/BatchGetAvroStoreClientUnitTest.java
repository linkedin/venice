package com.linkedin.venice.fastclient;

import com.beust.jcommander.internal.Lists;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.fastclient.utils.TestClientSimulator;
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
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * TODO:
 * Test non streaming batch get use cases
 * Verify stats?
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

  /**
   * 1 partition , 1 replica
   * @throws InterruptedException
   */
  @Test
  public void testSimpleStreamingBatchGet() throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client
        .generateKeyValues(0, 1000)
        .partitionKeys(1)
        .assignRouteToPartitions("https://host1.linkedin.com", 0)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host1.linkedin.com", 0)
        .respondToRequestWithKeyValues(5,1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }


  @Test
  public void testSimpleStreamingBatchGetMultiplePartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client
        .generateKeyValues(0, 1000)
        .partitionKeys(5)
        .assignRouteToPartitions("https://host1.linkedin.com", 0, 1, 2, 3, 4)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host1.linkedin.com",
            0, 1, 2, 3, 4)
        .respondToRequestWithKeyValues(5,1)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetMultipleRoutesAndPartitions()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client
        .generateKeyValues(0, 12)
        .partitionKeys(3)
        .assignRouteToPartitions("https://host0.linkedin.com", 0)
        .assignRouteToPartitions("https://host1.linkedin.com", 1)
        .assignRouteToPartitions("https://host2.linkedin.com", 2)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .respondToRequestWithKeyValues(7,3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }


  @Test
  public void testStreamingBatchGetMultiplePartitionsPerRoute()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    client
        .generateKeyValues(0, 12)
        .partitionKeys(3)
        .assignRouteToPartitions("https://host0.linkedin.com", 0, 1)
        .assignRouteToPartitions("https://host1.linkedin.com", 1, 2)
        .assignRouteToPartitions("https://host2.linkedin.com", 2, 0)
        // whenever I get a request for a partition and numberofreplicas respond with
        .expectReplicaRequestForPartitionAndRespondWithReplicas(0,
            Lists.newArrayList("https://host0.linkedin.com","https://host1.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(1,
            Lists.newArrayList("https://host1.linkedin.com","https://host0.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(2,
            Lists.newArrayList("https://host2.linkedin.com","https://host0.linkedin.com"))
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .respondToRequestWithKeyValues(7,3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsNoRetryTriggered()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .respondToRequestWithKeyValues(10,3)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsNoOrigResponse()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Simulate slow route which never actually comes back
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host1.linkedin.com",
            2)
        .respondToRequestWithKeyValues(55,4)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }


  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsOrigResponseLate()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host1.linkedin.com",
            2)
        .respondToRequestWithKeyValues(55,4)
        .respondToRequestWithKeyValues(70,3) // Response from original request was late
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsOrigResponseEarly()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host1.linkedin.com",
            2)
        .respondToRequestWithKeyValues(55,3) // Response from original request came earlier
        .respondToRequestWithKeyValues(70,4)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryMultiplePartitionsMixedResponse()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Route is very slow never
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host0.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(50,5, "https://host1.linkedin.com",
            2)
        .respondToRequestWithKeyValues(55,2)
        .respondToRequestWithKeyValues(60,5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryOriginalRequestErrorBeforeRetry()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host0.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(50,5, "https://host1.linkedin.com",
            2)
        // Route returns error before even retry starts
        .respondToRequestWithError(40,3, 500)
        .respondToRequestWithKeyValues(55,2)
        .respondToRequestWithKeyValues(60,5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryOriginalRequestErrorAfterRetry()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        // Route is little slow
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host0.linkedin.com",
            1)
        .expectRequestWithKeysForPartitionOnRoute(50,5, "https://host1.linkedin.com",
            2)
        // Route returns error before even retry starts
        .respondToRequestWithError(53,3, 500)
        .respondToRequestWithKeyValues(55,2)
        .respondToRequestWithKeyValues(60,5)
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailRetryRequestError()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host1.linkedin.com",
            2)
        .respondToRequestWithError(55,4,500)
        .respondToRequestWithKeyValues(70,3) // Response from original request was late
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete());
  }

  @Test
  public void testStreamingBatchGetLongTailBothError()
      throws InterruptedException, ExecutionException, TimeoutException {

    TestClientSimulator client = new TestClientSimulator();
    setupLongTailRetryWithMultiplePartitions(client)
        .expectRequestWithKeysForPartitionOnRoute(1,1, "https://host0.linkedin.com",
            0)
        .expectRequestWithKeysForPartitionOnRoute(1,2, "https://host1.linkedin.com",
            1)
        // Simulate slow route
        .expectRequestWithKeysForPartitionOnRoute(1,3, "https://host2.linkedin.com",
            2)
        .respondToRequestWithKeyValues(5,1)
        .respondToRequestWithKeyValues(6,2)
        .expectRequestWithKeysForPartitionOnRoute(50,4, "https://host1.linkedin.com",
            2)
        .respondToRequestWithError(55,4,500)
        .respondToRequestWithError(70,3, 500) // Error response
        .simulate();

    callStreamingBatchGetAndVerifyResults(client.getFastClient(), client.getKeyValues(), client.getSimulatorComplete()
    , true);
  }

  private TestClientSimulator setupLongTailRetryWithMultiplePartitions(TestClientSimulator client) {
    return client.generateKeyValues(0, 12) // generate 12 keys
        .partitionKeys(3) // partition into 3 partitions 0 , 1, 2
        .setLongTailRetryEnabledForBatchGet(true) // enable retry
        .setLongTailRetryThresholdForBatchGetInMicroseconds(50000) // 50 ms

        .assignRouteToPartitions("https://host0.linkedin.com", 0, 1)
        .assignRouteToPartitions("https://host1.linkedin.com", 1, 2)
        .assignRouteToPartitions("https://host2.linkedin.com", 2, 0)
        // whenever I get a request for a partition respond with the priority order as specified
        .expectReplicaRequestForPartitionAndRespondWithReplicas(0,
            Lists.newArrayList("https://host0.linkedin.com", "https://host2.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(1,
            Lists.newArrayList("https://host1.linkedin.com", "https://host0.linkedin.com"))
        .expectReplicaRequestForPartitionAndRespondWithReplicas(2,
            Lists.newArrayList("https://host2.linkedin.com", "https://host1.linkedin.com"));
  }

  private void callStreamingBatchGetAndVerifyResults(AvroGenericStoreClient<String, Utf8> fastClient,
      Map<String,String> keyValues, CompletableFuture<Integer> simulatorCompletion)
      throws ExecutionException, InterruptedException, TimeoutException {
    callStreamingBatchGetAndVerifyResults(fastClient, keyValues, simulatorCompletion, false);
  }
  private void callStreamingBatchGetAndVerifyResults(AvroGenericStoreClient<String, Utf8> fastClient,
      Map<String,String> keyValues, CompletableFuture<Integer> simulatorCompletion, boolean expectedError)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, String> results = new ConcurrentHashMap<>();
    AtomicBoolean isComplete = new AtomicBoolean();
    CompletableFuture<Integer> recordCompletion = new CompletableFuture<>();
    fastClient.streamingBatchGet(keyValues.keySet(), new StreamingCallback<String, Utf8>() {
      @Override
      public void onRecordReceived(String key, Utf8 value) {
        LOGGER.info("Record received " + key + ":" + value);
        if ( "nonExisting".equals(key)) {
          Assert.assertNull(value);
        } else {
          if ( results.containsKey(key)) {
            Assert.fail("Duplicate value received for key " + key);
          }
          results.put(key, value.toString());
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        LOGGER.info("OnCompletion called . Exception: {} isComplete: {} " , exception, isComplete.get());
        if ( !exception.isPresent()) {
          Assert.assertEquals(exception,Optional.empty());
          Assert.assertTrue(isComplete.compareAndSet(false,true));
          recordCompletion.complete(0);
        } else {
          recordCompletion.completeExceptionally(exception.get());
        }
      }
    });

    CompletableFuture<Void> allCompletion = CompletableFuture.allOf(recordCompletion, simulatorCompletion);
    allCompletion.whenComplete((v, e) -> {
      if ( e != null) {
        LOGGER.error("Exception received" , e);
        if ( expectedError) {
          Assert.assertFalse(isComplete.get());
        } else {
          Assert.fail("Exception received");
        }
      } else {
        LOGGER.info("Test completed successfully");
        Assert.assertTrue(isComplete.get());
      }
    });
    try {
      allCompletion.get(TIME_OUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException exception) {
      if ( expectedError) {
        LOGGER.info("Test completed successfully because was expecting an exception");
      } else throw exception;
    }
  }
}
