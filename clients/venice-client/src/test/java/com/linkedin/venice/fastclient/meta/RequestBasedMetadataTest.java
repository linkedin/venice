package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.fastclient.meta.RequestBasedMetadata.INITIAL_METADATA_WARMUP_REFRESH_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.KEY_SCHEMA;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.NEW_REPLICA_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.REPLICA1_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.REPLICA2_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.VALUE_SCHEMA;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.getMockMetaData;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class RequestBasedMetadataTest {
  private static final int CURRENT_VERSION = 1;
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;

  /**
   * firstMetadataUpdateFails: If this fails, start() will be blocked until metadata can be fetched.
   * firstConnWarmupFails: This is on best effort basis and this failing or succeeding shouldn't have
   *                       any difference in how metadata refresh works.
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMetadataWarmupWithUpdateFailure(boolean firstMetadataUpdateFails, boolean firstConnWarmupFails)
      throws IOException, InterruptedException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName, firstConnWarmupFails);
    RequestBasedMetadata requestBasedMetadata = null;
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, false, true, firstMetadataUpdateFails, scheduler);
      requestBasedMetadata.start();
      CountDownLatch isReadyLatch = requestBasedMetadata.getIsReadyLatch();

      // 1. verify based on isReadyLatch: warmup conn is on best effort, so it won't prevent warmup from finishing
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on the scheduled retries
      RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      if (firstMetadataUpdateFails) {
        // schedule retry after INITIAL_METADATA_WARMUP_REFRESH_INTERVAL_IN_SECONDS
        verify(requestBasedMetadata.getScheduler()).schedule(
            any(Runnable.class),
            eq(INITIAL_METADATA_WARMUP_REFRESH_INTERVAL_IN_SECONDS),
            eq(TimeUnit.SECONDS));
      }
      // after success, both cases should schedule retry after configured refresh interval:
      // testing that start() is only finished after warmup is done
      long periodicRetryAfterSuccessfulWarmup = requestBasedMetadata.getRefreshIntervalInSeconds();
      waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> {
        verify(finalRequestBasedMetadata.getScheduler())
            .schedule(any(Runnable.class), eq(periodicRetryAfterSuccessfulWarmup), eq(TimeUnit.SECONDS));
      });
    } finally {
      scheduler.shutdownNow();
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  /**
   * This is to test warmUpInstancesFutures in case if the one of the conn warmup fails
   *
   * @param firstConnWarmupFails REPLICA1_NAME conn warmup fails for the first time if true
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMetadataWarmupWithConnWarmupFailure(boolean firstConnWarmupFails)
      throws IOException, InterruptedException {
    String storeName = "testStore";
    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName, firstConnWarmupFails);
    RequestBasedMetadata requestBasedMetadata = null;
    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName);
      requestBasedMetadata.start();
      CountDownLatch isReadyLatch = requestBasedMetadata.getIsReadyLatch();

      // 1. verify based on isReadyLatch: warmup conn is on best effort, so it won't prevent warmup from finishing
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on warmUpInstancesFutures
      Map<String, CompletableFuture> warmUpInstancesFutures = requestBasedMetadata.getWarmUpInstancesFutures();
      assertEquals(warmUpInstancesFutures.size(), 2);
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
      assertEquals(warmUpInstancesFutures.get(REPLICA1_NAME).isCompletedExceptionally(), firstConnWarmupFails);
      assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
      assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
      assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());

      // after update runs for the 2nd time as per its scheduled run: both replicas should be warmed up
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmUpInstancesFutures.size(), 2);
        assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
        assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isCompletedExceptionally());
        assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
        assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
        assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
        assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());
      });
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  /**
   * This is to test warmUpInstancesFutures in case if there is a change in the metadata
   *
   * @param isMetadataChange if true, there is a change in the metadata (NEW_REPLICA_NAME replaces REPLICA1_NAME) for the 2nd update
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMetadataWarmupWithMetadataChange(boolean isMetadataChange) throws IOException, InterruptedException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName, false);
    RequestBasedMetadata requestBasedMetadata = null;
    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, isMetadataChange);
      requestBasedMetadata.start();
      CountDownLatch isReadyLatch = requestBasedMetadata.getIsReadyLatch();

      // 1. verify based on isReadyLatch: warmup conn is on best effort, so it won't prevent warmup from finishing
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on warmUpInstancesFutures
      Map<String, CompletableFuture> warmUpInstancesFutures = requestBasedMetadata.getWarmUpInstancesFutures();
      assertEquals(warmUpInstancesFutures.size(), 2);
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
      assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isCompletedExceptionally());
      assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
      assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
      assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());

      // after update runs for the 2nd time as per its scheduled run: warmUpInstancesFutures
      // should have the new replica if there is a metadata change
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmUpInstancesFutures.size(), 2);
        if (isMetadataChange) {
          assertTrue(warmUpInstancesFutures.containsKey(NEW_REPLICA_NAME));
          assertFalse(warmUpInstancesFutures.get(NEW_REPLICA_NAME).isCompletedExceptionally());
          assertTrue(warmUpInstancesFutures.get(NEW_REPLICA_NAME).isDone());
        } else {
          assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
          assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isCompletedExceptionally());
          assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
        }
        assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
        assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
        assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());
      });
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  /**
   * This is to test warmUpInstancesFutures in case if there is an incomplete warmup from the previous run
   *
   * @param isMetadataChange if true, there is a change in the metadata (NEW_REPLICA_NAME replaces REPLICA1_NAME) for the 2nd update
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMetadataWarmupWithIncompleteWarmup(boolean isMetadataChange)
      throws IOException, InterruptedException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName, false);
    RequestBasedMetadata requestBasedMetadata = null;
    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, isMetadataChange);
      Map<String, CompletableFuture> warmUpInstancesFutures = new VeniceConcurrentHashMap<>();
      CompletableFuture replica1Future = new CompletableFuture();
      warmUpInstancesFutures.put(REPLICA1_NAME, replica1Future);
      requestBasedMetadata.setWarmUpInstancesFutures(warmUpInstancesFutures);
      requestBasedMetadata.start();
      CountDownLatch isReadyLatch = requestBasedMetadata.getIsReadyLatch();

      // 1. verify based on isReadyLatch: warmup conn is on best effort, so it won't prevent warmup from finishing
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on warmUpInstancesFutures
      assertEquals(warmUpInstancesFutures.size(), 2);
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
      assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
      assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
      assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());

      // after update runs for the 2nd time as per its scheduled run:
      // If there is a metadata change: warmUpInstancesFutures should have the new replica and
      // the old replica's future should be cancelled
      // If no metadata change: still waiting on the first replica's warmup to be completed
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmUpInstancesFutures.size(), 2);
        if (isMetadataChange) {
          // replica1Future should be cancelled in warmupConnectionToInstances
          assertTrue(replica1Future.isCancelled());
          assertTrue(warmUpInstancesFutures.containsKey(NEW_REPLICA_NAME));
          assertFalse(warmUpInstancesFutures.get(NEW_REPLICA_NAME).isCompletedExceptionally());
          assertTrue(warmUpInstancesFutures.get(NEW_REPLICA_NAME).isDone());
        } else {
          assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
          assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
        }
        assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
        assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
        assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());
      });

      if (!isMetadataChange) {
        // mark the replica1Future as done. Now, after the update runs for the 3rd time as per its scheduled run:
        // warmUpInstancesFutures should be completed for both replicas
        replica1Future.complete(null);
        waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
          assertEquals(warmUpInstancesFutures.size(), 2);
          assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
          assertFalse(warmUpInstancesFutures.get(REPLICA1_NAME).isCompletedExceptionally());
          assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
          assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
          assertFalse(warmUpInstancesFutures.get(REPLICA2_NAME).isCompletedExceptionally());
          assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());
        });
      }

    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMetadata() throws IOException, InterruptedException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName);
    RequestBasedMetadata requestBasedMetadata = null;

    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, true);
      requestBasedMetadata.start();
      assertEquals(requestBasedMetadata.getStoreName(), storeName);
      assertEquals(requestBasedMetadata.getCurrentStoreVersion(), CURRENT_VERSION);
      assertEquals(requestBasedMetadata.getReplicas(CURRENT_VERSION, 0), Collections.singletonList(REPLICA1_NAME));
      assertEquals(
          requestBasedMetadata.getReplicas(CURRENT_VERSION, 1),
          Collections.singletonList(RequestBasedMetadataTestUtils.REPLICA2_NAME));
      assertEquals(requestBasedMetadata.getKeySchema().toString(), KEY_SCHEMA);
      assertEquals(requestBasedMetadata.getValueSchema(1).toString(), VALUE_SCHEMA);
      assertEquals(requestBasedMetadata.getLatestValueSchemaId(), Integer.valueOf(1));
      assertEquals(requestBasedMetadata.getLatestValueSchema().toString(), VALUE_SCHEMA);
      assertEquals(
          requestBasedMetadata.getCompressor(CompressionStrategy.ZSTD_WITH_DICT, CURRENT_VERSION),
          RequestBasedMetadataTestUtils.getZstdVeniceCompressor(storeName));
      final RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertEquals(
              finalRequestBasedMetadata.getReplicas(CURRENT_VERSION, 0),
              Collections.singletonList(RequestBasedMetadataTestUtils.NEW_REPLICA_NAME)));
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMetadataForwardCompat() throws IOException, InterruptedException {
    String storeName = "testStore";
    RequestBasedMetadata requestBasedMetadata = null;
    try {
      RouterBackedSchemaReader routerBackedSchemaReader =
          RequestBasedMetadataTestUtils.getMockRouterBackedSchemaReader();
      ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName);
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, routerBackedSchemaReader, true);
      requestBasedMetadata.start();
      int metadataResponseSchemaId = AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion();
      verify(routerBackedSchemaReader, times(1)).getValueSchema(metadataResponseSchemaId);
      // A new metadata response schema should be fetched for subsequent refreshes
      verify(routerBackedSchemaReader, timeout(3000).times(1)).getValueSchema(metadataResponseSchemaId + 1);
      // Ensure the new routing info with the new metadata response schema is processed successfully
      final RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertEquals(
              finalRequestBasedMetadata.getReplicas(CURRENT_VERSION, 0),
              Collections.singletonList(RequestBasedMetadataTestUtils.NEW_REPLICA_NAME)));
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }
}
