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
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Time;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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

      // 1. verify based on isReadyLatch
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
   * This is to test warmedUpInstances and warmUpInstancesFutures in case if the one of the conn warmup fails
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

      // 1. verify based on isReadyLatch
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on warmedUpInstances
      RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      Set<String> warmedUpInstances = finalRequestBasedMetadata.getWarmedUpInstances();
      if (firstConnWarmupFails) {
        assertEquals(warmedUpInstances.size(), 1);
        assertTrue(warmedUpInstances.contains(REPLICA2_NAME));
      } else {
        assertEquals(warmedUpInstances.size(), 2);
        assertTrue(warmedUpInstances.contains(REPLICA1_NAME));
        assertTrue(warmedUpInstances.contains(REPLICA2_NAME));
      }

      Map<String, CompletableFuture> warmUpInstancesFutures = finalRequestBasedMetadata.getWarmUpInstancesFutures();
      assertEquals(warmUpInstancesFutures.size(), 2);
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
      assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
      assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());

      // after update runs for the 2nd time as per its scheduled run: warmedUpInstances
      // should contain both the replica in both the case and warmUpInstancesFutures should
      // have REPLICA1_NAME as it tries to warmup only the failed replica
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmedUpInstances.size(), 2);
        assertTrue(warmedUpInstances.contains(REPLICA1_NAME));
        assertTrue(warmedUpInstances.contains(REPLICA2_NAME));
        assertEquals(warmUpInstancesFutures.size(), firstConnWarmupFails ? 1 : 0);
        if (firstConnWarmupFails) {
          assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
          assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
        }
      });
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }

  /**
   * This is to test warmedUpInstances and warmUpInstancesFutures in case if there is a change in the metadata
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

      // 1. verify based on isReadyLatch
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on warmedUpInstances
      RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      Set<String> warmedUpInstances = finalRequestBasedMetadata.getWarmedUpInstances();
      assertEquals(warmedUpInstances.size(), 2);
      assertTrue(warmedUpInstances.contains(REPLICA1_NAME));
      assertTrue(warmedUpInstances.contains(REPLICA2_NAME));

      Map<String, CompletableFuture> warmUpInstancesFutures = finalRequestBasedMetadata.getWarmUpInstancesFutures();
      assertEquals(warmUpInstancesFutures.size(), 2);
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA1_NAME));
      assertTrue(warmUpInstancesFutures.get(REPLICA1_NAME).isDone());
      assertTrue(warmUpInstancesFutures.containsKey(REPLICA2_NAME));
      assertTrue(warmUpInstancesFutures.get(REPLICA2_NAME).isDone());

      // after update runs for the 2nd time as per its scheduled run: warmedUpInstances
      // should contain the latest 2 replicas in both the cases and warmUpInstancesFutures
      // should only have NEW_REPLICA_NAME in case of isMetadataChange being true,
      // as it tries to warm up the new replica
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmedUpInstances.size(), 2);
        assertTrue(warmedUpInstances.contains(isMetadataChange ? NEW_REPLICA_NAME : REPLICA1_NAME));
        assertTrue(warmedUpInstances.contains(REPLICA2_NAME));
        assertEquals(warmUpInstancesFutures.size(), isMetadataChange ? 1 : 0);
        if (isMetadataChange) {
          assertTrue(warmUpInstancesFutures.containsKey(isMetadataChange ? NEW_REPLICA_NAME : REPLICA1_NAME));
          assertTrue(warmUpInstancesFutures.get(isMetadataChange ? NEW_REPLICA_NAME : REPLICA1_NAME).isDone());
        }
      });

      // after update runs for the 3rd time as per its scheduled run: warmedUpInstances
      // should be the same as 2nd run and warmUpInstancesFutures should be 0 as all replicas
      // are now warmed up
      waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertEquals(warmedUpInstances.size(), 2);
        assertTrue(warmedUpInstances.contains(isMetadataChange ? NEW_REPLICA_NAME : REPLICA1_NAME));
        assertTrue(warmedUpInstances.contains(REPLICA2_NAME));
        assertEquals(warmUpInstancesFutures.size(), 0);
      });

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
