package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.fastclient.meta.RequestBasedMetadata.INITIAL_METADATA_WARMUP_REFRESH_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.KEY_SCHEMA;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.VALUE_SCHEMA;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.getMockMetaData;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class RequestBasedMetadataTest {
  private static final int CURRENT_VERSION = 1;
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMetadataWarmup(boolean firstUpdateFails) throws IOException, InterruptedException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName);
    RequestBasedMetadata requestBasedMetadata = null;
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    try {
      requestBasedMetadata = getMockMetaData(clientConfig, storeName, false, true, firstUpdateFails, scheduler);
      requestBasedMetadata.start();
      CountDownLatch isReadyLatch = requestBasedMetadata.getIsReadyLatch();

      // 1. verify based on isReadyLatch
      assertEquals(isReadyLatch.getCount(), 0);

      // 2. verify based on the scheduled retries
      RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      if (firstUpdateFails) {
        // schedule retry after WARMUP_REFRESH_INTERVAL_IN_SECONDS
        verify(requestBasedMetadata.getScheduler()).schedule(
            any(Runnable.class),
            eq(INITIAL_METADATA_WARMUP_REFRESH_INTERVAL_IN_SECONDS),
            eq(TimeUnit.SECONDS));
      }
      // after success, both cases should schedule retry after configured refresh interval:
      // testing that start() is only finished after warmup is done
      long periodicRetryAfterSuccessfulWarmup = requestBasedMetadata.getRefreshIntervalInSeconds();
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> {
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
      assertEquals(
          requestBasedMetadata.getReplicas(CURRENT_VERSION, 0),
          Collections.singletonList(RequestBasedMetadataTestUtils.REPLICA1_NAME));
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
      TestUtils.waitForNonDeterministicAssertion(
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
      TestUtils.waitForNonDeterministicAssertion(
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
