package com.linkedin.venice.fastclient.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.MultiKeyLongTailRetryPolicy;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RequestBasedMetadataRetryPolicyTest {
  private static final int SCHEMA_ID = 5;

  private static class Fixture implements AutoCloseable {
    final ClientConfig config = RequestBasedMetadataTestUtils
        .getMockClientConfig("retry-policy-store", false, false, mock(ScheduledExecutorService.class));
    final D2TransportClient transport = mock(D2TransportClient.class);
    final RouterBackedSchemaReader schemas = RequestBasedMetadataTestUtils.getMockRouterBackedSchemaReader();
    final D2ServiceDiscovery discovery = mock(D2ServiceDiscovery.class);
    final D2ServiceDiscoveryResponse discovered = new D2ServiceDiscoveryResponse();
    final AtomicReference<CompletableFuture<TransportClientResponse>> response = new AtomicReference<>();
    final RequestBasedMetadata metadata;

    Fixture() throws Exception {
      discovered.setCluster("cluster-A");
      discovered.setServerD2Service("servers-A");
      doReturn(discovered).when(discovery).find(any(), anyString(), anyBoolean());
      doAnswer(ignored -> response.get()).when(transport).get(anyString());
      doReturn(Utils.getSchemaFromResource("avro/MetadataResponseRecord/v4/MetadataResponseRecord.avsc")).when(schemas)
          .getValueSchema(4);
      metadata = new RequestBasedMetadata(config, transport);
      metadata.setD2ServiceDiscovery(discovery);
      metadata.setMetadataResponseSchemaReader(schemas);
      metadata.discoverD2Service();
    }

    void refresh(String ranges) throws Exception {
      response.set(CompletableFuture.completedFuture(wireResponse(ranges, SCHEMA_ID)));
      metadata.updateCache(false);
    }

    @Override
    public void close() throws IOException {
      metadata.close();
      config.getClusterStats().getMetricsRepository().close();
    }
  }

  private static MetadataResponseRecord baseRecord() {
    return FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MetadataResponseRecord.SCHEMA$, MetadataResponseRecord.class)
        .deserialize(RequestBasedMetadataTestUtils.buildMetadataResponse(1).getBody());
  }

  private static TransportClientResponse wireResponse(String ranges, int schemaId) throws Exception {
    MetadataResponseRecord record = baseRecord();
    record.setMultiKeyLongTailRetryThresholdsInMs(ranges);
    Schema writer = schemaId == SCHEMA_ID
        ? MetadataResponseRecord.SCHEMA$
        : Utils.getSchemaFromResource("avro/MetadataResponseRecord/v4/MetadataResponseRecord.avsc");
    return serialize(record, writer, schemaId);
  }

  private static TransportClientResponse serialize(MetadataResponseRecord record, Schema writer, int schemaId) {
    return new TransportClientResponse(
        schemaId,
        CompressionStrategy.NO_OP,
        SerializerDeserializerFactory.getAvroGenericSerializer(writer).serialize(record));
  }

  @Test
  public void testRefreshReplaceInvalidRetainAndWithdrawal() throws Exception {
    try (Fixture f = new Fixture()) {
      assertNull(f.metadata.getMultiKeyLongTailRetryPolicy());
      f.refresh("1-:0");
      assertNull(f.metadata.getMultiKeyLongTailRetryPolicy());
      f.refresh("1-:8");
      MultiKeyLongTailRetryPolicy first = f.metadata.getMultiKeyLongTailRetryPolicy();
      assertEquals(first.getRetryThresholdInMicroSeconds(5000), 8000);
      f.refresh("1-:19");
      MultiKeyLongTailRetryPolicy updated = f.metadata.getMultiKeyLongTailRetryPolicy();
      assertEquals(updated.getRetryThresholdInMicroSeconds(5000), 19000);
      f.refresh("1-:2147484");
      assertSame(f.metadata.getMultiKeyLongTailRetryPolicy(), updated);
      assertEquals(f.config.getClusterStats().getMetricValues("invalid_multi_key_retry_policy", "Count").get(0), 2.0);
      f.refresh("");
      assertNull(f.metadata.getMultiKeyLongTailRetryPolicy());
      f.refresh("1-:8");
      f.response.set(CompletableFuture.completedFuture(wireResponse("1-:19", 4)));
      f.metadata.updateCache(false);
      assertNull(f.metadata.getMultiKeyLongTailRetryPolicy(), "old-schema success must withdraw the policy");
      f.refresh("1-:23");
      assertEquals(f.metadata.getMultiKeyLongTailRetryPolicy().getRetryThresholdInMicroSeconds(1), 23000);
    }
  }

  @Test
  public void testFailedRefreshDoesNotPublishCandidate() throws Exception {
    try (Fixture f = new Fixture()) {
      f.refresh("1-:8");
      MultiKeyLongTailRetryPolicy original = f.metadata.getMultiKeyLongTailRetryPolicy();
      CompletableFuture<TransportClientResponse> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("transport failed"));
      f.response.set(failed);
      assertThrows(VeniceClientException.class, () -> f.metadata.updateCache(true));
      assertSame(f.metadata.getMultiKeyLongTailRetryPolicy(), original);

      f.response.set(CompletableFuture.completedFuture(wireResponse("1-:19", SCHEMA_ID)));
      doThrow(new VeniceClientException("schema unavailable")).when(f.schemas).getValueSchema(SCHEMA_ID);
      assertThrows(VeniceClientException.class, () -> f.metadata.updateCache(true));
      assertSame(f.metadata.getMultiKeyLongTailRetryPolicy(), original);
      doReturn(MetadataResponseRecord.SCHEMA$).when(f.schemas).getValueSchema(SCHEMA_ID);

      MetadataResponseRecord badMetadata = baseRecord();
      badMetadata.setMultiKeyLongTailRetryThresholdsInMs("1-:19");
      badMetadata.getVersionMetadata().setPartitionerClass("missing.partitioner");
      f.response
          .set(CompletableFuture.completedFuture(serialize(badMetadata, MetadataResponseRecord.SCHEMA$, SCHEMA_ID)));
      assertThrows(RuntimeException.class, () -> f.metadata.updateCache(true));
      assertSame(f.metadata.getMultiKeyLongTailRetryPolicy(), original, "a parsed candidate is not yet published");
      f.refresh("1-:19");
      assertEquals(f.metadata.getMultiKeyLongTailRetryPolicy().getRetryThresholdInMicroSeconds(1), 19000);
    }
  }

  @DataProvider
  public Object[][] migrationPolicies() {
    return new Object[][] { { "" }, { "1-:0" }, { "1-:19" } };
  }

  @Test(dataProvider = "migrationPolicies")
  public void testClusterMigrationNeverRetainsOldOrigin(String ranges) throws Exception {
    try (Fixture f = new Fixture()) {
      f.refresh("1-:8");
      f.discovered.setCluster("cluster-B");
      f.discovered.setServerD2Service("servers-B");
      CompletableFuture<TransportClientResponse> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("rediscover"));
      doReturn(failed, CompletableFuture.completedFuture(wireResponse(ranges, SCHEMA_ID))).when(f.transport)
          .get(anyString());
      f.metadata.updateCache(false);
      assertEquals(f.metadata.getClusterName(), "cluster-B");
      if ("1-:19".equals(ranges)) {
        assertEquals(f.metadata.getMultiKeyLongTailRetryPolicy().getRetryThresholdInMicroSeconds(1), 19000);
      } else {
        assertNull(f.metadata.getMultiKeyLongTailRetryPolicy());
      }
      // Once migration is discovered, even a failed first fetch must not expose the old cluster's policy.
      f.discovered.setCluster("cluster-C");
      doReturn(failed).when(f.transport).get(anyString());
      assertThrows(VeniceClientException.class, () -> f.metadata.updateCache(false));
      assertNull(f.metadata.getMultiKeyLongTailRetryPolicy());
    }
  }

  @Test
  public void testDeferredVersionStillAdoptsClusterPolicy() throws Exception {
    try (Fixture f = new Fixture()) {
      f.refresh("1-:8");
      f.metadata.start();
      MetadataResponseRecord deferred = FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(MetadataResponseRecord.SCHEMA$, MetadataResponseRecord.class)
          .deserialize(
              RequestBasedMetadataTestUtils.buildDeferredSwitchMetadataResponse(1, 2, StorageMode.INTERNAL).getBody());
      deferred.setMultiKeyLongTailRetryThresholdsInMs("1-:19");
      f.response.set(CompletableFuture.completedFuture(serialize(deferred, MetadataResponseRecord.SCHEMA$, SCHEMA_ID)));
      f.metadata.updateCache(false);
      assertEquals(f.metadata.getCurrentStoreVersion(), 1);
      assertEquals(f.metadata.getMultiKeyLongTailRetryPolicy().getRetryThresholdInMicroSeconds(1), 19000);
    }
  }

  @Test(timeOut = 30000)
  public void testConcurrentRefreshPublishesCompleteImmutablePolicy() throws Exception {
    try (Fixture f = new Fixture()) {
      f.refresh("1-500:1,501-:2");
      MultiKeyLongTailRetryPolicy captured = f.metadata.getMultiKeyLongTailRetryPolicy();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      CountDownLatch start = new CountDownLatch(1);
      AtomicBoolean finished = new AtomicBoolean();
      try {
        CompletableFuture<Void> reads = CompletableFuture.runAsync(() -> {
          start.countDown();
          do {
            MultiKeyLongTailRetryPolicy snapshot = f.metadata.getMultiKeyLongTailRetryPolicy();
            int small = snapshot.getRetryThresholdInMicroSeconds(500);
            int large = snapshot.getRetryThresholdInMicroSeconds(501);
            assertTrue((small == 1000 && large == 2000) || (small == 3000 && large == 4000));
          } while (!finished.get());
        }, executor);
        assertTrue(start.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < 20; i++) {
          f.refresh(i % 2 == 0 ? "1-500:3,501-:4" : "1-500:1,501-:2");
        }
        finished.set(true);
        reads.get(5, TimeUnit.SECONDS);
        assertEquals(captured.getRetryThresholdInMicroSeconds(501), 2000);
      } finally {
        finished.set(true);
        executor.shutdownNow();
      }
    }
  }
}
