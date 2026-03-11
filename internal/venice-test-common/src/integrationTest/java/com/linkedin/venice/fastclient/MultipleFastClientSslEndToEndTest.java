package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode.SERVER_BASED_METADATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * Integration test that verifies multiple fast clients can concurrently connect to
 * Venice servers over SSL and successfully read data. This validates that the cached
 * SSL factory in the server's channel initializer correctly handles concurrent
 * SSL connections from different client instances.
 */
public class MultipleFastClientSslEndToEndTest extends AbstractClientEndToEndSetup {
  private static final int NUM_CLIENTS = 5;

  @Test(timeOut = TIME_OUT)
  public void testMultipleFastClientsCanConcurrentlyReadOverSsl() throws Exception {
    List<AvroGenericStoreClient<String, GenericRecord>> clients = new ArrayList<>();

    try {
      // Create multiple fast clients, each with its own R2 transport and metrics
      for (int c = 0; c < NUM_CLIENTS; c++) {
        ClientConfig.ClientConfigBuilder clientConfigBuilder =
            new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
                .setR2Client(r2Client)
                .setDualReadEnabled(false)
                .setStoreMetadataFetchMode(SERVER_BASED_METADATA)
                .setD2Client(d2Client)
                .setClusterDiscoveryD2Service(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
                .setMetadataRefreshIntervalInSeconds(1)
                .setMetricsRepository(createVeniceMetricsRepository(false));

        AvroGenericStoreClient<String, GenericRecord> client =
            ClientFactory.getAndStartGenericStoreClient(clientConfigBuilder.build());
        clients.add(client);
      }

      // Issue concurrent single-get requests from all clients simultaneously
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (int c = 0; c < NUM_CLIENTS; c++) {
        final AvroGenericStoreClient<String, GenericRecord> client = clients.get(c);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            for (int i = 0; i < recordCnt; i++) {
              String key = keyPrefix + i;
              GenericRecord value = client.get(key).get();
              assertNotNull(value, "Client should get non-null value for key: " + key);
              assertEquals((int) value.get(VALUE_FIELD_NAME), i);
            }
            assertNull(client.get("nonExistingKey").get());
          } catch (Exception e) {
            throw new RuntimeException("Fast client read failed", e);
          }
        });
        futures.add(future);
      }

      // Wait for all clients to complete their reads
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

      // Also verify batch get works concurrently from all clients
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < recordCnt; i++) {
        keys.add(keyPrefix + i);
      }

      List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
      for (int c = 0; c < NUM_CLIENTS; c++) {
        final AvroGenericStoreClient<String, GenericRecord> client = clients.get(c);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            Map<String, GenericRecord> resultMap = client.batchGet(keys).get();
            assertEquals(resultMap.size(), recordCnt);
            for (int i = 0; i < recordCnt; i++) {
              String key = keyPrefix + i;
              assertEquals((int) resultMap.get(key).get(VALUE_FIELD_NAME), i);
            }
          } catch (Exception e) {
            throw new RuntimeException("Fast client batch get failed", e);
          }
        });
        batchFutures.add(future);
      }

      CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
    } finally {
      for (AvroGenericStoreClient<String, GenericRecord> client: clients) {
        client.close();
      }
    }
  }
}
