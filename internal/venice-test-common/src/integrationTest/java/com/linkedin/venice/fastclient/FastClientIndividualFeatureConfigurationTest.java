package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class FastClientIndividualFeatureConfigurationTest extends AbstractClientEndToEndSetup {
  private static final Logger LOGGER = LogManager.getLogger();

  @Test(timeOut = TIME_OUT)
  public void testServerReadQuota() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false);
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    // Update the read quota to 1000 and make 500 requests, all requests should be allowed.
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils
          .assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(1000)));
    });
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < recordCnt; i++) {
        String key = keyPrefix + i;
        GenericRecord value = genericFastClient.get(key).get();
        assertEquals((int) value.get(VALUE_FIELD_NAME), i);
      }
    }
    ArrayList<MetricsRepository> serverMetrics = new ArrayList<>();
    for (int i = 0; i < veniceCluster.getVeniceServers().size(); i++) {
      serverMetrics.add(veniceCluster.getVeniceServers().get(i).getMetricsRepository());
    }
    String readQuotaStorageNodeTokenBucketRemaining =
        ".venice-storage-node-token-bucket--QuotaRcuTokensRemaining.Gauge";
    String readQuotaRequestedString = "." + storeName + "--quota_rcu_requested.Count";
    String readQuotaRejectedString = "." + storeName + "--quota_rcu_rejected.Count";
    String readQuotaAllowedUnintentionally = "." + storeName + "--quota_rcu_allowed_unintentionally.Count";
    String readQuotaUsageRatio = "." + storeName + "--quota_requested_usage_ratio.Gauge";
    String clientConnectionCountGaugeString = ".server_connection_stats--client_connection_count.Gauge";
    String routerConnectionCountGaugeString = ".server_connection_stats--router_connection_count.Gauge";
    String clientConnectionCountRateString = ".server_connection_stats--client_connection_count.OccurrenceRate";
    String routerConnectionCountRateString = ".server_connection_stats--router_connection_count.OccurrenceRate";
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (MetricsRepository serverMetric: serverMetrics) {
        assertNotNull(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining));
        assertNotNull(serverMetric.getMetric(readQuotaRequestedString));
        assertNotNull(serverMetric.getMetric(readQuotaRejectedString));
        assertNotNull(serverMetric.getMetric(readQuotaUsageRatio));
        assertNotNull(serverMetric.getMetric(readQuotaAllowedUnintentionally));
        assertNotNull(serverMetric.getMetric(clientConnectionCountGaugeString));
        assertNotNull(serverMetric.getMetric(routerConnectionCountGaugeString));
        assertNotNull(serverMetric.getMetric(clientConnectionCountRateString));
        assertNotNull(serverMetric.getMetric(routerConnectionCountRateString));
      }
    });
    int quotaRequestedSum = 0;
    int clientConnectionCountRateSum = 0;
    int routerConnectionCountRateSum = 0;
    for (MetricsRepository serverMetric: serverMetrics) {
      quotaRequestedSum += serverMetric.getMetric(readQuotaRequestedString).value();
      clientConnectionCountRateSum += serverMetric.getMetric(clientConnectionCountRateString).value();
      routerConnectionCountRateSum += serverMetric.getMetric(routerConnectionCountRateString).value();
      assertEquals(serverMetric.getMetric(readQuotaRejectedString).value(), 0d);
      assertEquals(serverMetric.getMetric(readQuotaAllowedUnintentionally).value(), 0d);
      assertTrue(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining).value() > 0d);
    }
    assertTrue(quotaRequestedSum >= 500, "Quota requested sum: " + quotaRequestedSum);
    assertTrue(clientConnectionCountRateSum > 0, "Servers should have more than 0 client connections");
    assertEquals(routerConnectionCountRateSum, 0, "Servers should have 0 router connections");

    // Update the read quota to 50 and make as many requests needed to trigger quota rejected exception.
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils
          .assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(50)));
    });
    try {
      // Keep making requests until it gets rejected by read quota, it may take some time for the quota update to be
      // propagated to servers.
      for (int j = 0; j < Integer.MAX_VALUE; j++) {
        for (int i = 0; i < recordCnt; i++) {
          String key = keyPrefix + i;
          GenericRecord value = genericFastClient.get(key).get();
          assertEquals((int) value.get(VALUE_FIELD_NAME), i);
        }
      }
      fail("Exception should be thrown due to read quota violation");
    } catch (Exception clientException) {
      assertTrue(clientException.getMessage().contains("VeniceClientRateExceededException"));
    }
    boolean readQuotaRejected = false;
    for (MetricsRepository serverMetric: serverMetrics) {
      if (serverMetric.getMetric(readQuotaRejectedString).value() > 0) {
        readQuotaRejected = true;
      }
    }
    assertTrue(readQuotaRejected);
    // Restart the servers and quota should still be working
    for (VeniceServerWrapper veniceServerWrapper: veniceCluster.getVeniceServers()) {
      LOGGER.info("RESTARTING servers");
      veniceCluster.stopAndRestartVeniceServer(veniceServerWrapper.getPort());
    }
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < recordCnt; i++) {
        String key = keyPrefix + i;
        GenericRecord value = genericFastClient.get(key).get();
        assertEquals((int) value.get(VALUE_FIELD_NAME), i);
      }
    }
    quotaRequestedSum = 0;
    for (MetricsRepository serverMetric: serverMetrics) {
      quotaRequestedSum += serverMetric.getMetric(readQuotaRequestedString).value();
      assertEquals(serverMetric.getMetric(readQuotaAllowedUnintentionally).value(), 0d);
      assertTrue(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining).value() > 0d);
    }
    assertTrue(quotaRequestedSum >= 500, "Quota requested sum: " + quotaRequestedSum);
  }

  @Test(timeOut = TIME_OUT)
  public void testServerRejectReadComputeRequest() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false);
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);

    String key = keyPrefix + 0;

    // When read-computation is enabled, FC should return the value.
    VeniceResponseMap<String, ComputeGenericRecord> responseMap = genericFastClient.compute()
        .project(VALUE_FIELD_NAME)
        .streamingExecute(Collections.singleton(key))
        .get(TIME_OUT, TimeUnit.MILLISECONDS);
    assertFalse(responseMap.isEmpty());
    assertTrue(responseMap.isFullResponse());

    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(false)));
    });

    try {
      genericFastClient.compute()
          .project(VALUE_FIELD_NAME)
          .execute(Collections.singleton(key))
          .get(TIME_OUT, TimeUnit.MILLISECONDS);
      fail();
    } catch (Exception clientException) {
      assertTrue(ExceptionUtils.recursiveMessageContains(clientException, "Read compute is not enabled for the store"));
    }

    VeniceResponseMap<String, ComputeGenericRecord> responseMapWhenComputeDisabled = genericFastClient.compute()
        .project(VALUE_FIELD_NAME)
        .streamingExecute(Collections.singleton(key))
        .get(TIME_OUT, TimeUnit.MILLISECONDS);
    assertTrue(responseMapWhenComputeDisabled.isEmpty());
    assertFalse(responseMapWhenComputeDisabled.isFullResponse());
  }

  @Test(timeOut = TIME_OUT)
  public void testStreamingBatchGetWithMultiValueSchemaVersions() throws Exception {
    veniceCluster.useControllerClient(client -> client.addValueSchema(storeName, VALUE_SCHEMA_V2_STR));
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      keys.add(key);
    }
    Map<String, GenericRecord> resultMap = genericFastClient.batchGet(keys).get();
    assertEquals(resultMap.size(), recordCnt);
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      assertEquals((int) resultMap.get(key).get(VALUE_FIELD_NAME), i);
      assertNull(resultMap.get(key).get(SECOND_VALUE_FIELD_NAME));
    }
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, false);
    assertFalse(creationResponse.isError());
    PubSubBrokerWrapper pubSubBrokerWrapper = veniceCluster.getPubSubBrokerWrapper();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    final String storeVersion = creationResponse.getKafkaTopic();
    VeniceWriter<Object, Object, Object> veniceWriter =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(storeVersion).setKeySerializer(keySerializer)
                    .setValueSerializer(new VeniceAvroKafkaSerializer(VALUE_SCHEMA_V2_STR))
                    .build());
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    for (int i = 0; i < recordCnt; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA_V2);
      record.put(VALUE_FIELD_NAME, i);
      record.put(SECOND_VALUE_FIELD_NAME, i);
      veniceWriter.put(keyPrefix + i, record, 2).get();
    }
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicPushCompletion(storeVersion, controllerClient, 30, TimeUnit.SECONDS);
    });
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      final Map<String, GenericRecord> result = genericFastClient.batchGet(keys).get();
      assertEquals(result.size(), recordCnt);
      for (int i = 0; i < recordCnt; ++i) {
        String key = keyPrefix + i;
        assertEquals((int) result.get(key).get(VALUE_FIELD_NAME), i);
        assertEquals(result.get(key).get(SECOND_VALUE_FIELD_NAME), i);
      }
    });
  }
}
