package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class FastClientIndividualFeatureConfigurationTest extends AbstractClientEndToEndSetup {
  private static final Logger LOGGER = LogManager.getLogger();

  /*
   * @{link AbstractClientEndToEndSetup} enables the read quota for stores as part of prepareData and stores are
   * typically reused at a class level for tests. However, some tests require flows to disable read quota, and
   * it is important to reset so that these tests don't step on other tests. For now, we choose to add a before
   * method to class that validates disabled read quota behavior to reset the state to ensure other tests run
   * successfully.
   */
  @BeforeMethod
  public void enableStoreReadQuota() {
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true)));
    });
  }

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
      TestUtils.assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setReadQuotaInCU(1000).setStorageNodeReadQuotaEnabled(true)));
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
    String readQuotaRequestedQPSString = "." + storeName + "--quota_request.Rate";
    String readQuotaRejectedQPSString = "." + storeName + "--quota_rejected_request.Rate";
    String readQuotaRequestedKPSString = "." + storeName + "--quota_request_key_count.Rate";
    String readQuotaRejectedKPSString = "." + storeName + "--quota_rejected_key_count.Rate";
    String readQuotaAllowedUnintentionally = "." + storeName + "--quota_unintentionally_allowed_key_count.Count";
    String readQuotaUsageRatio = "." + storeName + "--quota_requested_usage_ratio.Gauge";
    String errorRequestString = ".total--error_request.OccurrenceRate";
    String clientConnectionCountGaugeString = ".server_connection_stats--client_connection_count.Gauge";
    String routerConnectionCountGaugeString = ".server_connection_stats--router_connection_count.Gauge";
    String clientConnectionCountRateString = ".server_connection_stats--client_connection_request.OccurrenceRate";
    String routerConnectionCountRateString = ".server_connection_stats--router_connection_request.OccurrenceRate";
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (MetricsRepository serverMetric: serverMetrics) {
        assertNotNull(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining));
        assertNotNull(serverMetric.getMetric(readQuotaRequestedQPSString));
        assertNotNull(serverMetric.getMetric(readQuotaRejectedQPSString));
        assertNotNull(serverMetric.getMetric(readQuotaRequestedKPSString));
        assertNotNull(serverMetric.getMetric(readQuotaRejectedKPSString));
        assertNotNull(serverMetric.getMetric(readQuotaUsageRatio));
        assertNotNull(serverMetric.getMetric(errorRequestString));
        assertNotNull(serverMetric.getMetric(readQuotaAllowedUnintentionally));
        assertNotNull(serverMetric.getMetric(clientConnectionCountGaugeString));
        assertNotNull(serverMetric.getMetric(routerConnectionCountGaugeString));
        assertNotNull(serverMetric.getMetric(clientConnectionCountRateString));
        assertNotNull(serverMetric.getMetric(routerConnectionCountRateString));
      }
    });
    int quotaRequestedQPSSum = 0;
    int quotaRequestedKPSSum = 0;
    int clientConnectionCountRateSum = 0;
    int routerConnectionCountRateSum = 0;
    for (MetricsRepository serverMetric: serverMetrics) {
      quotaRequestedQPSSum += serverMetric.getMetric(readQuotaRequestedQPSString).value();
      quotaRequestedKPSSum += serverMetric.getMetric(readQuotaRequestedKPSString).value();
      clientConnectionCountRateSum += serverMetric.getMetric(clientConnectionCountRateString).value();
      routerConnectionCountRateSum += serverMetric.getMetric(routerConnectionCountRateString).value();
      assertEquals(serverMetric.getMetric(readQuotaRejectedQPSString).value(), 0d);
      assertEquals(serverMetric.getMetric(readQuotaRejectedKPSString).value(), 0d);
      assertEquals(serverMetric.getMetric(readQuotaAllowedUnintentionally).value(), 0d);
      assertTrue(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining).value() > 0d);
    }
    assertTrue(quotaRequestedQPSSum >= 0, "Quota request sum: " + quotaRequestedQPSSum);
    assertTrue(quotaRequestedKPSSum >= 0, "Quota request key count sum: " + quotaRequestedKPSSum);
    assertTrue(clientConnectionCountRateSum > 0, "Servers should have more than 0 client connections");
    assertEquals(routerConnectionCountRateSum, 0, "Servers should have 0 router connections");
    // At least one server's usage ratio should eventually be a positive decimal
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      double usageRatio = 0;
      for (MetricsRepository serverMetric: serverMetrics) {
        usageRatio = serverMetric.getMetric(readQuotaUsageRatio).value();
        if (usageRatio > 0) {
          break;
        }
      }
      assertTrue(usageRatio > 0, "Quota usage ratio: " + usageRatio);
    });

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
    boolean errorRequest = false;

    for (MetricsRepository serverMetric: serverMetrics) {
      if (serverMetric.getMetric(readQuotaRejectedQPSString).value() > 0
          && serverMetric.getMetric(readQuotaRejectedKPSString).value() > 0) {
        readQuotaRejected = true;
      }
      if (serverMetric.getMetric(errorRequestString).value() > 0) {
        errorRequest = true;
      }
    }
    assertFalse(errorRequest);
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
    quotaRequestedQPSSum = 0;
    for (MetricsRepository serverMetric: serverMetrics) {
      quotaRequestedQPSSum += serverMetric.getMetric(readQuotaRequestedQPSString).value();
      assertEquals(serverMetric.getMetric(readQuotaAllowedUnintentionally).value(), 0d);
      assertTrue(serverMetric.getMetric(readQuotaStorageNodeTokenBucketRemaining).value() > 0d);
    }
    assertTrue(quotaRequestedQPSSum >= 0, "Quota request sum: " + quotaRequestedQPSSum);
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

    // It takes time for metadata to be propagated for fast client. To make the test deterministic we can create a new
    // client.

    genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);

    try {
      genericFastClient.compute()
          .project(VALUE_FIELD_NAME)
          .execute(Collections.singleton(key))
          .get(TIME_OUT, TimeUnit.MILLISECONDS);
      fail("The compute request should have thrown an exception.");
    } catch (Exception clientException) {
      if (!ExceptionUtils.recursiveMessageContains(clientException, "Read compute is not enabled for the store")) {
        fail("The exception message did not contain the expected string.", clientException);
      }
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
            .setSpeculativeQueryEnabled(false);
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

  @Test(timeOut = TIME_OUT)
  public void testStreamingBatchGetWithRetryAndQuotaRejection() throws Exception {
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils
          .assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(10)));
    });
    ArrayList<MetricsRepository> serverMetrics = new ArrayList<>();
    for (int i = 0; i < veniceCluster.getVeniceServers().size(); i++) {
      serverMetrics.add(veniceCluster.getVeniceServers().get(i).getMetricsRepository());
    }

    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setLongTailRetryEnabledForBatchGet(true)
            .setLongTailRetryThresholdForBatchGetInMicroSeconds(10000)
            .setSpeculativeQueryEnabled(false);
    MetricsRepository clientMetric = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, clientMetric, StoreMetadataFetchMode.SERVER_BASED_METADATA);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      keys.add(key);
    }
    try {
      for (int i = 0; i < 10; i++) {
        genericFastClient.batchGet(keys).get();
      }
      fail();
    } catch (ExecutionException exception) {
      assertTrue(exception.getCause() instanceof VeniceClientException);
      assertTrue(exception.getCause().getCause() instanceof VeniceClientRateExceededException);
      String readQuotaRejectedKPSString = "." + storeName + "--quota_rejected_key_count.Rate";
      String metricPrefix = "." + storeName + "--multiget_streaming_";
      int quotaRejectedKPSSum = 0;
      for (MetricsRepository serverMetric: serverMetrics) {
        quotaRejectedKPSSum += serverMetric.getMetric(readQuotaRejectedKPSString).value();
      }
      // Make sure retry is not triggered after encountering 429
      assertTrue(quotaRejectedKPSSum <= recordCnt, "quota rejected key count: " + quotaRejectedKPSSum);
      assertTrue(clientMetric.getMetric(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() < 1);
    }
  }

  @Test(timeOut = TIME_OUT)
  public void testStreamingBatchGetServerStats() throws Exception {
    ArrayList<MetricsRepository> serverMetrics = new ArrayList<>();
    for (int i = 0; i < veniceCluster.getVeniceServers().size(); i++) {
      serverMetrics.add(veniceCluster.getVeniceServers().get(i).getMetricsRepository());
    }
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setLongTailRetryEnabledForBatchGet(true)
            .setLongTailRetryThresholdForBatchGetInMicroSeconds(10000)
            .setSpeculativeQueryEnabled(false);
    MetricsRepository clientMetric = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, clientMetric, StoreMetadataFetchMode.SERVER_BASED_METADATA);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      keys.add(key);
    }
    genericFastClient.batchGet(keys).get();
    /**
     * Both multi-get and streaming multi-get is emitted under the multi-get request type on server side. See {@link com.linkedin.venice.listener.ServerStatsContext}
     */
    String multiGetRequestKeyCountMetric =
        ".total--" + RequestType.MULTI_GET.getMetricPrefix() + "request_key_count.Rate";
    String multiGetSuccessRequestKeyCountMetric =
        ".total--" + RequestType.MULTI_GET.getMetricPrefix() + "success_request_key_count.Rate";
    boolean nonZeroRequestedKeyCount = false;
    boolean nonZeroSuccessRequestKeyCount = false;
    for (MetricsRepository serverMetric: serverMetrics) {
      if (serverMetric.getMetric(multiGetRequestKeyCountMetric).value() > 0) {
        nonZeroRequestedKeyCount = true;
      }
      if (serverMetric.getMetric(multiGetSuccessRequestKeyCountMetric).value() > 0) {
        nonZeroSuccessRequestKeyCount = true;
      }
    }
    assertTrue(nonZeroRequestedKeyCount);
    assertTrue(nonZeroSuccessRequestKeyCount);
  }

  @Test(timeOut = TIME_OUT)
  public void testLongTailRetryManagerStats() throws IOException, ExecutionException, InterruptedException {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setLongTailRetryEnabledForBatchGet(true)
            .setLongTailRetryThresholdForBatchGetInMicroSeconds(10000)
            .setLongTailRetryBudgetEnforcementWindowInMs(1000)
            .setSpeculativeQueryEnabled(false);
    String multiKeyLongTailRetryManagerStatsPrefix = ".multi-key-long-tail-retry-manager-" + storeName + "--";
    String singleKeyLongTailRetryManagerStatsPrefix = ".single-key-long-tail-retry-manager-" + storeName + "--";
    MetricsRepository clientMetric = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, clientMetric, StoreMetadataFetchMode.SERVER_BASED_METADATA);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      keys.add(key);
    }
    for (int i = 0; i < 10; i++) {
      genericFastClient.batchGet(keys).get();
    }
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertTrue(
            clientMetric.getMetric(multiKeyLongTailRetryManagerStatsPrefix + "retry_limit_per_seconds.Gauge")
                .value() > 0,
            "Current value: "
                + clientMetric.getMetric(multiKeyLongTailRetryManagerStatsPrefix + "retry_limit_per_seconds.Gauge")
                    .value()));
    assertTrue(clientMetric.getMetric(multiKeyLongTailRetryManagerStatsPrefix + "retries_remaining.Gauge").value() > 0);
    assertEquals(
        clientMetric.getMetric(multiKeyLongTailRetryManagerStatsPrefix + "rejected_retry.OccurrenceRate").value(),
        0d);
    // single get long tail retry manager metrics shouldn't be initialized because it's not enabled
    assertNull(clientMetric.getMetric(singleKeyLongTailRetryManagerStatsPrefix + "retry_limit_per_seconds.Gauge"));
    assertNull(clientMetric.getMetric(singleKeyLongTailRetryManagerStatsPrefix + "retries_remaining.Gauge"));
    assertNull(clientMetric.getMetric(singleKeyLongTailRetryManagerStatsPrefix + "rejected_retry.OccurrenceRate"));
  }

  @Test(timeOut = TIME_OUT)
  public void testSNQuotaNotEnabled() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false);
    // Update store to disable storage node read quota
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(false)));
    });
    Assert.assertThrows(
        ConfigurationException.class,
        () -> getGenericFastClient(
            clientConfigBuilder,
            new MetricsRepository(),
            StoreMetadataFetchMode.SERVER_BASED_METADATA));
  }

  @Test(timeOut = TIME_OUT)
  public void testMultiKeyFanoutStats() throws IOException, ExecutionException, InterruptedException {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setLongTailRetryEnabledForBatchGet(true)
            .setSpeculativeQueryEnabled(false);
    MetricsRepository clientMetric = new MetricsRepository();
    String metricPrefix = ClientTestUtils.getMetricPrefix(storeName, RequestType.MULTI_GET_STREAMING);
    ;
    String fanoutSizeAverageMetricName = metricPrefix + "fanout_size.Avg";
    String fanoutSizeMaxMetricName = metricPrefix + "fanout_size.Max";
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, clientMetric, StoreMetadataFetchMode.SERVER_BASED_METADATA);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      keys.add(key);
    }
    for (int i = 0; i < 10; i++) {
      final Map<String, GenericRecord> result = genericFastClient.batchGet(keys).get();
      assertEquals(result.size(), keys.size());
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertNotNull(clientMetric.getMetric(fanoutSizeAverageMetricName));
      assertNotNull(clientMetric.getMetric(fanoutSizeMaxMetricName));
      assertTrue(clientMetric.getMetric(fanoutSizeAverageMetricName).value() >= 1.0);
      assertTrue(clientMetric.getMetric(fanoutSizeMaxMetricName).value() >= 1.0);
    });
  }
}
