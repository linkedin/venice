package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRouterRetry {
  private static final int MAX_KEY_LIMIT = 10;
  VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private String routerAddr;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String VALUE_SCHEMA_STR =
      "{\n" + "\"type\": \"record\",\n" + "\"name\": \"test_value_schema\",\n" + "\"fields\": [\n" + "  {\"name\": \""
          + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);
  private static final String KEY_PREFIX = "key_";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException, ExecutionException, InterruptedException {
    Utils.thisIsLocalhost();
    Properties extraProperties = new Properties();
    // Add the following specific configs for Router
    // To trigger long-tail retry
    extraProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    extraProperties.put(ConfigKeys.ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, MAX_KEY_LIMIT); // 10 keys at most in a
                                                                                         // batch-get request
    extraProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    extraProperties.put(ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, true);

    // For Controller
    extraProperties.put(
        ConfigKeys.DEFAULT_OFFLINE_PUSH_STRATEGY,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION.toString());

    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false, extraProperties);
    routerAddr = veniceCluster.getRandomRouterSslURL();

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    updateStore(0, MAX_KEY_LIMIT);

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress())
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(storeVersionName).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      veniceWriter.put(KEY_PREFIX + i, record, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });

    veniceCluster.refreshAllRouterMetaData();
    veniceCluster.stopVeniceServer(veniceCluster.getVeniceServers().get(0).getPort());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
  }

  private void updateStore(long readQuota, int maxKeyLimit) {
    controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setReadQuotaInCU(readQuota)
            .setReadComputationEnabled(true)
            .setBatchGetLimit(maxKeyLimit));
  }

  @Test(timeOut = 60000)
  public void testRouterRetry() throws ExecutionException, InterruptedException {
    try (AvroGenericStoreClient<String, GenericRecord> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddr)
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()))) {
      int rounds = 100;
      for (int cnt = 0; cnt < rounds; ++cnt) {
        // Send request to a read-disabled store
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
          keySet.add(KEY_PREFIX + i);
        }
        keySet.add("unknown_key");
        Map<String, GenericRecord> result = storeClient.batchGet(keySet).get();
        Assert.assertEquals(result.size(), MAX_KEY_LIMIT - 1);
        Map<String, ComputeGenericRecord> computeResult =
            storeClient.compute().project(VALUE_FIELD_NAME).execute(keySet).get();
        Assert.assertEquals(computeResult.size(), MAX_KEY_LIMIT - 1);

        for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
          GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
          record.put(VALUE_FIELD_NAME, i);
          Assert.assertEquals(result.get(KEY_PREFIX + i), record);
          Assert.assertEquals(computeResult.get(KEY_PREFIX + i).get(VALUE_FIELD_NAME), i);
        }

        /**
         * Test simple get
         */
        String key = KEY_PREFIX + 2;
        GenericRecord expectedValue = new GenericData.Record(VALUE_SCHEMA);
        expectedValue.put(VALUE_FIELD_NAME, 2);
        GenericRecord value = storeClient.get(key).get();
        Assert.assertEquals(value, expectedValue);

        // Test non-existing key
        value = storeClient.get("unknown_key").get();
        Assert.assertNull(value);
      }
    }

    // Verify retry metrics
    double noAvailableReplicaAbortedRetryRequestMetricForSingleGet = MetricsUtils
        .getSum(".total--no_available_replica_aborted_retry_request.Count", veniceCluster.getVeniceRouters());
    double noAvailableReplicaAbortedRetryRequestMetricForBatchGetStreaming = MetricsUtils.getSum(
        ".total--multiget_streaming_no_available_replica_aborted_retry_request.Count",
        veniceCluster.getVeniceRouters());
    double noAvailableReplicaAbortedRetryRequestMetricForComputeStreaming = MetricsUtils.getSum(
        ".total--compute_streaming_no_available_replica_aborted_retry_request.Count",
        veniceCluster.getVeniceRouters());
    Assert.assertTrue(
        noAvailableReplicaAbortedRetryRequestMetricForSingleGet > 0,
        "No available aborted retry request should be triggered for single-get");
    Assert.assertTrue(
        noAvailableReplicaAbortedRetryRequestMetricForBatchGetStreaming > 0,
        "No available aborted retry request should be triggered for batch-get streaming");
    Assert.assertTrue(
        noAvailableReplicaAbortedRetryRequestMetricForComputeStreaming > 0,
        "No available aborted retry request should be triggered for compute streaming");
    // No unhealthy request
    double unhealthyRequestMetricForSingleGet =
        MetricsUtils.getSum(".total--unhealthy_request.Count", veniceCluster.getVeniceRouters());
    double unhealthyRequestMetricForBatchGetStreaming =
        MetricsUtils.getSum(".total--multiget_streaming_unhealthy_request.Count", veniceCluster.getVeniceRouters());
    double unhealthyRequestMetricForForComputeStreaming =
        MetricsUtils.getSum(".total--compute_streaming_unhealthy_request.Count", veniceCluster.getVeniceRouters());
    Assert.assertEquals(unhealthyRequestMetricForSingleGet, 0.0, "Unhealthy request for single-get is unexpected");
    Assert.assertEquals(
        unhealthyRequestMetricForBatchGetStreaming,
        0.0,
        "Unhealthy request for batch-get streaming is unexpected");
    Assert.assertEquals(
        unhealthyRequestMetricForForComputeStreaming,
        0.0,
        "Unhealthy request for compute streaming is unexpected");
  }
}
