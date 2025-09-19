package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.ROUTER_ENABLE_READ_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static org.testng.Assert.assertEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
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


@Test(singleThreaded = true)
public class TestRetryQuotaRejection {
  private static final int MAX_KEY_LIMIT = 20;
  private VeniceClusterWrapper veniceCluster;
  private D2Client d2Client;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String UNUSED_FIELD_NAME = "unused_field";
  private static final String VALUE_SCHEMA_STR =
      "{\"type\": \"record\",\"name\": \"test_value_schema\",\"fields\": [{\"name\": \"" + UNUSED_FIELD_NAME
          + "\", \"type\": \"int\"}, {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]}";
  private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);
  private static final String KEY_PREFIX = "key_";

  protected boolean isRouterHttp2Enabled() {
    return false;
  }

  protected VeniceClusterWrapper getVeniceCluster() {
    return veniceCluster;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException, ExecutionException, InterruptedException {
    Utils.thisIsLocalhost();
    Properties extraProperties = new Properties();
    extraProperties.put(ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, true);
    extraProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT);
    extraProperties.put(ROUTER_ENABLE_READ_THROTTLING, false);
    extraProperties.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");

    veniceCluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(1)
            .numberOfRouters(2)
            .replicationFactor(2)
            .partitionSize(100)
            .sslToStorageNodes(true)
            .extraProperties(extraProperties)
            .build());
    Properties serverProperties = new Properties();
    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster.addVeniceServer(serverFeatureProperties, serverProperties);

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    updateStore(10, MAX_KEY_LIMIT);
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    veniceWriter = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(storeVersionName).setKeyPayloadSerializer(keySerializer)
                .setValuePayloadSerializer(valueSerializer)
                .build());

    d2Client = D2TestUtils.getD2Client(
        veniceCluster.getZk().getAddress(),
        true,
        isRouterHttp2Enabled() ? HttpProtocolVersion.HTTP_2 : HttpProtocolVersion.HTTP_1_1);
    D2TestUtils.startD2Client(d2Client);
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    for (int i = 0; i < 100; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      record.put(UNUSED_FIELD_NAME, -i);
      veniceWriter.put(KEY_PREFIX + i, record, valueSchemaId).get();
    }
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    veniceCluster.useControllerClient(
        cc -> TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> assertEquals(cc.getStore(storeName).getStore().getCurrentVersion(), pushVersion)));

    // Force router refresh metadata to reflect config update.
    veniceCluster.refreshAllRouterMetaData();
  }

  private void updateStore(long readQuota, int maxKeyLimit) {
    veniceCluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setReadQuotaInCU(readQuota)
            .setBatchGetLimit(maxKeyLimit)
            .setStorageNodeReadQuotaEnabled(true));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    if (d2Client != null) {
      d2Client.shutdown(null);
    }
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testServerQuotaRejection() {
    MetricsRepository clientMetrics = new MetricsRepository();

    try (AvroGenericStoreClient<String, GenericRecord> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2Client(d2Client)
            .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setMetricsRepository(clientMetrics)
            .setProjectionFieldValidationEnabled(false))) {
      int rounds = 40;
      for (int cur = 0; cur < rounds; ++cur) {
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
          keySet.add(KEY_PREFIX + i);
        }
        try {
          storeClient.batchGet(keySet).get();
        } catch (Exception e) {
          Assert.assertTrue(e.getCause().getMessage().contains("http status: 429"));
        }
      }

      // Check retry requests
      Assert.assertTrue(
          getAggregateRouterMetricValue(".total--retry_count.LambdaStat") == 0,
          "There should not be any retry requests in routers");
      Assert.assertTrue(
          getAggregateServerMetricValue("." + storeName + "--quota_rejected_request.Rate") > 0,
          "There should be retry requests inside servers");
    }
  }

  private double getAggregateRouterMetricValue(String metricName) {
    return MetricsUtils.getSum(metricName, veniceCluster.getVeniceRouters());
  }

  private double getAggregateServerMetricValue(String metricName) {
    return MetricsUtils.getSum(metricName, veniceCluster.getVeniceServers());
  }
}
