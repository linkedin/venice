package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.Metric;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.davinci.config.BlockingQueueType.*;
import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.*;
import static com.linkedin.venice.router.api.VenicePathParser.*;

@Test(singleThreaded = true)
public abstract class TestRead {
  private static final int MAX_KEY_LIMIT = 20;
  private static final Logger logger = Logger.getLogger(TestRead.class);
  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;

  private static final String KEY_SCHEMA_STR = "\"string\"";
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String VALUE_SCHEMA_STR = "{\n" + "\"type\": \"record\",\n" + "\"name\": \"test_value_schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\"}]\n" + "}";
  private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(VALUE_SCHEMA_STR);

  protected abstract StorageNodeClientType getStorageNodeClientType();

  protected boolean isHttp2Enabled() { return false;}

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    /**
     * The following config is used to detect Netty resource leaking.
     * If memory leak happens, you will see the following log message:
     *
     *  ERROR io.netty.util.ResourceLeakDetector - LEAK: ByteBuf.release() was not called before it's garbage-collected.
     *  See http://netty.io/wiki/reference-counted-objects.html for more information.
     **/

    System.setProperty("io.netty.leakDetection.maxRecords", "50");
    System.setProperty("io.netty.leakDetection.level", "paranoid");

    Utils.thisIsLocalhost();
    boolean isR2Client = getStorageNodeClientType() == StorageNodeClientType.R2_CLIENT;
    veniceCluster = ServiceFactory.getVeniceCluster(1, isR2Client ? 0: 1, 0, 2, 100, true, false);

    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, MAX_KEY_LIMIT); // 10 keys at most in a batch-get request
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    routerProperties.put(ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, false);
    // set config for whether use Netty client in Router or not
    routerProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, getStorageNodeClientType());
    routerProperties.put(ConfigKeys.ROUTER_HTTP2_R2_CLIENT_ENABLED, isHttp2Enabled());
    routerProperties.put(ConfigKeys.ROUTER_PER_NODE_CLIENT_ENABLED, true);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, !isR2Client);
    routerProperties.put(ConfigKeys.ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_MULTI_KEY_ROUTING_STRATEGY, HELIX_ASSISTED_ROUTING.name());
    routerProperties.put(ConfigKeys.ROUTER_HELIX_VIRTUAL_GROUP_FIELD_IN_DOMAIN, "zone");
    veniceCluster.addVeniceRouter(routerProperties);
    routerAddr = veniceCluster.getRandomRouterSslURL();

    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET, true); // test parallel lookup
    serverProperties.put(ConfigKeys.SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY, 1); // test bounded queue
    serverProperties.put(ConfigKeys.SERVER_COMPUTE_QUEUE_CAPACITY, 1);
    serverProperties.put(ConfigKeys.SERVER_BLOCKING_QUEUE_TYPE, ARRAY_BLOCKING_QUEUE.name());
    serverProperties.put(ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE, 3);
    serverProperties.put(ConfigKeys.SERVER_REST_SERVICE_EPOLL_ENABLED, true);
    serverProperties.put(ConfigKeys.SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP, "");
    serverProperties.put(ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED, true); // Enable Http/2 support

    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster.addVeniceServer(serverFeatureProperties, serverProperties);
    if (isR2Client) {
      veniceCluster.addVeniceServer(serverFeatureProperties, serverProperties);
    }
    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(KEY_SCHEMA_STR, VALUE_SCHEMA_STR);
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    updateStore(0, MAX_KEY_LIMIT);

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress()).createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }


  private void updateStore(long readQuota, int maxKeyLimit) {
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
            .setReadQuotaInCU(readQuota)
            .setReadComputationEnabled(true)
            .setBatchGetLimit(maxKeyLimit));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
    IOUtils.closeQuietly(veniceWriter);
  }

  @Test(timeOut = 50000, groups = {"flaky"})
  public void testRead() throws Exception {
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      veniceWriter.put(keyPrefix + i, record, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    double maxInflightRequestCount = getAggregateRouterMetricValue(".total--in_flight_request_count.Max");
    Assert.assertEquals(maxInflightRequestCount, 0.0, "There should be no in-flight requests yet!");

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, GenericRecord> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddr)
            .setSslEngineComponentFactory(SslUtils.getLocalSslFactory())
    );
    // Run multiple rounds
    int rounds = 100;
    int cur = 0;
    while (++cur <= rounds) {
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      Map<String, GenericRecord> result = storeClient.batchGet(keySet).get();
      Assert.assertEquals(result.size(), MAX_KEY_LIMIT - 1);
      Map<String, GenericRecord> computeResult = storeClient.compute().project(VALUE_FIELD_NAME).execute(keySet).get();
      Assert.assertEquals(computeResult.size(), MAX_KEY_LIMIT - 1);

      for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
        GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
        record.put(VALUE_FIELD_NAME, i);
        Assert.assertEquals(result.get(keyPrefix + i), record);
        Assert.assertEquals(computeResult.get(keyPrefix + i).get(VALUE_FIELD_NAME), i);
      }

      /**
       * Test simple get
       */
      String key = keyPrefix + 2;
      GenericRecord expectedValue = new GenericData.Record(VALUE_SCHEMA);
      expectedValue.put(VALUE_FIELD_NAME, 2);
      GenericRecord value = storeClient.get(key).get();
      Assert.assertEquals(value, expectedValue);
    }

    double maxInflightRequestCountAfterQueries = getAggregateRouterMetricValue(".total--in_flight_request_count.Max");
    Assert.assertTrue(maxInflightRequestCountAfterQueries > 0.0, "There should be in-flight requests now!");

    // Check retry requests
    Assert.assertTrue(getAggregateRouterMetricValue(".total--retry_count.LambdaStat") > 0,
        "After " + rounds + " reads, there should be some single-get retry requests");
    Assert.assertTrue(getAggregateRouterMetricValue(".total--multiget_retry_count.LambdaStat") > 0,
        "After " + rounds + " reads, there should be some batch-get retry requests");

    // Check Router connection pool metrics
    if (getStorageNodeClientType() == StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT) {
      // TODO: add connection pool stats for netty client
      Assert.assertTrue(getAggregateRouterMetricValue(".connection_pool--total_max_connection_count.LambdaStat") > 0,
          "Max connection count must be positive");
      Assert.assertTrue(getMaxRouterMetricValue(".connection_pool--connection_lease_request_latency.Max") > 0,
          "Connection lease max latency should be positive");
      Assert.assertTrue(getAggregateRouterMetricValue(".connection_pool--total_active_connection_count.LambdaStat") == 0,
          "Active connection count should be 0 since test queries are finished");
      Assert.assertTrue(getAggregateRouterMetricValue(".connection_pool--total_pending_connection_request_count.LambdaStat") == 0,
          "Pending connection request count should be 0 since test queries are finished");
      Assert.assertTrue(getAggregateRouterMetricValue(".connection_pool--total_idle_connection_count.LambdaStat") > 0,
          "There should be some idle connections since test queries are finished");

      Assert.assertTrue(getAggregateRouterMetricValue(".localhost--max_connection_count.Gauge") > 0,
          "Max connection count must be positive");
      Assert.assertTrue(getAggregateRouterMetricValue(".localhost--active_connection_count.Gauge") == 0,
          "Active connection count should be 0 since test queries are finished");
      Assert.assertTrue(getAggregateRouterMetricValue(".localhost--pending_connection_request_count.Gauge") == 0,
          "Pending connection request count should be 0 since test queries are finished");
      Assert.assertTrue(getAggregateRouterMetricValue(".localhost--idle_connection_count.Gauge") > 0,
          "There should be some idle connections since test queries are finished");
    }

    Assert.assertTrue(getAggregateRouterMetricValue(".localhost--response_waiting_time.50thPercentile") > 0);
    Assert.assertTrue(getAggregateRouterMetricValue(".localhost--multiget_response_waiting_time.50thPercentile") > 0);

    Assert.assertTrue(getAggregateRouterMetricValue(".localhost--request.Count") > 0);
    Assert.assertTrue(getAggregateRouterMetricValue(".localhost--multiget_request.Count") > 0);

    // Each round:
    // 1. We do MAX_KEY_LIMIT * 2 because we do a batch get and a batch compute
    // 2. And then + 1 because we also do a single get
    double expectedLookupCount = rounds * (MAX_KEY_LIMIT * 2 + 1.0);
    Assert.assertEquals(getAggregateRouterMetricValue(".total--request_usage.Total"), expectedLookupCount, 0.0001);
    Assert.assertEquals(getAggregateRouterMetricValue(".total--read_quota_usage_kps.Total"), expectedLookupCount, 0.0001);

    // following 2 asserts fails with HTTP/2 probably due to http2 frames, needs to validate on venice-p
    if (!isHttp2Enabled()) {
      Assert.assertEquals(getMaxServerMetricValue(".total--multiget_request_part_count.Max"), 1.0);
      Assert.assertEquals(getMaxServerMetricValue(".total--compute_request_part_count.Max"), 1.0);
    }
    // Verify storage node metrics
    Assert.assertTrue(getMaxServerMetricValue(".total--multiget_request_size_in_bytes.Max") > 0.0);
    Assert.assertTrue(getMaxServerMetricValue(".total--compute_request_size_in_bytes.Max") > 0.0);
    for (VeniceServerWrapper veniceServerWrapper : veniceCluster.getVeniceServers()) {
      Map<String, ? extends Metric> metrics = veniceServerWrapper.getMetricsRepository().metrics();
      metrics.forEach( (mName, metric) -> {
        if (mName.startsWith(String.format(".%s_current--disk_usage_in_bytes.", storeName))) {
          double value = metric.value();
          Assert.assertNotEquals(value, (double) StatsErrorCode.NULL_BDB_ENVIRONMENT.code, "Got a NULL_BDB_ENVIRONMENT!");
          Assert.assertNotEquals(value, (double) StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code, "Got NULL_STORAGE_ENGINE_STATS!");
          Assert.assertTrue(value > 0.0, "Disk usage for current version should be positive. Got: " + value);
        }
      });
    }

    /**
     * Test batch get limit
     */
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < MAX_KEY_LIMIT + 1; ++i) {
      keySet.add(keyPrefix + i);
    }
    try {
      storeClient.batchGet(keySet).get();
      Assert.fail("Should receive exception since the batch request key count exceeds cluster-level threshold");
    } catch (Exception e) {
    }
    // Bump up store-level max key count in batch-get request
    updateStore(10000l, MAX_KEY_LIMIT + 1);

    // It will take some time to let Router receive the store update.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        storeClient.batchGet(keySet).get();
      } catch (Exception e) {
        Assert.fail("StoreClient should not throw exception since we have bumped up store-level batch-get key count limit");
      }
    });

    // Single get quota test

    int throttledRequestsForSingleGet = (int) getAggregateRouterMetricValue(".total--multiget_throttled_request.Count");
    Assert.assertEquals(throttledRequestsForSingleGet, 0, "The throttled_request metric should be at zero before the test.");

    double throttledRequestLatencyForSingleGet = getAggregateRouterMetricValue(".total--throttled_request_latency.Max");
    Assert.assertEquals(throttledRequestLatencyForSingleGet, 0.0, "There should be no single get throttled request latency yet!");

    updateStore(1l, MAX_KEY_LIMIT);
    int quotaExceptionsCount = 0;
    int numberOfRequests = 100;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numberOfRequests; i++) {
      try {
        storeClient.get(keyPrefix + i).get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        Assert.assertTrue(cause instanceof VeniceClientHttpException);
        Assert.assertEquals(((VeniceClientHttpException)cause).getHttpStatus(), HttpResponseStatus.TOO_MANY_REQUESTS.code());
        quotaExceptionsCount++;
      }
    }
    long runTimeMs = System.currentTimeMillis() - startTime;
    Assert.assertTrue(quotaExceptionsCount > 0,
        "There were no quota exceptions at all for single gets! "
            + "(Test too slow? " + runTimeMs + " ms for " + numberOfRequests + " requests)");

    int throttledRequestsForSingleGetAfterQueries = (int) getAggregateRouterMetricValue(".total--throttled_request.Count");
    Assert.assertEquals(throttledRequestsForSingleGetAfterQueries, quotaExceptionsCount,
        "The throttled_request metric is inconsistent with the number of quota exceptions received by the client!");

    double throttledRequestLatencyForSingleGetAfterQueries = getAggregateRouterMetricValue(".total--throttled_request_latency.Max");
    /** TODO Re-enable this assertion once we stop throwing batch get quota exceptions from {@link com.linkedin.venice.router.api.VeniceDelegateMode} */
    //Assert.assertTrue(throttledRequestLatencyForSingleGetAfterQueries > 0.0, "There should be single get throttled request latency now!");


    // Batch get quota test

    int throttledRequestsForBatchGet = (int) getAggregateRouterMetricValue(".total--multiget_throttled_request.Count");
    Assert.assertEquals(throttledRequestsForBatchGet, 0, "The throttled_request metric should be at zero before the test.");

    double throttledRequestLatencyForBatchGet = getAggregateRouterMetricValue(".total--multiget_throttled_request_latency.Max");
    Assert.assertEquals(throttledRequestLatencyForBatchGet, 0.0, "There should be no batch get throttled request latency yet!");

    keySet.clear();
    for (int i = 0; i < MAX_KEY_LIMIT; ++i) {
      keySet.add(keyPrefix + i);
    }
    int quotaExceptionsCountForSingleGetsOnly = quotaExceptionsCount;
    long startTimeForBatchGet = System.currentTimeMillis();
    int quotaExceptionsCountForBatchGet = 0;
    for (int i = 0; i < numberOfRequests; i++) {
      try {
        storeClient.batchGet(keySet).get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        Assert.assertTrue(cause instanceof VeniceClientHttpException);
        Assert.assertTrue(cause.getMessage().contains("Quota exceeded"), "Did not get the expected exception message: " + cause.getMessage());
        quotaExceptionsCountForBatchGet++;
      }
    }
    long runTimeForBatchGetMs = System.currentTimeMillis() - startTimeForBatchGet;
    Assert.assertTrue(quotaExceptionsCountForBatchGet > 0,
        "There were no quota exceptions at all for batch gets! "
            + "(Test too slow? " + runTimeForBatchGetMs + " ms for " + numberOfRequests + " requests)");

    int throttledRequestsForBatchGetAfterQueries = (int) getAggregateRouterMetricValue(".total--multiget_throttled_request.Count");
    Assert.assertEquals(throttledRequestsForBatchGetAfterQueries, quotaExceptionsCountForBatchGet,
        "The throttled_request metric is inconsistent with the number of quota exceptions received by the client! (quotaExceptionsCountForSingleGetsOnly = " + quotaExceptionsCountForSingleGetsOnly + ")");

    double throttledRequestLatencyForBatchGetAfterQueries = getAggregateRouterMetricValue(".total--multiget_throttled_request_latency.Max");
    /** TODO Re-enable this assertion once we stop throwing batch get quota exceptions from {@link com.linkedin.venice.router.api.VeniceDelegateMode} */
//    Assert.assertTrue(throttledRequestLatencyForBatchGetAfterQueries > 0.0, "There should be batch get throttled request latency now!");
  }

  private double getMaxServerMetricValue(String metricName) {
    return MetricsUtils.getMax(metricName, veniceCluster.getVeniceServers());
  }

  private double getMaxRouterMetricValue(String metricName) {
    return MetricsUtils.getMax(metricName, veniceCluster.getVeniceRouters());
  }

  private double getAggregateRouterMetricValue(String metricName) {
    return MetricsUtils.getSum(metricName, veniceCluster.getVeniceRouters());
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testD2ServiceDiscovery() {
    String routerUrl = veniceCluster.getRandomRouterURL();
    try (CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(2000).build())
        .build()) {
      client.start();
      HttpGet routerRequest =
          new HttpGet(routerUrl + "/" + VenicePathParser.TYPE_CLUSTER_DISCOVERY + "/" + storeName);
      HttpResponse response = client.execute(routerRequest, null).get();
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream);
      } catch (IOException e) {
        throw new VeniceException(e);
      }
      Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
          "Could not get d2 service correctly. Response:" + responseBody);

      ObjectMapper mapper = new ObjectMapper();
      D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse =
          mapper.readValue(responseBody.getBytes(), D2ServiceDiscoveryResponse.class);
      Assert.assertFalse(d2ServiceDiscoveryResponse.isError());
      Assert.assertEquals(d2ServiceDiscoveryResponse.getD2Service(),
          veniceCluster.getRandomVeniceRouter().getD2Service());
      Assert.assertEquals(d2ServiceDiscoveryResponse.getCluster(), veniceCluster.getClusterName());
      Assert.assertEquals(d2ServiceDiscoveryResponse.getName(), storeName);
    } catch (Exception e) {
      Assert.fail("Met an exception.", e);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testRouterHealthCheck() {
    String routerUrl = veniceCluster.getRandomRouterURL();
    try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {
      client.start();
      HttpOptions healthCheckRequest = new HttpOptions(routerUrl);
      HttpResponse response = client.execute(healthCheckRequest, null).get();
      Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
          "Router fails to respond to health check.");

      HttpGet healthCheckGetRequest = new HttpGet(routerUrl + "/" + TYPE_HEALTH_CHECK);
      HttpResponse getResponse = client.execute(healthCheckGetRequest, null).get();
      Assert.assertEquals(getResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
          "Router fails to respond to health check.");
    } catch (Exception e) {
      Assert.fail("Met an exception:", e);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testResourceStateLookup() {
    String routerURL = veniceCluster.getRandomRouterURL();
    try (CloseableHttpAsyncClient client = HttpAsyncClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(2000).build())
        .build()) {
      client.start();
      HttpGet routerRequest =
          new HttpGet(routerURL + "/" + TYPE_RESOURCE_STATE + "/" + storeVersionName);
      HttpResponse response = client.execute(routerRequest, null).get();
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream);
      }
      Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
          "Failed to get resource state for " + storeVersionName + ". Response: " + responseBody);
      ObjectMapper mapper = new ObjectMapper();
      ResourceStateResponse resourceStateResponse = mapper.readValue(responseBody.getBytes(), ResourceStateResponse.class);
      Assert.assertEquals(resourceStateResponse.getName(), storeVersionName);
      logger.info(responseBody);
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }
}
