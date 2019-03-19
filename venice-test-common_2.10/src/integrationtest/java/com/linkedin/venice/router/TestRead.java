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
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.httpclient.StorageNodeClientType;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroSerializer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.meta.PersistenceType.*;


//TODO: merge TestRead and TestRouterCache.
@Test(singleThreaded = true)
public abstract class TestRead {
  private static final int MAX_KEY_LIMIT = 10;
  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object> veniceWriter;

  protected abstract StorageNodeClientType getStorageNodeClientType();

  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
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
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 0, 2, 100, true, false);
    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, MAX_KEY_LIMIT); // 10 keys at most in a batch-get request
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    // set config for whether use Netty client in Router or not
    routerProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, getStorageNodeClientType());
    veniceCluster.addVeniceRouter(routerProperties);
    routerAddr = veniceCluster.getRandomRouterSslURL();

    // By default, the storage engine is BDB, and we would like test ROCKS_DB here as well.
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.PERSISTENCE_TYPE, ROCKS_DB);
    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    veniceCluster.addVeniceServer(serverFeatureProperties, serverProperties);

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    updateStore(0, MAX_KEY_LIMIT);

    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    keySerializer = new VeniceAvroSerializer(stringSchema);
    valueSerializer = new VeniceAvroSerializer(stringSchema);

    veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress()).getVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  private void updateStore(long readQuota, int maxKeyLimit) {
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
            .setReadQuotaInCU(readQuota)
            .setSingleGetRouterCacheEnabled(true)
            .setBatchGetLimit(maxKeyLimit));
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
    IOUtils.closeQuietly(veniceWriter);
  }

  @Test(timeOut = 50000)
  public void testRead() throws Exception {
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.getAndStartGenericAvroClient(
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
      Map<String, CharSequence> result = storeClient.batchGet(keySet).get();
      Assert.assertEquals(result.size(), MAX_KEY_LIMIT - 1);
      for (int i = 0; i < MAX_KEY_LIMIT - 1; ++i) {
        Assert.assertEquals(result.get(keyPrefix + i).toString(), valuePrefix + i);
      }
      /**
       * Test simple get
       */
      String key = keyPrefix + 2;
      String expectedValue = valuePrefix + 2;
      CharSequence value = storeClient.get(key).get();
      Assert.assertEquals(value.toString(), expectedValue);
    }

    // Check retry requests
    double singleGetRetries = 0;
    double batchGetRetries = 0;
    for (VeniceRouterWrapper veniceRouterWrapper : veniceCluster.getVeniceRouters()) {
      MetricsRepository metricsRepository = veniceRouterWrapper.getMetricsRepository();
      Map<String, ? extends Metric> metrics = metricsRepository.metrics();
      if (metrics.containsKey(".total--retry_count.LambdaStat")) {
        singleGetRetries += metrics.get(".total--retry_count.LambdaStat").value();
      }
      if (metrics.containsKey(".total--multiget_retry_count.LambdaStat")) {
        batchGetRetries += metrics.get(".total--multiget_retry_count.LambdaStat").value();
      }
    }
    Assert.assertTrue(singleGetRetries > 0, "After " + rounds + " reads, there should be some single-get retry requests");
    Assert.assertTrue(batchGetRetries > 0, "After " + rounds + " reads, there should be some batch-get retry requests");
    // Check Router connection pool metrics
    double totalMaxConnectionCount = 0;
    double connectionLeaseRequestLatencyMax = 0;
    double totalActiveConnectionCount = 0;
    double totalIdleConnectionCount = 0;
    double totalPendingConnectionRequestCount = 0;
    double totalLocalhostActiveConnectionCount = 0;
    double totalLocalhostIdleConnectionCount = 0;
    double totalLocalhostPendingConnectionCount = 0;
    double totalLocalhostMaxConnectionCount = 0;

    double totalRequestUsage = 0;

    double localhostResponseWaitingTimeForSingleGet = 0;
    double localhostResponseWaitingTimeForMultiGet = 0;
    double localhostRequestCount = 0;
    double localhostRequestCountForMultiGet = 0;

    double totalReadQuotaUsage = 0;

    for (VeniceRouterWrapper veniceRouterWrapper : veniceCluster.getVeniceRouters()) {
      MetricsRepository metricsRepository = veniceRouterWrapper.getMetricsRepository();
      Map<String, ? extends Metric> metrics = metricsRepository.metrics();
      if (metrics.containsKey(".connection_pool--total_max_connection_count.LambdaStat")) {
        totalMaxConnectionCount += metrics.get(".connection_pool--total_max_connection_count.LambdaStat").value();
      }
      if (metrics.containsKey(".connection_pool--connection_lease_request_latency.Max")) {
        double routerConnectionLeaseRequestLatencyMax = metrics.get(".connection_pool--connection_lease_request_latency.Max").value();
        connectionLeaseRequestLatencyMax = Math.max(connectionLeaseRequestLatencyMax, routerConnectionLeaseRequestLatencyMax);
      }
      if (metrics.containsKey(".connection_pool--total_active_connection_count.LambdaStat")) {
        totalActiveConnectionCount += metrics.get(".connection_pool--total_active_connection_count.LambdaStat").value();
      }
      if (metrics.containsKey(".connection_pool--total_pending_connection_request_count.LambdaStat")) {
        totalPendingConnectionRequestCount += metrics.get(".connection_pool--total_pending_connection_request_count.LambdaStat").value();
      }
      if (metrics.containsKey(".connection_pool--total_idle_connection_count.LambdaStat")) {
        totalIdleConnectionCount += metrics.get(".connection_pool--total_idle_connection_count.LambdaStat").value();
      }
      if (metrics.containsKey(".localhost--active_connection_count.Gauge")) {
        totalLocalhostActiveConnectionCount += metrics.get(".localhost--active_connection_count.Gauge").value();
      }
      if (metrics.containsKey(".localhost--idle_connection_count.Gauge")) {
        totalLocalhostIdleConnectionCount += metrics.get(".localhost--idle_connection_count.Gauge").value();
      }
      if (metrics.containsKey(".localhost--pending_connection_request_count.Gauge")) {
        totalLocalhostPendingConnectionCount += metrics.get(".localhost--pending_connection_request_count.Gauge").value();
      }
      if (metrics.containsKey(".localhost--max_connection_count.Gauge")) {
        totalLocalhostMaxConnectionCount += metrics.get(".localhost--max_connection_count.Gauge").value();
      }
      if (metrics.containsKey(".localhost--response_waiting_time.50thPercentile")) {
        localhostResponseWaitingTimeForSingleGet = metrics.get(".localhost--response_waiting_time.50thPercentile").value();
      }
      if (metrics.containsKey(".localhost--multiget_response_waiting_time.50thPercentile")) {
        localhostResponseWaitingTimeForMultiGet = metrics.get(".localhost--multiget_response_waiting_time.50thPercentile").value();
      }
      if (metrics.containsKey(".localhost--request.Count")) {
        localhostRequestCount += metrics.get(".localhost--request.Count").value();
      }
      if (metrics.containsKey(".localhost--multiget_request.Count")) {
        localhostRequestCountForMultiGet += metrics.get(".localhost--multiget_request.Count").value();
      }
      if (metrics.containsKey(".total--request_usage.Total")) {
        totalRequestUsage += metrics.get(".total--request_usage.Total").value();
      }
      if (metrics.containsKey(".total--read_quota_usage_kps.Total")) {
        totalReadQuotaUsage += metrics.get(".total--read_quota_usage_kps.Total").value();
      }
    }

    if (getStorageNodeClientType() == StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT) {
      // TODO: add connection pool stats for netty client
      Assert.assertTrue(totalMaxConnectionCount > 0, "Max connection count must be positive");
      Assert.assertTrue(connectionLeaseRequestLatencyMax > 0, "Connection lease max latency should be positive");
      Assert.assertTrue(totalActiveConnectionCount == 0, "Active connection count should be 0 since test queries are finished");
      Assert.assertTrue(totalPendingConnectionRequestCount == 0, "Pending connection request count should be 0 since test queries are finished");
      Assert.assertTrue(totalIdleConnectionCount > 0, "There should be some idle connections since test queries are finished");

      Assert.assertTrue(totalLocalhostMaxConnectionCount > 0, "Max connection count must be positive");
      Assert.assertTrue(totalLocalhostActiveConnectionCount == 0, "Active connection count should be 0 since test queries are finished");
      Assert.assertTrue(totalLocalhostPendingConnectionCount == 0, "Pending connection request count should be 0 since test queries are finished");
      Assert.assertTrue(totalLocalhostIdleConnectionCount > 0, "There should be some idle connections since test queries are finished");
    }

    Assert.assertTrue(localhostResponseWaitingTimeForSingleGet > 0);
    Assert.assertTrue(localhostResponseWaitingTimeForMultiGet > 0);

    Assert.assertTrue(localhostRequestCount > 0);
    Assert.assertTrue(localhostRequestCountForMultiGet > 0);

    Assert.assertEquals(totalRequestUsage, rounds * (MAX_KEY_LIMIT + 1.0), 0.0001);
    Assert.assertEquals(totalReadQuotaUsage, rounds * (MAX_KEY_LIMIT + 1.0), 0.0001);

    // Verify storage node metrics
    double maxMultiGetRequestPartCount = Double.MIN_VALUE;
    for (VeniceServerWrapper veniceServerWrapper : veniceCluster.getVeniceServers()) {
      Map<String, ? extends Metric> metrics = veniceServerWrapper.getMetricsRepository().metrics();
      if (metrics.containsKey(".total--multiget_request_part_count.Max")) {
        maxMultiGetRequestPartCount = Math.max(maxMultiGetRequestPartCount, metrics.get(".total--multiget_request_part_count.Max").value());
      }
      metrics.forEach( (mName, metric) -> {
        if (mName.contains("_current--disk_usage_in_bytes")) {
          Assert.assertTrue(metric.value() > 0.0, "Disk usage for current version should be postive");
        }
      });
    }
    Assert.assertEquals(maxMultiGetRequestPartCount, 1.0);

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

    //check client can receive quota exceeding message if it is exceeded.
    updateStore(1l, MAX_KEY_LIMIT);
    try {
      for (int i = 0; i < 100; i++) {
        storeClient.get(keyPrefix + i).get();
      }
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceClientHttpException);
      Assert.assertTrue(e.getCause().getMessage().contains("Quota exceeds!"));
    }
  }

  @Test
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
}
