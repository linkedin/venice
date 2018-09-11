package com.linkedin.venice.router;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestRouterCache {
  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

  private String zkAddress;

  //We're instantiate Venice cluster before the method since MetricsRepository can't reset.
  //TODO: optimize it if any chances
  @BeforeMethod
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 3, 0, 2, 100, true, false);
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());

    // Need to start 3 routers with cache enabled
    int routerNum = 3;
    String[] routerUrls = new String[routerNum];
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_CACHE_ENABLED, "true");
    // avoid long tail retry
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 60000); // 60 seconds
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:3600000"); // 1 hour
    for (int i = 0; i < routerNum; ++i) {
      VeniceRouterWrapper router = veniceCluster.addVeniceRouter(routerProperties);
      routerUrls[i] = "http://" + router.getAddress();
    }

    // Announce all the routers to D2
    zkAddress = veniceCluster.getZk().getAddress();
    // Enable sticky routing on client side
    D2TestUtils.setupD2Config(zkAddress, false, D2TestUtils.DEFAULT_TEST_CLUSTER_NAME,
        D2TestUtils.DEFAULT_TEST_SERVICE_NAME, true);
    List<D2Server> d2Servers = D2TestUtils.getD2Servers(zkAddress, routerUrls);
    d2Servers.forEach(d2Server -> d2Server.forceStart());
  }

  private VersionCreationResponse createStore(boolean isCompressed, boolean isRouterCacheEnabled, boolean isBatchGetRouterCacheEnabled) {
    VersionCreationResponse response = veniceCluster.getNewStoreVersion();
    // Update default quota and enable router cache
    controllerClient.updateStore(response.getName(), new UpdateStoreQueryParams()
            .setReadQuotaInCU(10000l)
            .setCompressionStrategy(isCompressed ? CompressionStrategy.GZIP : CompressionStrategy.NO_OP)
            .setSingleGetRouterCacheEnabled(isRouterCacheEnabled)
            .setBatchGetRouterCacheEnabled(isBatchGetRouterCacheEnabled));

    return response;
  }

  @AfterMethod
  public void cleanUp() {
    IOUtils.closeQuietly(controllerClient);
    IOUtils.closeQuietly(veniceCluster);
  }

  @DataProvider(name = "isCompressed_isCacheEnabled_isBatchGetCacheEnabled")
  public static Object[][] pushStatues() {
    return new Object[][]{{false, true, false}, {false, false, true}, {true, true, false}, {true, false, true}};
  }

  @Test(timeOut = 20000, dataProvider = "isCompressed_isCacheEnabled_isBatchGetCacheEnabled")
  public void testRead(boolean isCompressed, boolean isRouterCacheEnabled, boolean isBatchGetRouterCacheEnabled) throws Exception {
    // Create test store
    VersionCreationResponse creationResponse = createStore(isCompressed, isRouterCacheEnabled, isBatchGetRouterCacheEnabled);
    String topic = creationResponse.getKafkaTopic();
    String storeName = creationResponse.getName();
    int pushVersion = creationResponse.getVersion();
    int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    VeniceAvroGenericSerializer keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    DefaultSerializer valueSerializer = new DefaultSerializer();
    //since compressor only takes bytes. Manually serialize the value to bytes first
    VeniceAvroGenericSerializer avroSerializer = new VeniceAvroGenericSerializer(stringSchema);

    CompressionStrategy compressionStrategy = isCompressed ? CompressionStrategy.GZIP : CompressionStrategy.NO_OP;
    VeniceCompressor compressor = CompressorFactory.getCompressor(compressionStrategy);

    VeniceWriter<Object, byte[]> veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress())
        .getVeniceWriter(topic, keySerializer, valueSerializer);



    String keyPrefix = "key_";
    String valuePrefix = "value_";

    veniceWriter.broadcastStartOfPush(true, false, compressionStrategy, new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 100; ++i) {
      veniceWriter.put(keyPrefix + i, compressor.compress(avroSerializer.serialize(topic,valuePrefix + i)), valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });

    IOUtils.closeQuietly(veniceWriter);

    // Test with D2 Client
    D2Client d2Client = D2TestUtils.getAndStartD2Client(zkAddress);
    AvroGenericStoreClient<String, CharSequence> storeClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME).setD2Client(d2Client));
    // Run multiple rounds
    int rounds = 50;
    int cur = 0;
    String existingKey = keyPrefix + 2;
    String expectedValue = valuePrefix + 2;
    String nonExistingKey = keyPrefix + 100;
    while (++cur <= rounds) {
      /**
       * Batch get should not get impacted by caching feature.
       */
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      Map<String, CharSequence> result = storeClient.batchGet(keySet).get();
      Assert.assertEquals(result.size(), 10);
      for (int i = 0; i < 10; ++i) {
        Assert.assertEquals(result.get(keyPrefix + i).toString(), valuePrefix + i);
      }
      /**
       * Test existing key
       */
      CharSequence value = storeClient.get(existingKey).get();
      Assert.assertEquals(value.toString(), expectedValue);
      /**
       * Test non-existing key
       */
      Assert.assertNull(storeClient.get(nonExistingKey).get());
    }

    double totalCacheLookupRequest = 0;
    double totalCacheHitRequest = 0;
    double totalCachePutRequest = 0;
    double totalBatchGetCacheHitRequest = 0;
    for (VeniceRouterWrapper veniceRouterWrapper : veniceCluster.getVeniceRouters()) {
      MetricsRepository metricsRepository = veniceRouterWrapper.getMetricsRepository();
      Map<String, ? extends Metric> metrics = metricsRepository.metrics();

      if (metrics.containsKey(".total--cache_lookup_request.Count")) {
        totalCacheLookupRequest += metrics.get(".total--cache_lookup_request.Count").value();
      }
      if (metrics.containsKey(".total--cache_hit_request.Count")) {
        totalCacheHitRequest += metrics.get(".total--cache_hit_request.Count").value();
      }
      if (metrics.containsKey(".total--cache_put_request.Count")) {
        totalCachePutRequest += metrics.get(".total--cache_put_request.Count").value();
      }
      if (metrics.containsKey(".total--multiget_cache_hit_request.Count")) {
        totalBatchGetCacheHitRequest += metrics.get(".total--multiget_cache_hit_request.Count").value();
      }
    }

    if (isRouterCacheEnabled) {
      Assert.assertEquals(totalCacheLookupRequest, 100.0);
      Assert.assertEquals(totalCacheHitRequest, 98.0);
      Assert.assertEquals(totalCachePutRequest, 2.0);
    } else if (isBatchGetRouterCacheEnabled) {
      Assert.assertEquals(totalBatchGetCacheHitRequest, 470.0);
    }
  }
}
