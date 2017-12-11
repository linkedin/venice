package com.linkedin.venice.router;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;

@Test(singleThreaded = true)
public class TestRouterCache {
  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;

  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object> veniceWriter;

  private String zkAddress;


  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(1, 3, 0, 2, 100, true, false);

    // Need to start 3 routers with cache enabled
    int routerNum = 3;
    String[] routerUrls = new String[routerNum];
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_CACHE_ENABLED, "true");
    // avoid long tail retry
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 60000); // 60 seconds
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

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // Update default quota and enable router cache
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs());
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.of(10000l), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.of(Boolean.TRUE), Optional.empty());

    VeniceProperties clientProps =
        new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, veniceCluster.getKafka().getAddress())
            .put(ZOOKEEPER_ADDRESS, veniceCluster.getZk().getAddress())
            .put(CLUSTER_NAME, veniceCluster.getClusterName()).build();

    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    keySerializer = new VeniceAvroGenericSerializer(stringSchema);
    valueSerializer = new VeniceAvroGenericSerializer(stringSchema);

    veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress())
        .getVeniceWriter(storeVersionName, keySerializer, valueSerializer);
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
    IOUtils.closeQuietly(veniceWriter);
  }

  @Test(timeOut = 20000)
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
    }

    Assert.assertEquals(totalCacheLookupRequest, 100.0);
    Assert.assertEquals(totalCacheHitRequest, 98.0);
    Assert.assertEquals(totalCachePutRequest, 2.0);
  }
}
