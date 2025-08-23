package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.KEY_URN_COMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static org.testng.Assert.assertEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.compression.UrnDictV1;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientKeyCompressionTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientTest.class);
  private static final int TEST_TIMEOUT = 120_000;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKeyUrnCompression() throws Exception {
    // Setup store and create version 1
    String keySchema = "\"string\"";
    String valueSchema = "\"int\"";
    int keyCount = 1000;
    Map batchData = new HashMap<>();
    for (int i = 0; i < keyCount; ++i) {
      String urnType;
      if (i % 3 == 0) {
        urnType = "company";
      } else if (i % 2 == 0) {
        urnType = "member";
      } else {
        urnType = "group";
      }
      String key = UrnDictV1.URN_PREFIX + urnType + UrnDictV1.URN_SEPARATOR + i;
      batchData.put(key, i);
    }
    String storeName = cluster.createStore(keySchema, valueSchema, batchData.entrySet().stream());

    cluster.createMetaSystemStore(storeName);
    cluster.createPushStatusSystemStore(storeName);
    // Enable key urn compression
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setKeyUrnCompressionEnabled(true));
    // Create a new version with key urn compression enabled
    cluster.createVersion(storeName, keySchema, valueSchema, batchData.entrySet().stream());

    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .put(KEY_URN_COMPRESSION_ENABLED, true)
        .build();

    // Create dvc client and subscribe
    DaVinciClient<Object, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig);
    client.subscribeAll().get();

    Map<String, Integer> keyValueMap = (Map<String, Integer>) batchData;
    // Verify data correctness
    for (Map.Entry<String, Integer> entry: keyValueMap.entrySet()) {
      Object actualValue = client.get(entry.getKey()).get();
      assertEquals(actualValue, entry.getValue(), "Value mismatch for key: " + entry.getKey());
    }

    // Close the dvc client
    client.close();
  }
}
