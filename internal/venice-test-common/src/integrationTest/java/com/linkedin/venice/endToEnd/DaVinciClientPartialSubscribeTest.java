package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientPartialSubscribeTest {
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 120_000;
  private DaVinciClusterFixture fixture;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    fixture = new DaVinciClusterFixture(true);
    cluster = fixture.getCluster();
    d2Client = fixture.getD2Client();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testBootstrapSubscription(DaVinciConfig daVinciConfig) throws Exception {
    String storeName1 = fixture.createStoreWithSystemStores(KEY_COUNT);
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    // Test multiple clients sharing the same ClientConfig/MetricsRepository & base data path
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, daVinciConfig);

      // Test non-existent key access
      client1.subscribeAll().get();
      assertNull(client1.get(KEY_COUNT + 1).get());

      // Test single-get access
      Map<Integer, Integer> keyValueMap = new HashMap<>();
      for (int k = 0; k < KEY_COUNT; ++k) {
        assertEquals(client1.get(k).get(), 1);
        keyValueMap.put(k, 1);
      }

      // Test batch-get access
      assertEquals(client1.batchGet(keyValueMap.keySet()).get(), keyValueMap);
    }

    // Test managed clients
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster,
        Optional.of(Collections.singleton(storeName1)))) {

      DaVinciClient<Integer, Object> client1 = factory.getAndStartGenericAvroClient(storeName1, daVinciConfig);

      Set<Integer> partitions = new HashSet<>();

      for (int i = 0; i < 2; i++) {
        partitions.add(i);
      }

      client1.subscribe(partitions);
      assertEquals(client1.getPartitionCount(), 3);

      DaVinciBackend daVinciBackend = AvroGenericDaVinciClient.getBackend();
      if (daVinciBackend != null) {
        StoreBackend storeBackend = daVinciBackend.getStoreOrThrow(storeName1);
        ComplementSet<Integer> subscription = storeBackend.getSubscription();
        assertTrue(subscription.contains(0));
        assertTrue(subscription.contains(1));
        assertFalse(subscription.contains(2));
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class)
  public void testPartialSubscription(DaVinciConfig daVinciConfig) throws Exception {
    String storeName = fixture.createStoreWithSystemStores(KEY_COUNT);
    VeniceProperties backendConfig =
        new PropertyBuilder().put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L).build();

    Set<Integer> keySet = new HashSet<>();
    for (int i = 0; i < KEY_COUNT; ++i) {
      keySet.add(i);
    }

    try (DaVinciClient<Integer, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // We only subscribe to 1/3 of the partitions so some data will not be present locally.
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(() -> client.batchGet(keySet).get());
    }

    // Update the store to use non-default partitioner
    cluster.useControllerClient(
        controllerClient -> TestUtils.assertCommand(
            controllerClient.updateStore(
                storeName,
                new UpdateStoreQueryParams().setPartitionerClass(ConstantVenicePartitioner.class.getName())
                    .setPartitionerParams(
                        Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(2))))));
    cluster.createVersion(storeName, KEY_COUNT);
    try (DaVinciClient<Integer, Object> client =
        ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, daVinciConfig, backendConfig)) {
      // Only subscribe a subset of the partitions
      client.subscribe(Collections.singleton(0)).get();
      assertThrows(() -> client.batchGet(keySet).get());
    }
  }

}
