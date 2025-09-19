package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_USE_HEARTBEAT_LAG_FOR_READY_TO_SERVE_CHECK_ENABLED;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.NonLocalAccessException;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientTestWithHeartbeatReadyToServeCheckTest {
  private static final int KEY_COUNT = 10;
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

  @Test(dataProvider = "dv-client-config-provider", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT
      * 2)
  public void testHybridStore(DaVinciConfig daVinciConfig) throws Exception {
    // Create store
    final int partitionCount = 2;
    final int emptyPartition = 0;
    final int dataPartition = 1;
    String storeName = Utils.getUniqueString("hybrid-store");

    // Convert it to hybrid
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(dataPartition)));
    setupHybridStore(storeName, paramsConsumer);

    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(SERVER_USE_HEARTBEAT_LAG_FOR_READY_TO_SERVE_CHECK_ENABLED, true)
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();

    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Integer> client = factory.getAndStartGenericAvroClient(storeName, daVinciConfig);
      // subscribe to a partition without data
      client.subscribe(Collections.singleton(emptyPartition)).get();
      for (int i = 0; i < KEY_COUNT; i++) {
        int key = i;
        assertThrows(NonLocalAccessException.class, () -> client.get(key).get());
      }
      client.unsubscribe(Collections.singleton(emptyPartition));

      // subscribe to a partition with data
      client.subscribe(Collections.singleton(dataPartition)).get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {

        Map<Integer, Integer> keyValueMap = new HashMap<>();
        for (Integer i = 0; i < KEY_COUNT; i++) {
          assertEquals(client.get(i).get(), i);
          keyValueMap.put(i, i);
        }

        Map<Integer, Integer> batchGetResult = client.batchGet(keyValueMap.keySet()).get();
        assertNotNull(batchGetResult);
        assertEquals(batchGetResult, keyValueMap);
      });

      // Write some fresh records to override the old value. Make sure we can read the new value.
      List<Pair<Object, Object>> dataToPublish = new ArrayList<>();
      dataToPublish.add(new Pair<>(0, 1));
      dataToPublish.add(new Pair<>(1, 2));
      dataToPublish.add(new Pair<>(3, 4));

      generateHybridData(storeName, dataToPublish);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Pair<Object, Object> entry: dataToPublish) {
          assertEquals(client.get((Integer) entry.getFirst()).get(), entry.getSecond());
        }
      });
    }
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer) throws Exception {
    setupHybridStore(storeName, paramsConsumer, KEY_COUNT);
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer, int keyCount)
      throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10)
        .setIncrementalPushEnabled(true);
    paramsConsumer.accept(params);
    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      cluster.createMetaSystemStore(storeName);
      client.updateStore(storeName, params);
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      if (keyCount > 0) {
        SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
            cluster,
            storeName,
            Version.PushType.STREAM,
            Pair.create(VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
        try {
          for (int i = 0; i < keyCount; i++) {
            IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i);
          }
        } finally {
          producer.stop();
        }
      }
    });
  }

  private void generateHybridData(String storeName, List<Pair<Object, Object>> dataToWrite) {
    SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
        cluster,
        storeName,
        Version.PushType.STREAM,
        Pair.create(VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
    try {
      for (Pair<Object, Object> record: dataToWrite) {
        IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, record.getFirst(), record.getSecond());
      }
    } finally {
      producer.stop();
    }
  }
}
