package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_MEMTABLE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientMemoryLimitTest {
  private static final int TEST_TIMEOUT = 120_000;
  private VeniceClusterWrapper venice;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 10L);
    // To allow more times for DaVinci clients to report status
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS, 15);
    venice = ServiceFactory.getVeniceCluster(1, 2, 1, 1, 100, false, false, clusterConfig);
    d2Client = new D2ClientBuilder().setZkHosts(venice.getZk().getAddress())
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
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  private VeniceProperties getDaVinciBackendConfig(boolean ingestionIsolationEnabledInDaVinci) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder venicePropertyBuilder = new PropertyBuilder();
    venicePropertyBuilder.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(D2_ZK_HOSTS_ADDRESS, venice.getZk().getAddress())
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .put(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, "2MB")
        .put(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "10MB");
    if (ingestionIsolationEnabledInDaVinci) {
      venicePropertyBuilder.put(SERVER_INGESTION_MODE, IngestionMode.ISOLATED);
      venicePropertyBuilder.put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, TestUtils.getFreePort());
      venicePropertyBuilder.put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, TestUtils.getFreePort());
      venicePropertyBuilder.put(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "-Xms256M;-Xmx256M");
      venicePropertyBuilder.put(INGESTION_MEMORY_LIMIT, "296MB"); // 256M + 10M + 30M
    } else {
      venicePropertyBuilder.put(INGESTION_MEMORY_LIMIT, "30MB");
    }

    return venicePropertyBuilder.build();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDaVinciMemoryLimitShouldFailLargeDataPush(boolean ingestionIsolationEnabledInDaVinci)
      throws Exception {
    String storeName = Utils.getUniqueString("davinci_memory_limit_test");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, true, 100, 100);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      venice.createMetaSystemStore(storeName);
      venice.createPushStatusSystemStore(storeName);

      // Make sure DaVinci push status system store is enabled
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      assertFalse(storeResponse.isError(), "Store response receives an error: " + storeResponse.getError());
      assertTrue(storeResponse.getStore().isDaVinciPushStatusStoreEnabled());

      // Do an VPJ push
      runVPJ(vpjProperties, 1, controllerClient);

      // Verify some records (note, records 1-100 have been pushed)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i <= 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Spin up DaVinci client
      VeniceProperties backendConfig = getDaVinciBackendConfig(ingestionIsolationEnabledInDaVinci);
      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig)) {

        DaVinciClient daVinciClient = factory.getGenericAvroClient(
            storeName,
            new DaVinciConfig().setIsolated(true).setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClient.start();
        daVinciClient.subscribeAll().get(30, TimeUnit.SECONDS);

        // Validate some entries
        for (int i = 1; i <= 100; i++) {
          String key = Integer.toString(i);
          Object value = daVinciClient.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
        }

        // Run a bigger push and the push should fail
        inputDir = getTempDataDirectory();
        inputDirPath = "file://" + inputDir.getAbsolutePath();
        writeSimpleAvroFileWithUserSchema(inputDir, true, 1000, 100000);
        final Properties vpjPropertiesForV2 = defaultVPJProps(venice, inputDirPath, storeName);

        VeniceException exception =
            expectThrows(VeniceException.class, () -> runVPJ(vpjPropertiesForV2, 2, controllerClient));
        assertTrue(exception.getMessage().contains("Found a failed partition replica in Da Vinci"));
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDaVinciMemoryLimitShouldFailLargeDataPushAndResumeHybridStore(
      boolean ingestionIsolationEnabledInDaVinci) throws Exception {
    String batchOnlyStoreName = Utils.getUniqueString("davinci_memory_limit_test_batch_only");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, true, 100, 100);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, batchOnlyStoreName);

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(batchOnlyStoreName).setVeniceURL(venice.getRandomRouterURL()))) {
      venice.createMetaSystemStore(batchOnlyStoreName);
      venice.createPushStatusSystemStore(batchOnlyStoreName);

      // Make sure DaVinci push status system store is enabled
      StoreResponse storeResponseForBatchOnlyStore = controllerClient.getStore(batchOnlyStoreName);
      assertFalse(
          storeResponseForBatchOnlyStore.isError(),
          "Store response receives an error: " + storeResponseForBatchOnlyStore.getError());
      assertTrue(storeResponseForBatchOnlyStore.getStore().isDaVinciPushStatusStoreEnabled());

      // Create a hybrid store
      String hybridStoreName = Utils.getUniqueString("davinci_memory_limit_test_hybrid");
      String hybridKeySchemaStr = "\"string\"";
      String hybridValueSchemaStr = "\"string\"";
      NewStoreResponse storeCreationResponseForHybridStore =
          controllerClient.createNewStore(hybridStoreName, "test_owner", hybridKeySchemaStr, hybridValueSchemaStr);
      assertFalse(
          storeCreationResponseForHybridStore.isError(),
          "Received error when creating a store: " + storeCreationResponseForHybridStore.getError());
      // Update it to hybrid
      ControllerResponse updateStoreResponseForHybridStore = controllerClient.updateStore(
          hybridStoreName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(60).setHybridOffsetLagThreshold(1));
      assertFalse(
          updateStoreResponseForHybridStore.isError(),
          "Received error when converting a hybrid store: " + updateStoreResponseForHybridStore.getError());
      venice.createMetaSystemStore(hybridStoreName);
      venice.createPushStatusSystemStore(hybridStoreName);

      ControllerResponse emptyPushForHybridStore =
          controllerClient.sendEmptyPushAndWait(hybridStoreName, "test_hybrid_push_v1", 1024 * 1024 * 100l, 30 * 1000);
      assertFalse(
          emptyPushForHybridStore.isError(),
          "Failed to empty push to the hybrid store: " + emptyPushForHybridStore.getError());

      // Do an VPJ push to the batch-only store
      runVPJ(vpjProperties, 1, controllerClient);

      // Verify some records (note, records 1-100 have been pushed)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i <= 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Spin up DaVinci client
      VeniceProperties backendConfig = getDaVinciBackendConfig(ingestionIsolationEnabledInDaVinci);
      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig)) {

        DaVinciClient daVinciClientForBatchOnlyStore = factory.getGenericAvroClient(
            batchOnlyStoreName,
            new DaVinciConfig().setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClientForBatchOnlyStore.start();
        daVinciClientForBatchOnlyStore.subscribeAll().get(30, TimeUnit.SECONDS);

        // Validate some entries
        for (int i = 1; i <= 100; i++) {
          String key = Integer.toString(i);
          Object value = daVinciClientForBatchOnlyStore.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
        }

        DaVinciClient daVinciClientForHybridStore = factory.getGenericAvroClient(
            hybridStoreName,
            new DaVinciConfig().setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClientForHybridStore.start();
        daVinciClientForHybridStore.subscribeAll().get(30, TimeUnit.SECONDS);

        // Write some records and verify
        SystemProducer veniceProducer = getSamzaProducer(venice, hybridStoreName, Version.PushType.STREAM);

        int hybridStoreKeyId = 0;
        for (; hybridStoreKeyId <= 100; ++hybridStoreKeyId) {
          sendCustomSizeStreamingRecord(veniceProducer, hybridStoreName, hybridStoreKeyId, 1000);
        }

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 0; i <= 100; ++i) {
            try {
              assertNotNull(
                  daVinciClientForHybridStore.get(Integer.toString(i)).get(),
                  "Value for key: " + i + " shouldn't be null");
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          }
        });

        // Run a bigger push and the push should fail
        inputDir = getTempDataDirectory();
        inputDirPath = "file://" + inputDir.getAbsolutePath();
        writeSimpleAvroFileWithUserSchema(inputDir, true, 1000, 100000);
        final Properties vpjPropertiesForV2 = defaultVPJProps(venice, inputDirPath, batchOnlyStoreName);

        VeniceException exception =
            expectThrows(VeniceException.class, () -> runVPJ(vpjPropertiesForV2, 2, controllerClient));
        assertTrue(exception.getMessage().contains("Found a failed partition replica in Da Vinci"));

        // Write more records to the hybrid store.
        for (; hybridStoreKeyId < 200; ++hybridStoreKeyId) {
          sendCustomSizeStreamingRecord(veniceProducer, hybridStoreName, hybridStoreKeyId, 1000);
        }

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          for (int i = 0; i < 200; ++i) {
            try {
              assertNotNull(
                  daVinciClientForHybridStore.get(Integer.toString(i)).get(),
                  "Value for key: " + i + " shouldn't be null");
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testHybridStoreHittingMemoryLimiterShouldResumeAfterFreeUpResource(// ) throws Exception {
      boolean ingestionIsolationEnabledInDaVinci) throws Exception {
    String batchOnlyStoreName = Utils.getUniqueString("davinci_memory_limit_test_batch_only");
    // Test a medium push close to the memory limit
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, true, 190, 100000); // ~19MB
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, batchOnlyStoreName);

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(batchOnlyStoreName).setVeniceURL(venice.getRandomRouterURL()))) {
      venice.createMetaSystemStore(batchOnlyStoreName);
      venice.createPushStatusSystemStore(batchOnlyStoreName);

      // Make sure DaVinci push status system store is enabled
      StoreResponse storeResponseForBatchOnlyStore = controllerClient.getStore(batchOnlyStoreName);
      assertFalse(
          storeResponseForBatchOnlyStore.isError(),
          "Store response receives an error: " + storeResponseForBatchOnlyStore.getError());
      assertTrue(storeResponseForBatchOnlyStore.getStore().isDaVinciPushStatusStoreEnabled());

      // Create a hybrid store
      String hybridStoreName = Utils.getUniqueString("davinci_memory_limit_test_hybrid");
      String hybridKeySchemaStr = "\"string\"";
      String hybridValueSchemaStr = "\"string\"";
      NewStoreResponse storeCreationResponseForHybridStore =
          controllerClient.createNewStore(hybridStoreName, "test_owner", hybridKeySchemaStr, hybridValueSchemaStr);
      assertFalse(
          storeCreationResponseForHybridStore.isError(),
          "Received error when creating a store: " + storeCreationResponseForHybridStore.getError());
      // Update it to hybrid
      ControllerResponse updateStoreResponseForHybridStore = controllerClient.updateStore(
          hybridStoreName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(60).setHybridOffsetLagThreshold(1));
      assertFalse(
          updateStoreResponseForHybridStore.isError(),
          "Received error when converting a hybrid store: " + updateStoreResponseForHybridStore.getError());
      venice.createMetaSystemStore(hybridStoreName);
      venice.createPushStatusSystemStore(hybridStoreName);

      ControllerResponse emptyPushForHybridStore =
          controllerClient.sendEmptyPushAndWait(hybridStoreName, "test_hybrid_push_v1", 1024 * 1024 * 100l, 30 * 1000);
      assertFalse(
          emptyPushForHybridStore.isError(),
          "Failed to empty push to the hybrid store: " + emptyPushForHybridStore.getError());

      // Do an VPJ push to the batch-only store
      runVPJ(vpjProperties, 1, controllerClient);

      // Verify some records (note, records 1-150 have been pushed)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i <= 150; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Spin up DaVinci client
      VeniceProperties backendConfig = getDaVinciBackendConfig(ingestionIsolationEnabledInDaVinci);
      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig)) {

        DaVinciClient daVinciClientForBatchOnlyStore = factory.getGenericAvroClient(
            batchOnlyStoreName,
            new DaVinciConfig().setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClientForBatchOnlyStore.start();
        daVinciClientForBatchOnlyStore.subscribeAll().get(30, TimeUnit.SECONDS);

        // Validate some entries
        for (int i = 1; i <= 150; i++) {
          String key = Integer.toString(i);
          Object value = daVinciClientForBatchOnlyStore.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
        }

        DaVinciClient daVinciClientForHybridStore = factory.getGenericAvroClient(
            hybridStoreName,
            new DaVinciConfig().setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClientForHybridStore.start();
        daVinciClientForHybridStore.subscribeAll().get(30, TimeUnit.SECONDS);

        // Write some large records and verify
        SystemProducer veniceProducer = getSamzaProducer(venice, hybridStoreName, Version.PushType.STREAM);

        int hybridStoreKeyId = 0;
        for (; hybridStoreKeyId < 100; ++hybridStoreKeyId) {
          sendCustomSizeStreamingRecord(veniceProducer, hybridStoreName, hybridStoreKeyId, 100000);
        }

        // Hybrid store ingestion should be stuck and verify the metrics
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          assertEquals(
              metricsRepository.metrics()
                  .get("." + hybridStoreName + "--ingestion_stuck_by_memory_constraint.Gauge")
                  .value(),
              1.0d);
          assertEquals(
              metricsRepository.metrics().get(".total--ingestion_stuck_by_memory_constraint.Gauge").value(),
              1.0d);
          assertEquals(
              metricsRepository.metrics()
                  .get("." + batchOnlyStoreName + "--ingestion_stuck_by_memory_constraint.Gauge")
                  .value(),
              0.0d);
        });

        // DaVinci unsubscribes the batch only store
        daVinciClientForBatchOnlyStore.unsubscribeAll();

        // After removing the batch-only store, the hybrid store should resume
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          assertEquals(
              metricsRepository.metrics()
                  .get("." + hybridStoreName + "--ingestion_stuck_by_memory_constraint.Gauge")
                  .value(),
              0.0d);
          assertEquals(
              metricsRepository.metrics().get(".total--ingestion_stuck_by_memory_constraint.Gauge").value(),
              0.0d);
          assertEquals(
              metricsRepository.metrics()
                  .get("." + batchOnlyStoreName + "--ingestion_stuck_by_memory_constraint.Gauge")
                  .value(),
              0.0d);
        });

        // Ingestion of hybrid store current version should resume
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 0; i < 100; ++i) {
            try {
              assertNotNull(
                  daVinciClientForHybridStore.get(Integer.toString(i)).get(),
                  "Value for key: " + i + " shouldn't be null");
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          }
        });
      }
    }
  }
}
