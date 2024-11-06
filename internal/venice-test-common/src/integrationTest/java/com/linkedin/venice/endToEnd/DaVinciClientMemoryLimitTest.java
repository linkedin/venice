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
import static com.linkedin.venice.ConfigKeys.INGESTION_MEMORY_LIMIT_STORE_LIST;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_APPLICATION_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SERVICE_PORT;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
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
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DaVinciClientMemoryLimitTest {
  private static final int TEST_TIMEOUT = 180_000;
  private VeniceClusterWrapper veniceCluster;
  private D2Client d2Client;
  private String storeName;
  private String storeNameWithoutMemoryEnforcement;
  private Properties vpjProperties;
  private AvroGenericStoreClient client;
  private CachingDaVinciClientFactory factory;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 10L);
    // To allow more times for DaVinci clients to report status
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS, 15);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(2)
        .replicationFactor(1)
        .partitionSize(100)
        .sslToKafka(false)
        .sslToStorageNodes(false)
        .extraProperties(clusterConfig)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
    d2Client = new D2ClientBuilder().setZkHosts(veniceCluster.getZk().getAddress())
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
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  private VeniceProperties getDaVinciBackendConfig(
      boolean ingestionIsolationEnabledInDaVinci,
      boolean useDaVinciSpecificExecutionStatusForError) {
    return getDaVinciBackendConfig(
        ingestionIsolationEnabledInDaVinci,
        useDaVinciSpecificExecutionStatusForError,
        Collections.EMPTY_SET);
  }

  private VeniceProperties getDaVinciBackendConfig(
      boolean ingestionIsolationEnabledInDaVinci,
      boolean useDaVinciSpecificExecutionStatusForError,
      Set<String> memoryLimitStores) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder venicePropertyBuilder = new PropertyBuilder();
    venicePropertyBuilder.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(D2_ZK_HOSTS_ADDRESS, veniceCluster.getZk().getAddress())
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .put(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, "2MB")
        .put(ROCKSDB_TOTAL_MEMTABLE_USAGE_CAP_IN_BYTES, "10MB")
        .put(INGESTION_MEMORY_LIMIT_STORE_LIST, String.join(",", memoryLimitStores))
        .put(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, useDaVinciSpecificExecutionStatusForError);
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

  /**
   * TODO: If we deflake the 2 disabled tests in this class, we should make them leverage the stores setup here, or else
   * move them to another class...
   */
  @BeforeMethod
  public void storeSetup() throws IOException {
    this.storeName = Utils.getUniqueString("davinci_memory_limit_test");
    this.storeNameWithoutMemoryEnforcement = Utils.getUniqueString("store_without_memory_enforcement");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 100, 100);

    this.vpjProperties = defaultVPJProps(veniceCluster, inputDirPath, this.storeName);
    setupStore(this.storeName, this.vpjProperties, recordSchema);

    Properties otherProps = defaultVPJProps(veniceCluster, inputDirPath, this.storeNameWithoutMemoryEnforcement);
    setupStore(this.storeNameWithoutMemoryEnforcement, otherProps, recordSchema);

    this.client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()));

    this.factory = null; // will be constructed in the test
  }

  private void setupStore(String store, Properties vpjProperties, Schema recordSchema) {
    this.veniceCluster.createStoreForJob(recordSchema, vpjProperties);
    this.veniceCluster.createMetaSystemStore(store);
    this.veniceCluster.createPushStatusSystemStore(store);

    // Make sure system stores are enabled
    this.veniceCluster.useControllerClient(cc -> {
      StoreResponse storeResponse = TestUtils.assertCommand(cc.getStore(store));
      assertTrue(storeResponse.getStore().isDaVinciPushStatusStoreEnabled());
      assertTrue(storeResponse.getStore().isStoreMetaSystemStoreEnabled());
    });
  }

  @AfterMethod
  public void storeCleanup() {
    Utils.closeQuietlyWithErrorLogged(this.client);
    Utils.closeQuietlyWithErrorLogged(this.factory);

    this.veniceCluster.useControllerClient(cc -> {
      cc.disableAndDeleteStore(storeName);
      cc.disableAndDeleteStore(storeNameWithoutMemoryEnforcement);
    });
  }

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void testDaVinciMemoryLimitShouldFailLargeDataPush(
      boolean ingestionIsolationEnabledInDaVinci,
      boolean useDaVinciSpecificExecutionStatusForError) throws Exception {

    // Run VPJ
    this.veniceCluster.useControllerClient(cc -> runVPJ(this.vpjProperties, 1, cc));

    // Verify some records (note, records 1-100 have been pushed)
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      try {
        for (int i = 1; i <= 100; i++) {
          String key = Integer.toString(i);
          Object value = this.client.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
        }
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });

    // Spin up DaVinci client
    VeniceProperties backendConfig = getDaVinciBackendConfig(
        ingestionIsolationEnabledInDaVinci,
        useDaVinciSpecificExecutionStatusForError,
        new HashSet<>(Arrays.asList(storeName)));
    this.factory = new CachingDaVinciClientFactory(
        this.d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        backendConfig);

    DaVinciClient daVinciClient = factory.getGenericAvroClient(
        this.storeName,
        new DaVinciConfig().setIsolated(true).setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));

    // N.B.: The start and subscribe calls below seem to be frequent causes of failure.
    daVinciClient.start();

    // Indefinite wait... the test's timeout will kick in, in the worst case...
    daVinciClient.subscribeAll().get();

    // Validate some entries
    for (int i = 1; i <= 100; i++) {
      String key = Integer.toString(i);
      Object value = daVinciClient.get(key).get();
      assertNotNull(value, "Key " + i + " should not be missing!");
    }

    // Run a bigger push and the push should fail
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 1000, 100000);
    final Properties vpjPropertiesForV2 = defaultVPJProps(this.veniceCluster, inputDirPath, this.storeName);

    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> this.veniceCluster.useControllerClient(cc -> runVPJ(vpjPropertiesForV2, 2, cc)));
    assertTrue(
        exception.getMessage()
            .contains(
                "status: " + (useDaVinciSpecificExecutionStatusForError
                    ? ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED
                    : ExecutionStatus.ERROR)));
    assertTrue(
        exception.getMessage()
            .contains(
                "Found a failed partition replica in Da Vinci"
                    + (useDaVinciSpecificExecutionStatusForError ? " due to memory limit reached" : "")));

    // Run a bigger push against a non-enforced store should succeed
    Properties vpjProperties3 = defaultVPJProps(veniceCluster, inputDirPath, storeNameWithoutMemoryEnforcement);

    // Run a big VPJ push without DaVinci and it should succeed
    this.veniceCluster.useControllerClient(cc -> runVPJ(vpjProperties3, 1, cc));

    // Spin up a DaVinci client for this store
    DaVinciClient daVinciClientForStoreWithoutMemoryEnforcement = this.factory.getGenericAvroClient(
        this.storeNameWithoutMemoryEnforcement,
        new DaVinciConfig().setIsolated(true).setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
    daVinciClientForStoreWithoutMemoryEnforcement.start();

    // Indefinite wait... the test's timeout will kick in, in the worst case...
    daVinciClientForStoreWithoutMemoryEnforcement.subscribeAll().get();

    // Another big push should succeed as well
    this.veniceCluster.useControllerClient(cc -> runVPJ(vpjProperties3, 2, cc));
  }

  @Test(enabled = false)
  public void testDaVinciMemoryLimitShouldFailLargeDataPushAndResumeHybridStore(
      boolean ingestionIsolationEnabledInDaVinci,
      boolean useDaVinciSpecificExecutionStatusForError) throws Exception {
    String batchOnlyStoreName = Utils.getUniqueString("davinci_memory_limit_test_batch_only");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 100, 100);
    Properties vpjProperties = defaultVPJProps(veniceCluster, inputDirPath, batchOnlyStoreName);

    try (
        ControllerClient controllerClient =
            createStoreForJob(veniceCluster.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(batchOnlyStoreName)
                .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      veniceCluster.createMetaSystemStore(batchOnlyStoreName);
      veniceCluster.createPushStatusSystemStore(batchOnlyStoreName);

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
      veniceCluster.createMetaSystemStore(hybridStoreName);
      veniceCluster.createPushStatusSystemStore(hybridStoreName);

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
      VeniceProperties backendConfig =
          getDaVinciBackendConfig(ingestionIsolationEnabledInDaVinci, useDaVinciSpecificExecutionStatusForError);
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
        SystemProducer veniceProducer = getSamzaProducer(veniceCluster, hybridStoreName, Version.PushType.STREAM);

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
        TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 1000, 100000);
        final Properties vpjPropertiesForV2 = defaultVPJProps(veniceCluster, inputDirPath, batchOnlyStoreName);

        VeniceException exception =
            expectThrows(VeniceException.class, () -> runVPJ(vpjPropertiesForV2, 2, controllerClient));
        assertTrue(
            exception.getMessage()
                .contains(
                    "status: " + (useDaVinciSpecificExecutionStatusForError
                        ? ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED
                        : ExecutionStatus.ERROR)));
        assertTrue(
            exception.getMessage()
                .contains(
                    "Found a failed partition replica in Da Vinci"
                        + (useDaVinciSpecificExecutionStatusForError ? " due to memory limit reached" : "")));

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
      } finally {
        controllerClient.disableAndDeleteStore(batchOnlyStoreName);
        controllerClient.disableAndDeleteStore(hybridStoreName);
      }
    }
  }

  /**
   * This test is buggy; if ingestion error happens for a completed replica, server would unsubscribe the partition and
   * clear the partition exception list; therefore, whether the Gauge metric has value depends on if measurement takes
   * place after the exception happens but before the partition exception list is cleared.
   * TODO: fix metric "ingestion_stuck_by_memory_constraint"
   * @param ingestionIsolationEnabledInDaVinci
   * @throws Exception
   */
  @Test(enabled = false, timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testHybridStoreHittingMemoryLimiterShouldResumeAfterFreeUpResource(
      boolean ingestionIsolationEnabledInDaVinci) throws Exception {
    String batchOnlyStoreName = Utils.getUniqueString("davinci_memory_limit_test_batch_only");
    // Test a medium push close to the memory limit
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 190, 100000); // ~19MB
    Properties vpjProperties = defaultVPJProps(veniceCluster, inputDirPath, batchOnlyStoreName);

    try (
        ControllerClient controllerClient =
            createStoreForJob(veniceCluster.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(batchOnlyStoreName)
                .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      veniceCluster.createMetaSystemStore(batchOnlyStoreName);
      veniceCluster.createPushStatusSystemStore(batchOnlyStoreName);

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
      veniceCluster.createMetaSystemStore(hybridStoreName);
      veniceCluster.createPushStatusSystemStore(hybridStoreName);

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
      VeniceProperties backendConfig = getDaVinciBackendConfig(ingestionIsolationEnabledInDaVinci, false);
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
        SystemProducer veniceProducer = getSamzaProducer(veniceCluster, hybridStoreName, Version.PushType.STREAM);

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
      } finally {
        controllerClient.disableAndDeleteStore(batchOnlyStoreName);
        controllerClient.disableAndDeleteStore(hybridStoreName);
      }
    }
  }
}
