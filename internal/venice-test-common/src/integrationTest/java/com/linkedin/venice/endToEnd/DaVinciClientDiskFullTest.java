package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_MEMTABLE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.VersionBackend;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.SentPushJobDetailsTracker;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.MockedConstruction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientDiskFullTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientDiskFullTest.class);
  private static final int TEST_TIMEOUT = 180_000;
  // v2 push: enough records to trigger ingestion, but size doesn't matter since DiskUsage is mocked
  private static final int LARGE_PUSH_RECORD_COUNT = 100;
  private static final int LARGE_PUSH_RECORD_SIZE = 1000;
  private VeniceClusterWrapper venice;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    // To allow more times for DaVinci clients to report status
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS, 15);
    venice = getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(1)
            .partitionSize(100)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .extraProperties(clusterConfig)
            .build());
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

  private VeniceProperties getDaVinciBackendConfig(boolean useDaVinciSpecificExecutionStatusForError) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder venicePropertyBuilder = new PropertyBuilder();

    venicePropertyBuilder.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(D2_ZK_HOSTS_ADDRESS, venice.getZk().getAddress())
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(ROCKSDB_MEMTABLE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 2 * 1024 * 1024L)
        .put(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 2 * 1024 * 1024L)
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .put(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, useDaVinciSpecificExecutionStatusForError)
        // Threshold value is irrelevant — DiskUsage is mocked via mockConstruction in the test
        .put(SERVER_DISK_FULL_THRESHOLD, 0.95);
    return venicePropertyBuilder.build();
  }

  /** copy of SentPushJobDetailsTrackerImpl from venice-push-job */
  public class SentPushJobDetailsTrackerImpl implements SentPushJobDetailsTracker {
    private final List<String> storeNames;
    private final List<Integer> versions;
    private final List<PushJobDetails> recordedPushJobDetails;

    public SentPushJobDetailsTrackerImpl() {
      storeNames = new ArrayList<>();
      versions = new ArrayList<>();
      recordedPushJobDetails = new ArrayList<>();
    }

    @Override
    public void record(String storeName, int version, PushJobDetails pushJobDetails) {
      storeNames.add(storeName);
      versions.add(version);
      recordedPushJobDetails.add(makeCopyOf(pushJobDetails));
    }

    List<PushJobDetails> getRecordedPushJobDetails() {
      return Collections.unmodifiableList(recordedPushJobDetails);
    }

    private PushJobDetails makeCopyOf(PushJobDetails pushJobDetails) {
      PushJobDetails copy = new PushJobDetails();
      copy.reportTimestamp = pushJobDetails.reportTimestamp;
      copy.overallStatus = new ArrayList<>(pushJobDetails.overallStatus);
      copy.coloStatus =
          pushJobDetails.coloStatus == null ? pushJobDetails.coloStatus : new HashMap<>(pushJobDetails.coloStatus);
      copy.pushId = pushJobDetails.pushId;
      copy.partitionCount = pushJobDetails.partitionCount;
      copy.valueCompressionStrategy = pushJobDetails.valueCompressionStrategy;
      copy.chunkingEnabled = pushJobDetails.chunkingEnabled;
      copy.jobDurationInMs = pushJobDetails.jobDurationInMs;
      copy.totalNumberOfRecords = pushJobDetails.totalNumberOfRecords;
      copy.totalKeyBytes = pushJobDetails.totalKeyBytes;
      copy.totalRawValueBytes = pushJobDetails.totalRawValueBytes;
      copy.pushJobConfigs = pushJobDetails.pushJobConfigs == null
          ? pushJobDetails.pushJobConfigs
          : new HashMap<>(pushJobDetails.pushJobConfigs);
      copy.producerConfigs = pushJobDetails.producerConfigs == null
          ? pushJobDetails.producerConfigs
          : new HashMap<>(pushJobDetails.producerConfigs);
      copy.pushJobLatestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
      copy.failureDetails = pushJobDetails.failureDetails;
      return copy;
    }
  }

  /**
   * Tests that DaVinci client correctly detects and reports disk full errors during ingestion.
   *
   * Uses {@code mockConstruction(DiskUsage.class)} to intercept the DiskUsage instance created
   * inside KafkaStoreIngestionService. An {@link AtomicBoolean} controls when {@code isDiskFull()}
   * returns true, making the test fully deterministic — no dependency on actual disk space,
   * RocksDB flush timing, or CI runner disk fluctuations.
   *
   * Flow:
   * 1. Push v1 (small, 1 record) via VPJ to server
   * 2. Start DaVinci client with mocked DiskUsage (isDiskFull = false)
   * 3. DaVinci subscribes and ingests v1 successfully
   * 4. Flip isDiskFull to true
   * 5. Push v2 — DaVinci ingests and immediately hits "disk full"
   * 6. VPJ detects the DaVinci error status and throws VeniceException
   */
  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDaVinciDiskFullFailure(boolean useDaVinciSpecificExecutionStatusForError) throws Exception {
    String storeName = Utils.getUniqueString("davinci_disk_full_test");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 1, 10);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    vpjProperties.putAll(
        PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      venice.createMetaSystemStore(storeName);
      venice.createPushStatusSystemStore(storeName);

      // Make sure DaVinci push status system store is enabled
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      assertFalse(storeResponse.isError(), "Store response receives an error: " + storeResponse.getError());
      assertTrue(storeResponse.getStore().isDaVinciPushStatusStoreEnabled());

      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);

      // Verify some records (note, record 1 have been pushed)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i <= 1; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Use mockConstruction to intercept DiskUsage created inside KafkaStoreIngestionService.
      // This eliminates all non-determinism from disk space measurement and RocksDB flush timing.
      AtomicBoolean simulateDiskFull = new AtomicBoolean(false);

      try (MockedConstruction<DiskUsage> mockedDiskUsage = mockConstruction(DiskUsage.class, (mock, context) -> {
        when(mock.isDiskFull(anyLong())).thenAnswer(inv -> simulateDiskFull.get());
        when(mock.getDiskStatus()).thenReturn("Mocked DiskUsage: simulated disk full for testing");
      })) {
        // Spin up DaVinci client — DiskUsage constructor is intercepted by mockConstruction
        VeniceProperties backendConfig = getDaVinciBackendConfig(useDaVinciSpecificExecutionStatusForError);
        MetricsRepository metricsRepository = new MetricsRepository();
        try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
            d2Client,
            VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
            metricsRepository,
            backendConfig,
            venice)) {

          DaVinciClient daVinciClient = factory.getGenericAvroClient(
              storeName,
              new DaVinciConfig().setIsolated(true).setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
          daVinciClient.start();

          daVinciClient.subscribeAll().get(30, TimeUnit.SECONDS);

          // Verify mockConstruction intercepted the DiskUsage constructor.
          // If this fails, a refactor moved DiskUsage creation outside KafkaStoreIngestionService.
          assertFalse(
              mockedDiskUsage.constructed().isEmpty(),
              "mockConstruction did not intercept any DiskUsage construction — "
                  + "DiskUsage may no longer be created in the DaVinci client path");

          // Validate entries
          for (int i = 1; i <= 1; i++) {
            String key = Integer.toString(i);
            Object value = daVinciClient.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }

          // Deterministically trigger disk full — every subsequent isDiskFull() call returns true
          LOGGER.info("Flipping simulateDiskFull to true before v2 push");
          simulateDiskFull.set(true);

          // Run a push that will trigger disk full in DaVinci
          inputDir = getTempDataDirectory();
          inputDirPath = "file://" + inputDir.getAbsolutePath();
          TestWriteUtils
              .writeSimpleAvroFileWithStringToStringSchema(inputDir, LARGE_PUSH_RECORD_COUNT, LARGE_PUSH_RECORD_SIZE);
          final Properties vpjPropertiesForV2 = defaultVPJProps(venice, inputDirPath, storeName);

          SentPushJobDetailsTrackerImpl pushJobDetailsTracker = new SentPushJobDetailsTrackerImpl();

          VeniceException exception = expectThrows(
              VeniceException.class,
              () -> runVPJ(vpjPropertiesForV2, 2, controllerClient, Optional.of(pushJobDetailsTracker)));
          assertTrue(
              exception.getMessage()
                  .contains(
                      "status: " + (useDaVinciSpecificExecutionStatusForError
                          ? ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL
                          : ExecutionStatus.ERROR)),
              exception.getMessage());
          assertTrue(
              exception.getMessage()
                  .contains(
                      "Found a failed partition replica in Da Vinci"
                          + (useDaVinciSpecificExecutionStatusForError ? " due to disk threshold reached" : "")),
              exception.getMessage());

          assertEquals(
              pushJobDetailsTracker.getRecordedPushJobDetails()
                  .get(pushJobDetailsTracker.getRecordedPushJobDetails().size() - 1)
                  .getPushJobLatestCheckpoint()
                  .intValue(),
              useDaVinciSpecificExecutionStatusForError
                  ? PushJobCheckpoints.DVC_INGESTION_ERROR_DISK_FULL.getValue()
                  : PushJobCheckpoints.START_JOB_STATUS_POLLING.getValue());
        }
      } finally {
        controllerClient.disableAndDeleteStore(storeName);
      }
    }
  }

  /**
   * Validates that Da Vinci client reads survive a StoreIngestionTask failure on the live version.
   *
   * Flow:
   * 1. Push data via VPJ, DaVinci client subscribes and ingests successfully
   * 2. Verify all records are readable from DaVinci
   * 3. Inject a task-level exception into the live version's StoreIngestionTask via
   *    {@code setLastStoreIngestionException()}
   * 4. The SIT's run loop picks up the exception in {@code checkIngestionProgress()}, which triggers
   *    {@code handleIngestionException()} and reports error for all partitions
   * 5. Since partition futures in {@link VersionBackend} were already completed successfully,
   *    {@code completePartitionExceptionally()} is a no-op ({@link java.util.concurrent.CompletableFuture}
   *    can only be completed once)
   * 6. Verify all records are STILL readable — the SIT failure must not break existing reads
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testReadsFromLiveVersionSurviveSITFailure() throws Exception {
    int recordCount = 10;
    String storeName = Utils.getUniqueString("davinci_sit_failure_read_test");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount);
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    vpjProperties.putAll(
        PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties)) {
      venice.createMetaSystemStore(storeName);
      venice.createPushStatusSystemStore(storeName);

      StoreResponse storeResponse = controllerClient.getStore(storeName);
      assertFalse(storeResponse.isError(), "Store response error: " + storeResponse.getError());
      assertTrue(storeResponse.getStore().isDaVinciPushStatusStoreEnabled());

      // Push v1
      runVPJ(vpjProperties, 1, controllerClient);

      VeniceProperties backendConfig = getDaVinciBackendConfig(true);
      MetricsRepository metricsRepository = new MetricsRepository();
      try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
          d2Client,
          VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
          metricsRepository,
          backendConfig,
          venice)) {

        DaVinciClient<String, Object> daVinciClient = factory.getGenericAvroClient(
            storeName,
            new DaVinciConfig().setIsolated(true).setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK));
        daVinciClient.start();
        daVinciClient.subscribeAll().get(30, TimeUnit.SECONDS);

        // Verify all records are readable after successful ingestion
        for (int i = 1; i <= recordCount; i++) {
          String key = Integer.toString(i);
          Object value = daVinciClient.get(key).get();
          assertNotNull(value, "Key " + key + " should be readable after v1 ingestion");
          assertEquals(value.toString(), "test_name_" + i);
        }
        LOGGER.info("All records verified readable from Da Vinci client before SIT failure injection");

        // Get the live version's StoreIngestionTask and inject a task-level exception
        DaVinciBackend backend = AvroGenericDaVinciClient.getBackend();
        try (ReferenceCounted<VersionBackend> versionRef =
            backend.getStoreOrThrow(storeName).getDaVinciCurrentVersion()) {
          VersionBackend versionBackend = versionRef.get();
          assertNotNull(versionBackend, "Current version backend should not be null");
          String versionTopic = versionBackend.getVersion().kafkaTopicName();
          LOGGER.info("Injecting SIT failure for version topic: {}", versionTopic);

          StoreIngestionTask sit =
              backend.getIngestionBackend().getStoreIngestionService().getStoreIngestionTask(versionTopic);
          assertNotNull(sit, "StoreIngestionTask should exist for " + versionTopic);

          // Inject task-level exception — this causes the SIT's run loop to throw from
          // checkIngestionProgress() and report error for all partitions
          sit.setLastStoreIngestionException(new VeniceException("Injected SIT failure for testing"));
        }

        // Wait for the SIT to process the exception and report errors.
        // Core assertion: reads from the live version must still work after SIT failure.
        // VersionBackend.completePartitionExceptionally() is a no-op on already-completed futures,
        // so the partition remains ready-to-serve and reads should succeed.
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 1; i <= recordCount; i++) {
            String key = Integer.toString(i);
            try {
              Object value = daVinciClient.get(key).get();
              assertNotNull(value, "Key " + key + " should still be readable after SIT failure");
              assertEquals(value.toString(), "test_name_" + i, "Key " + key + " value mismatch after SIT failure");
            } catch (Exception e) {
              throw new AssertionError("Read for key " + key + " failed after SIT failure", e);
            }
          }
        });
        LOGGER.info("All records still readable after SIT failure — test passed");
      } finally {
        controllerClient.disableAndDeleteStore(storeName);
      }
    }
  }
}
