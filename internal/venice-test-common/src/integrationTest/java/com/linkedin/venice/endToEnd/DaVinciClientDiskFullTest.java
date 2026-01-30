package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
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
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientDiskFullTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientDiskFullTest.class);
  private static final int TEST_TIMEOUT = 180_000;
  // Maximum record size allowed by VPJ is ~950KB, use 900KB to be safe
  private static final int MAX_RECORD_SIZE = 900_000;
  // Minimum data size to write (100MB) - actual size may be larger based on disk space
  private static final long MIN_DATA_SIZE_BYTES = 100_000_000L;
  private VeniceClusterWrapper venice;
  private D2Client d2Client;
  // Calculated dynamically based on disk space to ensure we exceed DiskUsage reserve
  private int largePushRecordCount;
  private int largePushRecordMinSize;

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

  /**
   * Calculate the data size and threshold needed to trigger disk full during the large push.
   *
   * DiskUsage has a reserve mechanism: reserveSpaceBytes = 0.001 * freeSpaceBytesRequired
   * The actual disk check only happens after writing reserveSpaceBytes worth of data.
   * So we need: dataSize > reserveSpaceBytes
   *
   * Since freeSpaceBytesRequired ≈ usableSpaceBytes, we need: dataSize > 0.001 * usableSpaceBytes
   * We use 0.3% of usable space (3x the minimum) for safety margin.
   *
   * This method sets {@link #largePushRecordCount} and {@link #largePushRecordMinSize},
   * and returns the threshold.
   */
  private double calculateDataSizeAndThreshold() throws IOException {
    FileStore disk = Files.getFileStore(Paths.get(FileUtils.getTempDirectoryPath()));
    long totalSpaceBytes = disk.getTotalSpace();
    long usableSpaceBytes = disk.getUsableSpace();

    // Calculate minimum data size needed to exceed DiskUsage reserve (0.1% of freeSpaceBytesRequired)
    // Use 0.3% of usable space for safety margin (3x the reserve)
    long minRequiredDataSize = (long) (usableSpaceBytes * 0.003);
    // Use at least MIN_DATA_SIZE_BYTES, but scale up if disk is very large
    long dataSizeBytes = Math.max(MIN_DATA_SIZE_BYTES, minRequiredDataSize);

    // Calculate record count and size, respecting MAX_RECORD_SIZE limit
    // Use MAX_RECORD_SIZE and calculate how many records we need
    largePushRecordMinSize = MAX_RECORD_SIZE;
    largePushRecordCount = (int) Math.max(1, dataSizeBytes / largePushRecordMinSize);
    long actualDataSize = (long) largePushRecordMinSize * largePushRecordCount;

    // Set threshold so disk becomes "full" after writing half the data
    long marginBytes = actualDataSize / 2;
    // freeSpaceBytesRequired = (1 - threshold) * totalSpaceBytes = usableSpaceBytes - marginBytes
    // threshold = 1 - (usableSpaceBytes - marginBytes) / totalSpaceBytes
    double diskFullThreshold = 1.0 - (double) (usableSpaceBytes - marginBytes) / totalSpaceBytes;
    diskFullThreshold = Math.max(0.01, Math.min(0.99, diskFullThreshold));

    LOGGER.info(
        "Disk: totalSpace={}, usableSpace={}, minRequiredDataSize={}, actualDataSize={}, recordCount={}, recordSize={}, threshold={}",
        totalSpaceBytes,
        usableSpaceBytes,
        minRequiredDataSize,
        actualDataSize,
        largePushRecordCount,
        largePushRecordMinSize,
        diskFullThreshold);
    return diskFullThreshold;
  }

  private VeniceProperties getDaVinciBackendConfig(
      boolean useDaVinciSpecificExecutionStatusForError,
      Boolean isD2ClientEnabled) throws IOException {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder venicePropertyBuilder = new PropertyBuilder();

    venicePropertyBuilder.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(D2_ZK_HOSTS_ADDRESS, venice.getZk().getAddress())
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .put(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, useDaVinciSpecificExecutionStatusForError)
        .put(SERVER_DISK_FULL_THRESHOLD, calculateDataSizeAndThreshold())
        .put(SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED, isD2ClientEnabled);
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

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "Two-True-and-False")
  public void testDaVinciDiskFullFailure(boolean useDaVinciSpecificExecutionStatusForError, Boolean isD2ClientEnabled)
      throws Exception {
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

      // Spin up DaVinci client
      VeniceProperties backendConfig =
          getDaVinciBackendConfig(useDaVinciSpecificExecutionStatusForError, isD2ClientEnabled);
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

        // Validate entries
        for (int i = 1; i <= 1; i++) {
          String key = Integer.toString(i);
          Object value = daVinciClient.get(key).get();
          assertNotNull(value, "Key " + i + " should not be missing!");
        }

        // Run a bigger push and the push should fail
        inputDir = getTempDataDirectory();
        inputDirPath = "file://" + inputDir.getAbsolutePath();
        TestWriteUtils
            .writeSimpleAvroFileWithStringToStringSchema(inputDir, largePushRecordCount, largePushRecordMinSize);
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
      } finally {
        controllerClient.disableAndDeleteStore(storeName);
      }
    }
  }
}
