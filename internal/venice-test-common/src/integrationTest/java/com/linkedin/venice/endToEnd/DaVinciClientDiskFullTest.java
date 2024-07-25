package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_DISCOVERY_D2_SERVICE;
import static com.linkedin.venice.ConfigKeys.D2_ZK_HOSTS_ADDRESS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_NO_REPORT_RETRY_MAX_ATTEMPTS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DISK_FULL_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobCheckpoints.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobCheckpoints.START_JOB_STATUS_POLLING;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.PUSH_JOB_STATUS_UPLOAD_ENABLE;
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
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.SentPushJobDetailsTracker;
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
  private VeniceClusterWrapper venice;
  private D2Client d2Client;
  private final int largePushRecordCount = 1000;
  private final int largePushRecordMinSize = 100000;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 10L);
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

  /** find the disk threshold to be configured fail on a push based on given parameters */
  private double getDiskFullThreshold(int recordCount, int recordSizeMin) throws IOException {
    FileStore disk = Files.getFileStore(Paths.get(FileUtils.getTempDirectoryPath()));
    long totalSpaceBytes = disk.getTotalSpace();
    long usableSpaceBytes = disk.getUsableSpace();

    long recordSizeBytes = (long) recordSizeMin * recordCount;
    long freeSpaceBytesRemainingAfterUsage = usableSpaceBytes - recordSizeBytes;
    double diskFullThreshold = 1 - (double) freeSpaceBytesRemainingAfterUsage / totalSpaceBytes;
    LOGGER.info(
        "totalSpaceBytes: {}, usableSpaceBytes: {}, recordSizeBytes: {}, freeSpaceBytesRemainingAfterUsage: {}, diskFullThreshold: {}",
        totalSpaceBytes,
        usableSpaceBytes,
        recordSizeBytes,
        freeSpaceBytesRemainingAfterUsage,
        diskFullThreshold);
    return diskFullThreshold;
  }

  private VeniceProperties getDaVinciBackendConfig(boolean useDaVinciSpecificExecutionStatusForError)
      throws IOException {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder venicePropertyBuilder = new PropertyBuilder();

    venicePropertyBuilder.put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(D2_ZK_HOSTS_ADDRESS, venice.getZk().getAddress())
        .put(CLUSTER_DISCOVERY_D2_SERVICE, VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .put(USE_DA_VINCI_SPECIFIC_EXECUTION_STATUS_FOR_ERROR, useDaVinciSpecificExecutionStatusForError)
        .put(SERVER_DISK_FULL_THRESHOLD, getDiskFullThreshold(largePushRecordCount, largePushRecordMinSize));
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

  @Test(timeOut = TEST_TIMEOUT, dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testDaVinciDiskFullFailure(boolean useDaVinciSpecificExecutionStatusForError) throws Exception {
    String storeName = Utils.getUniqueString("davinci_disk_full_test");
    // Test a small push
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 1, 10);
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
      VeniceProperties backendConfig = getDaVinciBackendConfig(useDaVinciSpecificExecutionStatusForError);
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
        vpjPropertiesForV2.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, "true");

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
                ? DVC_INGESTION_ERROR_DISK_FULL.getValue()
                : START_JOB_STATUS_POLLING.getValue());
      } finally {
        controllerClient.disableAndDeleteStore(storeName);
      }
    }
  }
}
