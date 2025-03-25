package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertNotNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDaVinciRequestBasedMetaRepository {
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;

  private static final String CLUSTER_NAME = "venice-cluster";
  private VeniceClusterWrapper clusterWrapper;

  // StoreName -> ControllerClient
  // Using map to check which stores are created
  private final Map<String, ControllerClient> controllerClients = new HashMap<>();
  // StoreName -> Directory
  private final Map<String, File> pushJobAvroDataDirs = new HashMap<>();

  private DaVinciConfig daVinciConfig;
  private MetricsRepository dvcMetricsRepo;
  private D2Client daVinciD2RemoteFabric;
  private CachingDaVinciClientFactory daVinciClientFactory;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws IOException {

    VeniceClusterCreateOptions.Builder options = new VeniceClusterCreateOptions.Builder().clusterName(CLUSTER_NAME)
        .numberOfRouters(1)
        .numberOfServers(2)
        .numberOfControllers(2)
        .replicationFactor(2)
        .forkServer(false);
    clusterWrapper = ServiceFactory.getVeniceCluster(options.build());

    // Set up DVC Client Factory
    VeniceProperties backendConfig =
        new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
            .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
            .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
            .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
            .build();
    daVinciConfig = new DaVinciConfig();
    daVinciConfig.setUseRequestBasedMetaRepository(true);
    daVinciD2RemoteFabric = D2TestUtils.getAndStartD2Client(clusterWrapper.getZk().getAddress());
    dvcMetricsRepo = new MetricsRepository();
    daVinciClientFactory = new CachingDaVinciClientFactory(
        daVinciD2RemoteFabric,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        dvcMetricsRepo,
        backendConfig);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {

    // Shutdown remote fabric
    D2ClientUtils.shutdownClient(daVinciD2RemoteFabric);

    // Close client factory
    daVinciClientFactory.close();

    // Close controller clients
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      entry.getValue().close();
    }

    // Close cluster wrapper
    clusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDVCRequestBasedMetaRepositoryStringToString()
      throws IOException, ExecutionException, InterruptedException {

    // Set up store
    String storeName = Utils.getUniqueString("venice-store");
    runPushJob( // String to String
        storeName,
        TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(getPushJobAvroFileDirectory(storeName)));

    try (DaVinciClient<String, Object> storeClient =
        daVinciClientFactory.getAndStartGenericAvroClient(storeName, daVinciConfig)) {
      storeClient.subscribeAll().get();

      int recordCount = TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
      for (int i = 1; i <= recordCount; i++) {
        Assert.assertEquals(
            storeClient.get(Integer.toString(i)).get().toString(),
            TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
      Assert.assertEquals(getMetric(dvcMetricsRepo, "current_version_number.Gauge", storeName), (double) 1);

      // Run new push job
      recordCount = 200;
      runPushJob(
          storeName,
          TestWriteUtils
              .writeSimpleAvroFileWithStringToStringSchema(getPushJobAvroFileDirectory(storeName), recordCount));

      // Verify version swap
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
        Assert.assertEquals(getMetric(dvcMetricsRepo, "current_version_number.Gauge", storeName), (double) 2);
      });

      for (int i = 1; i <= recordCount; i++) {
        Assert.assertEquals(
            storeClient.get(Integer.toString(i)).get().toString(),
            TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX + i);
      }
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDVCRequestBasedMetaRepositoryStringToNameRecord()
      throws ExecutionException, InterruptedException, IOException {

    // Set up store
    String storeName = Utils.getUniqueString("venice-store");
    runPushJob( // String to String
        storeName,
        TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(getPushJobAvroFileDirectory(storeName)));

    try (DaVinciClient<String, Object> storeClient =
        daVinciClientFactory.getAndStartGenericAvroClient(storeName, daVinciConfig)) {
      storeClient.subscribeAll().get();

      int recordCount = TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
      for (int i = 1; i <= recordCount; i++) {
        // Verify storeClient can read
        Assert.assertEquals(
            storeClient.get(Integer.toString(i)).get().toString(),
            TestWriteUtils.renderNameRecord(TestWriteUtils.STRING_TO_NAME_RECORD_V1_SCHEMA, i)
                .get(DEFAULT_VALUE_FIELD_PROP)
                .toString());
      }

      // Verify version
      Assert.assertEquals(getMetric(dvcMetricsRepo, "current_version_number.Gauge", storeName), (double) 1);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testDVCRequestBasedMetaRepositoryStringToNameRecordVersions()
      throws IOException, ExecutionException, InterruptedException {

    // Set up store
    String storeName = Utils.getUniqueString("venice-store");
    runPushJob( // String to String
        storeName,
        TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema(getPushJobAvroFileDirectory(storeName)));

    try (DaVinciClient<String, Object> storeClient =
        daVinciClientFactory.getAndStartGenericAvroClient(storeName, daVinciConfig)) {
      storeClient.subscribeAll().get();

      for (int i = 0; i < TestWriteUtils.countStringToNameRecordSchemas(); i++) {
        Schema schema = TestWriteUtils.getStringToNameRecordSchema(i);
        int currentValueVersion = i + 2;

        // Run new push job with new version
        int recordCount = currentValueVersion * TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
        runPushJob(
            storeName,
            TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordSchema(
                getPushJobAvroFileDirectory(storeName),
                schema,
                recordCount));

        // Verify version swap
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, false, () -> {
          Assert.assertEquals(
              getMetric(dvcMetricsRepo, "current_version_number.Gauge", storeName),
              (double) (currentValueVersion));
        });

        // Verify storeClient can read all
        for (int j = 1; j <= recordCount; j++) {
          Assert.assertEquals(
              storeClient.get(Integer.toString(j)).get().toString(),
              TestWriteUtils.renderNameRecord(schema, j).get(DEFAULT_VALUE_FIELD_PROP).toString());
        }
      }
    } finally {
      deleteStore(storeName);
    }
  }

  private double getMetric(MetricsRepository metricsRepository, String metricName, String storeName) {
    Metric metric = metricsRepository.getMetric("." + storeName + "--" + metricName);
    assertNotNull(metric, "Expected metric " + metricName + " not found.");
    return metric.value();
  }

  private File getPushJobAvroFileDirectory(String storeName) {
    if (!pushJobAvroDataDirs.containsKey(storeName)) {
      pushJobAvroDataDirs.put(storeName, getTempDataDirectory());
    }

    return pushJobAvroDataDirs.get(storeName);
  }

  private void runPushJob(String storeName, Schema schema) {

    ControllerClient controllerClient;
    File dataDir = getPushJobAvroFileDirectory(storeName);
    String dataDirPath = "file:" + dataDir.getAbsolutePath();

    if (!controllerClients.containsKey(storeName)) {
      // Init store
      controllerClient = IntegrationTestPushUtils.createStoreForJob(
          CLUSTER_NAME,
          schema,
          TestWriteUtils.defaultVPJProps(
              clusterWrapper.getVeniceControllers().get(0).getControllerUrl(),
              dataDirPath,
              storeName));
      controllerClients.put(storeName, controllerClient);
    } else {
      controllerClient = controllerClients.get(storeName);

      // Add new schema
      Schema valueSchema = schema.getField(DEFAULT_VALUE_FIELD_PROP).schema();
      SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, valueSchema.toString());
      Assert.assertFalse(schemaResponse.isError(), schemaResponse.getError());
    }

    Properties props =
        TestWriteUtils.defaultVPJProps(controllerClient.getLeaderControllerUrl(), dataDirPath, storeName);
    TestWriteUtils.runPushJob(storeName + "_" + Utils.getUniqueString("push_job"), props);
  }

  private void deleteStore(String storeName) {
    if (!controllerClients.containsKey(storeName)) {
      Assert.fail("Store not created: " + storeName);
    }
    ControllerClient controllerClient = controllerClients.get(storeName);
    controllerClient.disableAndDeleteStore(storeName);
    controllerClient.close();
    controllerClients.remove(storeName);
  }
}
