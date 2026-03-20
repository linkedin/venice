package com.linkedin.venice.featurematrix;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.PictModelParser;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.ClusterConfigBuilder;
import com.linkedin.venice.featurematrix.setup.StoreConfigurator;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Smoke test for the Feature Matrix framework.
 *
 * Validates the end-to-end flow: PICT parsing -> cluster setup -> store creation ->
 * batch push -> thin client read. Uses a single-region VeniceClusterWrapper for simplicity.
 *
 * Filters PICT test cases to batch-only + thin-client combinations (max 3 cases)
 * to keep runtime short.
 */
@Test(singleThreaded = true)
public class FeatureMatrixSmokeTest {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixSmokeTest.class);
  private static final int TEST_TIMEOUT_MS = 180_000; // 3 minutes per test
  private static final int MAX_SMOKE_TESTS = 3;
  private static final int VERSION_AVAILABILITY_TIMEOUT_SEC = 30;

  private VeniceClusterWrapper veniceCluster;
  private List<TestCaseConfig> smokeTestCases;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws IOException {
    // Parse PICT-generated test cases
    List<TestCaseConfig> allCases = PictModelParser.parseFromClasspath("generated-test-cases.tsv");
    LOGGER.info("Parsed {} total test cases from PICT output", allCases.size());

    // Filter to batch-only + thin client cases for smoke testing
    smokeTestCases = allCases.stream()
        .filter(tc -> tc.getTopology() == Topology.BATCH_ONLY)
        .filter(tc -> tc.getClientType() == ClientType.THIN)
        .limit(MAX_SMOKE_TESTS)
        .collect(Collectors.toList());

    if (smokeTestCases.isEmpty()) {
      // Fall back to any batch-only case
      smokeTestCases = allCases.stream()
          .filter(tc -> tc.getTopology() == Topology.BATCH_ONLY)
          .limit(MAX_SMOKE_TESTS)
          .collect(Collectors.toList());
    }

    LOGGER.info("Selected {} smoke test cases", smokeTestCases.size());
    Assert.assertFalse(smokeTestCases.isEmpty(), "No suitable smoke test cases found in PICT output");

    // Use the first test case's server config to create the cluster
    TestCaseConfig representative = smokeTestCases.get(0);
    Properties serverProps = ClusterConfigBuilder.buildServerProperties(representative);

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .extraProperties(serverProps)
        .build();

    veniceCluster = ServiceFactory.getVeniceCluster(options);
    LOGGER.info("Venice cluster started: {}", veniceCluster.getClusterName());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @DataProvider(name = "smokeTestCases")
  public Object[][] smokeTestCases() {
    Object[][] result = new Object[smokeTestCases.size()][1];
    for (int i = 0; i < smokeTestCases.size(); i++) {
      result[i][0] = smokeTestCases.get(i);
    }
    return result;
  }

  @Test(dataProvider = "smokeTestCases", timeOut = TEST_TIMEOUT_MS)
  public void testBatchPushAndRead(TestCaseConfig config) throws Exception {
    LOGGER.info("=== Smoke test: {} ===", config.getTestName());

    // 1. Write test data to Avro file
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);

    // 2. Set up store name and VPJ properties
    String storeName = Utils.getUniqueString("fm-smoke");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    // 3. Build store params from test case W-dimensions
    UpdateStoreQueryParams storeParams = StoreConfigurator.buildUpdateParams(config);
    storeParams.setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA);
    storeParams.setBatchGetLimit(2000);

    // 4. Create store with the config
    createStoreForJob(
        veniceCluster.getClusterName(),
        recordSchema.getField("key").schema().toString(),
        recordSchema.getField("value").schema().toString(),
        vpjProps,
        storeParams).close();

    LOGGER.info("Store {} created with config TC{}", storeName, config.getTestCaseId());

    // 5. Run VPJ batch push
    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("VPJ completed for store {}", storeName);

    // 6. Wait for version to be available on routers
    waitForStoreVersion(storeName);

    // 7. Validate reads via thin client
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Single get validation
      for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
        String key = Integer.toString(i);
        Object value = client.get(key).get();
        Assert.assertNotNull(value, "Single get returned null for key " + key + " (TC" + config.getTestCaseId() + ")");
        String expected = DEFAULT_USER_DATA_VALUE_PREFIX + i;
        Assert.assertEquals(
            value.toString(),
            expected,
            "Single get wrong value for key " + key + " (TC" + config.getTestCaseId() + ")");
      }
      LOGGER.info("Single get validation passed for {} keys", DEFAULT_USER_DATA_RECORD_COUNT);

      // Batch get validation
      java.util.Set<String> keys = new java.util.HashSet<>();
      for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
        keys.add(Integer.toString(i));
      }
      java.util.Map<String, Object> results = client.batchGet(keys).get();
      Assert.assertEquals(
          results.size(),
          DEFAULT_USER_DATA_RECORD_COUNT,
          "Batch get returned wrong count (TC" + config.getTestCaseId() + ")");
      LOGGER.info("Batch get validation passed for {} keys", results.size());
    }

    LOGGER.info("=== Smoke test PASSED: TC{} ===", config.getTestCaseId());
  }

  private void waitForStoreVersion(String storeName) {
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(VERSION_AVAILABILITY_TIMEOUT_SEC, TimeUnit.SECONDS, true, true, () -> {
        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        Assert.assertTrue(currentVersion > 0, "Store " + storeName + " has no current version yet");
        veniceCluster.refreshAllRouterMetaData();
      });
    });
  }
}
