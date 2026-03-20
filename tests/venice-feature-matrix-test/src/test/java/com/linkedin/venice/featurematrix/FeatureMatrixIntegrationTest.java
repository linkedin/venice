package com.linkedin.venice.featurematrix;

import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.PictModelParser;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener;
import com.linkedin.venice.featurematrix.setup.StoreConfigurator;
import com.linkedin.venice.featurematrix.write.BatchPushExecutor;
import com.linkedin.venice.featurematrix.write.StreamingWriteExecutor;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Main integration test class for the Venice Feature Matrix.
 *
 * Uses TestNG Factory pattern to create one test instance per unique (S, RT, C)
 * cluster configuration. Each instance runs all (W, R) test cases that share
 * the same infrastructure settings.
 *
 * Uses VeniceTwoLayerMultiRegionMultiClusterWrapper to support multi-region features
 * (native replication, active-active, target region push).
 *
 * Test flow per invocation:
 * 1. Create store with W-dimension flags
 * 2. Write data (batch push, streaming writes as applicable)
 * 3. Validate reads (single get, batch get)
 * 4. Cleanup store
 */
@Listeners(FeatureMatrixReportListener.class)
public class FeatureMatrixIntegrationTest {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixIntegrationTest.class);

  private static final String TEST_CASES_RESOURCE = "generated-test-cases.tsv";
  private static final int VERSION_AVAILABILITY_TIMEOUT_SEC = 60;
  private static final int STREAMING_RECORD_START = DEFAULT_USER_DATA_RECORD_COUNT + 1;
  private static final int STREAMING_RECORD_END = DEFAULT_USER_DATA_RECORD_COUNT + 20;

  private final String clusterConfigKey;
  private final List<TestCaseConfig> testCases;
  private FeatureMatrixClusterSetup clusterSetup;

  @Factory(dataProvider = "clusterConfigs")
  public FeatureMatrixIntegrationTest(String clusterConfigKey, List<TestCaseConfig> testCases) {
    this.clusterConfigKey = clusterConfigKey;
    this.testCases = testCases;
  }

  @DataProvider(name = "clusterConfigs")
  public static Object[][] clusterConfigs() throws IOException {
    List<TestCaseConfig> allTestCases = PictModelParser.parseFromClasspath(TEST_CASES_RESOURCE);
    Map<String, List<TestCaseConfig>> groups = PictModelParser.groupByClusterConfig(allTestCases);

    Object[][] result = new Object[groups.size()][2];
    int i = 0;
    for (Map.Entry<String, List<TestCaseConfig>> entry: groups.entrySet()) {
      result[i][0] = entry.getKey();
      result[i][1] = entry.getValue();
      i++;
    }

    LOGGER.info("Created {} cluster config groups from {} total test cases", groups.size(), allTestCases.size());
    return result;
  }

  @BeforeClass
  public void setupCluster() {
    LOGGER.info("Setting up multi-region cluster for config: {}", clusterConfigKey);

    TestCaseConfig representative = testCases.get(0);
    clusterSetup = new FeatureMatrixClusterSetup(representative);
    clusterSetup.create();

    LOGGER.info("Multi-region cluster setup complete. Will run {} test cases.", testCases.size());
  }

  @DataProvider(name = "storeAndClientMatrix")
  public Object[][] storeAndClientMatrix() {
    Object[][] result = new Object[testCases.size()][1];
    for (int i = 0; i < testCases.size(); i++) {
      result[i][0] = testCases.get(i);
    }
    return result;
  }

  @Test(dataProvider = "storeAndClientMatrix", timeOut = 300_000)
  public void testFeatureCombination(TestCaseConfig config) throws Exception {
    LOGGER.info("=== Running test case: {} ===", config.getTestName());

    VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster = clusterSetup.getMultiRegionCluster();
    String parentControllerUrl = clusterSetup.getParentControllerUrl();
    String clusterName = clusterSetup.getClusterName();

    // 1. Write batch data to Avro file
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    // 2. Create and configure store via parent controller
    String storeName = Utils.getUniqueString("fm-store");

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl)) {
      // Create the store
      StoreConfigurator.createAndConfigureStore(
          parentControllerClient,
          clusterName,
          config,
          keySchemaStr,
          valueSchemaStr,
          storeName);

      // Set storage quota and batch get limit
      UpdateStoreQueryParams extraParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setBatchGetLimit(2000);
      parentControllerClient.updateStore(storeName, extraParams);

      LOGGER.info("Store {} created with config TC{}", storeName, config.getTestCaseId());

      // 3. Handle data writes based on topology
      if (config.getTopology() == Topology.NEARLINE_ONLY) {
        // Nearline-only stores need an empty push to initialize the version before streaming
        LOGGER.info("Executing empty push for nearline-only store {}", storeName);
        VersionCreationResponse emptyPushResponse =
            parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1000);
        Assert.assertFalse(
            emptyPushResponse.isError(),
            "Empty push failed for " + storeName + ": " + emptyPushResponse.getError());

        // Wait for version to be available before streaming
        waitForStoreVersion(parentControllerClient, storeName);

        // Stream data
        StreamingWriteExecutor
            .executeStreamingWrite(storeName, multiRegionCluster, 0, STREAMING_RECORD_START, STREAMING_RECORD_END);
      } else {
        // Batch push (for batch-only and hybrid topologies)
        BatchPushExecutor.executeBatchPush(storeName, config, multiRegionCluster, inputDir);

        // Wait for version to be available
        waitForStoreVersion(parentControllerClient, storeName);

        // Streaming writes (for hybrid)
        if (config.getTopology() == Topology.HYBRID) {
          StreamingWriteExecutor
              .executeStreamingWrite(storeName, multiRegionCluster, 0, STREAMING_RECORD_START, STREAMING_RECORD_END);
        }
      }

      // 4. Validate reads (skip DaVinci for now — requires separate setup)
      if (config.getClientType() != ClientType.DA_VINCI) {
        validateReads(config, storeName);
      } else {
        LOGGER.info("Skipping DaVinci client validation for TC{} (not yet wired)", config.getTestCaseId());
      }

      // 5. Cleanup
      parentControllerClient.disableAndDeleteStore(storeName);

      LOGGER.info("=== Test case PASSED: {} ===", config.getTestName());
    }
  }

  private void validateReads(TestCaseConfig config, String storeName) throws Exception {
    String routerUrl = clusterSetup.getRandomRouterURL();

    try (AvroGenericStoreClient<String, Object> client = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

      // Single get validation for batch data
      if (config.getTopology() != Topology.NEARLINE_ONLY) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 1; i <= 10; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            Assert.assertNotNull(value, "Single get null for key " + key + " TC" + config.getTestCaseId());
            Assert.assertEquals(
                value.toString(),
                DEFAULT_USER_DATA_VALUE_PREFIX + i,
                "Single get wrong for key " + key + " TC" + config.getTestCaseId());
          }
        });
        LOGGER.info("Single get validation passed for batch data");
      }

      // Batch get validation
      if (config.getTopology() != Topology.NEARLINE_ONLY) {
        Set<String> keys = new HashSet<>();
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          keys.add(Integer.toString(i));
        }
        Map<String, Object> results = client.batchGet(keys).get();
        Assert.assertEquals(
            results.size(),
            DEFAULT_USER_DATA_RECORD_COUNT,
            "Batch get wrong count TC" + config.getTestCaseId());
        LOGGER.info("Batch get validation passed for {} keys", results.size());
      }

      // Streaming data validation (for hybrid and nearline-only)
      if (config.getTopology() == Topology.HYBRID || config.getTopology() == Topology.NEARLINE_ONLY) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (int i = STREAMING_RECORD_START; i <= STREAMING_RECORD_START + 5; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            Assert.assertNotNull(value, "Streaming data null for key " + key + " TC" + config.getTestCaseId());
            Assert.assertEquals(
                value.toString(),
                "stream_" + i,
                "Streaming data wrong for key " + key + " TC" + config.getTestCaseId());
          }
        });
        LOGGER.info("Streaming data validation passed");
      }
    }
  }

  private void waitForStoreVersion(ControllerClient controllerClient, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(VERSION_AVAILABILITY_TIMEOUT_SEC, TimeUnit.SECONDS, true, true, () -> {
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      Assert.assertTrue(currentVersion > 0, "Store " + storeName + " has no current version yet");
    });
  }

  @AfterClass
  public void tearDownCluster() {
    LOGGER.info("Tearing down cluster for config: {}", clusterConfigKey);
    if (clusterSetup != null) {
      clusterSetup.tearDown();
    }
  }
}
