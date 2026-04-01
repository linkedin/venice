package com.linkedin.venice.featurematrix;

import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.StoreConfigurator;
import com.linkedin.venice.featurematrix.validation.DataIntegrityValidator;
import com.linkedin.venice.featurematrix.write.BatchPushExecutor;
import com.linkedin.venice.featurematrix.write.StreamingWriteExecutor;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * Runs a single feature matrix test case: creates a store, writes data,
 * validates reads, and cleans up. Shared by both per-TC and shard test classes.
 */
public class TestCaseRunner {
  private static final Logger LOGGER = LogManager.getLogger(TestCaseRunner.class);

  private static final int VERSION_AVAILABILITY_TIMEOUT_SEC = 60;
  private static final int STREAMING_RECORD_START = DEFAULT_USER_DATA_RECORD_COUNT + 1;
  private static final int STREAMING_RECORD_END = DEFAULT_USER_DATA_RECORD_COUNT + 20;

  /**
   * Runs a complete test case lifecycle:
   * 1. Generate input data (with chunking-aware record sizes)
   * 2. Create and configure store
   * 3. Push data (batch/streaming based on topology)
   * 4. Handle deferred version swap if enabled
   * 5. Validate reads via DataIntegrityValidator
   * 6. Clean up store
   */
  public static void runTestCase(TestCaseConfig config, FeatureMatrixClusterSetup clusterSetup) throws Exception {
    LOGGER.info("=== Running test case: {} ===", config.getTestName());

    VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster = clusterSetup.getMultiRegionCluster();
    String parentControllerUrl = clusterSetup.getParentControllerUrl();
    String clusterName = clusterSetup.getClusterName();

    // 1. Write batch data to Avro file
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = BatchPushExecutor.writeTestInputData(inputDir, config);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

    // 2. Create and configure store via parent controller
    String storeName = Utils.getUniqueString("fm-store");

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl)) {
      StoreConfigurator.createAndConfigureStore(
          parentControllerClient,
          clusterName,
          config,
          keySchemaStr,
          valueSchemaStr,
          storeName);

      UpdateStoreQueryParams extraParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setBatchGetLimit(2000);
      parentControllerClient.updateStore(storeName, extraParams);

      LOGGER.info("Store {} created with config TC{}", storeName, config.getTestCaseId());

      // 3. Handle data writes based on topology
      if (config.getTopology() == Topology.NEARLINE_ONLY) {
        LOGGER.info("Executing empty push for nearline-only store {}", storeName);
        VersionCreationResponse emptyPushResponse =
            parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1000);
        Assert.assertFalse(
            emptyPushResponse.isError(),
            "Empty push failed for " + storeName + ": " + emptyPushResponse.getError());

        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(storeName, emptyPushResponse.getVersion()),
            parentControllerClient,
            120,
            TimeUnit.SECONDS);

        StreamingWriteExecutor
            .executeStreamingWrite(storeName, multiRegionCluster, 0, STREAMING_RECORD_START, STREAMING_RECORD_END);
      } else {
        BatchPushExecutor.executeBatchPush(storeName, config, multiRegionCluster, inputDir);

        if (config.isTargetRegionPush() && config.isDeferredSwap()) {
          LOGGER.info("Waiting for deferred version swap (target region) for store {}", storeName);
          TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
            Map<String, Integer> coloVersions =
                parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();
            for (Map.Entry<String, Integer> entry: coloVersions.entrySet()) {
              Assert.assertTrue(entry.getValue() > 0, "Region " + entry.getKey() + " has no current version yet");
            }
          });
        } else if (config.isDeferredSwap()) {
          LOGGER.info("Manually triggering deferred version swap for store {}", storeName);
          parentControllerClient.rollForwardToFutureVersion(storeName, "");
          waitForStoreVersion(parentControllerClient, storeName);
        } else {
          waitForStoreVersion(parentControllerClient, storeName);
        }

        if (config.getTopology() == Topology.HYBRID) {
          StreamingWriteExecutor
              .executeStreamingWrite(storeName, multiRegionCluster, 0, STREAMING_RECORD_START, STREAMING_RECORD_END);
        }

        // W12: TTL repush — KIF repush on top of the initial batch push version
        if (config.isTtlRepush()) {
          LOGGER.info("Executing TTL repush for store {}", storeName);
          BatchPushExecutor.executeTtlRepush(storeName, clusterName, multiRegionCluster);
          waitForStoreVersion(parentControllerClient, storeName);
        }
      }

      // 4. Validate reads
      if (config.getClientType() != ClientType.DA_VINCI) {
        validateReads(config, storeName, clusterSetup);
      } else {
        LOGGER.info("Skipping DaVinci client validation for TC{} (not yet wired)", config.getTestCaseId());
      }

      // 5. Cleanup
      parentControllerClient.disableAndDeleteStore(storeName);

      LOGGER.info("=== Test case PASSED: {} ===", config.getTestName());
    }
  }

  private static void validateReads(TestCaseConfig config, String storeName, FeatureMatrixClusterSetup clusterSetup)
      throws Exception {
    String routerUrl = clusterSetup.getRandomRouterURL();

    try (AvroGenericStoreClient<String, Object> client = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {

      // Batch data validation (single get + batch get)
      if (config.getTopology() != Topology.NEARLINE_ONLY) {
        DataIntegrityValidator.validateSingleGet(client, config);
        DataIntegrityValidator.validateBatchGet(client, config);
      }

      // Streaming data validation (hybrid and nearline-only)
      if (config.getTopology() == Topology.HYBRID || config.getTopology() == Topology.NEARLINE_ONLY) {
        DataIntegrityValidator.validateStreamingData(client, config, STREAMING_RECORD_START, STREAMING_RECORD_END);
      }
    }
  }

  private static void waitForStoreVersion(ControllerClient controllerClient, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(VERSION_AVAILABILITY_TIMEOUT_SEC, TimeUnit.SECONDS, true, true, () -> {
      int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
      Assert.assertTrue(currentVersion > 0, "Store " + storeName + " has no current version yet");
    });
  }
}
