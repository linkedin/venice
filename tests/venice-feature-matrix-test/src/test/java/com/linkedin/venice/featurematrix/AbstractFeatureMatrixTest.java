package com.linkedin.venice.featurematrix;

import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener;
import com.linkedin.venice.featurematrix.setup.StoreConfigurator;
import com.linkedin.venice.featurematrix.write.BatchPushExecutor;
import com.linkedin.venice.featurematrix.write.StreamingWriteExecutor;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Base class for generated per-TC feature matrix tests.
 *
 * Each generated subclass overrides {@link #getConfig()} to return its specific
 * {@link TestCaseConfig}. Gradle distributes subclasses across JVMs via
 * {@code forkEvery=1} + {@code maxParallelForks=4}.
 *
 * Test flow:
 * 1. Create multi-region cluster (BeforeClass)
 * 2. Create store, write data, validate reads (Test)
 * 3. Tear down cluster (AfterClass)
 */
@Listeners(FeatureMatrixReportListener.class)
@Test
public abstract class AbstractFeatureMatrixTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractFeatureMatrixTest.class);

  private static final int VERSION_AVAILABILITY_TIMEOUT_SEC = 60;
  private static final int STREAMING_RECORD_START = DEFAULT_USER_DATA_RECORD_COUNT + 1;
  private static final int STREAMING_RECORD_END = DEFAULT_USER_DATA_RECORD_COUNT + 20;

  private FeatureMatrixClusterSetup clusterSetup;

  public abstract TestCaseConfig getConfig();

  @BeforeClass
  public void setupCluster() {
    TestCaseConfig config = getConfig();
    LOGGER.info("Setting up multi-region cluster for TC{}", config.getTestCaseId());
    clusterSetup = new FeatureMatrixClusterSetup(config);
    clusterSetup.create();
    LOGGER.info("Multi-region cluster setup complete for TC{}", config.getTestCaseId());
  }

  @Test(timeOut = 300_000)
  public void testFeatureCombination() throws Exception {
    TestCaseConfig config = getConfig();
    LOGGER.info("=== Running test case: {} ===", config.getTestName());

    VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster = clusterSetup.getMultiRegionCluster();
    String parentControllerUrl = clusterSetup.getParentControllerUrl();
    String clusterName = clusterSetup.getClusterName();

    // 1. Write batch data to Avro file
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
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
          // Combined target-region push with deferred swap: the DeferredVersionSwapService
          // handles replication and version swap automatically.
          LOGGER.info("Waiting for deferred version swap (target region) for store {}", storeName);
          TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, true, () -> {
            Map<String, Integer> coloVersions =
                parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();
            for (Map.Entry<String, Integer> entry: coloVersions.entrySet()) {
              Assert.assertTrue(entry.getValue() > 0, "Region " + entry.getKey() + " has no current version yet");
            }
          });
        } else if (config.isDeferredSwap()) {
          // Deferred swap only (no target region): VPJ defers the swap, so we must
          // manually trigger it via rollForwardToFutureVersion. The push is already
          // complete (BatchPushExecutor is synchronous), so rollForward is safe.
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
      }

      // 4. Validate reads (skip DaVinci for now)
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

      if (config.getTopology() != Topology.NEARLINE_ONLY) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 1; i <= 10; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            Assert.assertNotNull(value, "Single get null for key " + key + " TC" + config.getTestCaseId());
            Assert.assertTrue(
                value.toString().contains("first_name_" + i),
                "Single get wrong for key " + key + " TC" + config.getTestCaseId() + " got: " + value);
          }
        });
        LOGGER.info("Single get validation passed for batch data");
      }

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

      if (config.getTopology() == Topology.HYBRID || config.getTopology() == Topology.NEARLINE_ONLY) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (int i = STREAMING_RECORD_START; i <= STREAMING_RECORD_START + 5; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            Assert.assertNotNull(value, "Streaming data null for key " + key + " TC" + config.getTestCaseId());
            Assert.assertTrue(
                value.toString().contains("first_name_" + i),
                "Streaming data wrong for key " + key + " TC" + config.getTestCaseId() + " got: " + value);
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

  @AfterClass(alwaysRun = true)
  public void tearDownCluster() {
    TestCaseConfig config = getConfig();
    LOGGER.info("Tearing down cluster for TC{}", config.getTestCaseId());
    if (clusterSetup != null) {
      clusterSetup.tearDown();
    }
  }
}
