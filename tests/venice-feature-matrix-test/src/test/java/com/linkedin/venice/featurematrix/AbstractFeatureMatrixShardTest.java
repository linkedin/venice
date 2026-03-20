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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Base class for sharded feature matrix tests.
 *
 * Each generated shard subclass provides a list of {@link TestCaseConfig}s.
 * TCs sharing the same cluster config key are run together on one cluster,
 * avoiding redundant cluster setup/teardown within a shard.
 */
@Listeners(FeatureMatrixReportListener.class)
@Test
public abstract class AbstractFeatureMatrixShardTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractFeatureMatrixShardTest.class);

  private static final int VERSION_AVAILABILITY_TIMEOUT_SEC = 60;
  private static final int STREAMING_RECORD_START = DEFAULT_USER_DATA_RECORD_COUNT + 1;
  private static final int STREAMING_RECORD_END = DEFAULT_USER_DATA_RECORD_COUNT + 20;

  public abstract List<TestCaseConfig> getConfigs();

  @Test(timeOut = 900_000)
  public void runShard() throws Exception {
    List<TestCaseConfig> configs = getConfigs();
    LOGGER.info("=== Starting shard with {} test cases ===", configs.size());

    // Group TCs by cluster config key to reuse clusters
    Map<String, List<TestCaseConfig>> groups = new LinkedHashMap<>();
    for (TestCaseConfig config: configs) {
      groups.computeIfAbsent(config.getClusterConfigKey(), k -> new ArrayList<>()).add(config);
    }

    LOGGER.info("Grouped into {} cluster configs", groups.size());

    int passCount = 0;
    List<String> failures = new ArrayList<>();

    for (Map.Entry<String, List<TestCaseConfig>> group: groups.entrySet()) {
      List<TestCaseConfig> groupConfigs = group.getValue();
      TestCaseConfig representative = groupConfigs.get(0);

      LOGGER
          .info("Setting up cluster for {} TCs (config key hash: {})", groupConfigs.size(), group.getKey().hashCode());

      FeatureMatrixClusterSetup clusterSetup = new FeatureMatrixClusterSetup(representative);
      try {
        clusterSetup.create();

        for (TestCaseConfig config: groupConfigs) {
          try {
            runTestCase(config, clusterSetup);
            passCount++;
            LOGGER.info("=== TC{} PASSED ===", config.getTestCaseId());
          } catch (Exception e) {
            String msg = "TC" + config.getTestCaseId() + ": " + e.getMessage();
            failures.add(msg);
            LOGGER.error("=== TC{} FAILED: {} ===", config.getTestCaseId(), e.getMessage(), e);
          }
        }
      } finally {
        clusterSetup.tearDown();
      }
    }

    LOGGER.info("=== Shard complete: {} passed, {} failed ===", passCount, failures.size());
    if (!failures.isEmpty()) {
      Assert.fail("Shard had " + failures.size() + " failure(s):\n" + String.join("\n", failures));
    }
  }

  private void runTestCase(TestCaseConfig config, FeatureMatrixClusterSetup clusterSetup) throws Exception {
    LOGGER.info("=== Running test case: {} ===", config.getTestName());

    VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster = clusterSetup.getMultiRegionCluster();
    String parentControllerUrl = clusterSetup.getParentControllerUrl();
    String clusterName = clusterSetup.getClusterName();

    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField("key").schema().toString();
    String valueSchemaStr = recordSchema.getField("value").schema().toString();

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

      if (config.getClientType() != ClientType.DA_VINCI) {
        validateReads(config, storeName, clusterSetup);
      } else {
        LOGGER.info("Skipping DaVinci client validation for TC{} (not yet wired)", config.getTestCaseId());
      }

      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }

  private void validateReads(TestCaseConfig config, String storeName, FeatureMatrixClusterSetup clusterSetup)
      throws Exception {
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
}
