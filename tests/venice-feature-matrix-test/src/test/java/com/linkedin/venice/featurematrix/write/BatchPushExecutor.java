package com.linkedin.venice.featurematrix.write;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.File;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes batch push (VPJ) and incremental push operations using Venice's integration test utilities.
 */
public class BatchPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(BatchPushExecutor.class);

  /**
   * Executes a batch push using the Spark DataWriter engine.
   */
  public static void executeBatchPush(
      String storeName,
      TestCaseConfig config,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      File inputDir) {
    LOGGER.info(
        "Executing batch push for store={}, compression={}, deferredSwap={}, targetRegion={}",
        storeName,
        config.getCompression(),
        config.isDeferredSwap(),
        config.isTargetRegionPush());

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = defaultVPJProps(multiRegionCluster, inputDirPath, storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");

    if (config.isTargetRegionPush() && config.isDeferredSwap()) {
      // Combined flag: push to target region, then deferred swap service replicates and swaps
      vpjProps.setProperty(VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, "true");
    } else {
      if (config.isDeferredSwap()) {
        vpjProps.setProperty(VenicePushJobConstants.DEFER_VERSION_SWAP, "true");
      }
      if (config.isTargetRegionPush()) {
        vpjProps.setProperty(VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED, "true");
      }
    }

    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("Batch push completed for store {}", storeName);
  }

  /**
   * Executes an incremental push using VPJ with the incremental push flag.
   */
  public static void executeIncrementalPush(
      String storeName,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      File inputDir) {
    LOGGER.info("Executing incremental push for store={}", storeName);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProps = defaultVPJProps(multiRegionCluster, inputDirPath, storeName);
    vpjProps
        .setProperty(VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
    vpjProps.setProperty(VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED, "true");
    vpjProps.setProperty(VenicePushJobConstants.INCREMENTAL_PUSH, "true");

    IntegrationTestPushUtils.runVPJ(vpjProps);
    LOGGER.info("Incremental push completed for store {}", storeName);
  }
}
