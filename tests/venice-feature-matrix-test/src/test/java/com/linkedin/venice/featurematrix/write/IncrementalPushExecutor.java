package com.linkedin.venice.featurematrix.write;

import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes incremental push operations against a multi-region cluster.
 * Delegates to {@link BatchPushExecutor#executeIncrementalPush}.
 */
public class IncrementalPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(IncrementalPushExecutor.class);

  /**
   * Executes an incremental push with the provided Avro data directory.
   */
  public static void executeIncrementalPush(
      String storeName,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      File inputDir) {
    LOGGER.info("Executing incremental push for store={}", storeName);
    BatchPushExecutor.executeIncrementalPush(storeName, multiRegionCluster, inputDir);
  }
}
