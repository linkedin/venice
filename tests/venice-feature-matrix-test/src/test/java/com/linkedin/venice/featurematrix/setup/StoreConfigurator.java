package com.linkedin.venice.featurematrix.setup;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Compression;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Creates and configures Venice stores based on Write-path (W) dimension values.
 * Uses UpdateStoreQueryParams to set store properties via the controller API.
 */
public class StoreConfigurator {
  private static final Logger LOGGER = LogManager.getLogger(StoreConfigurator.class);

  private static final long HYBRID_REWIND_SECONDS = 60L;
  private static final long HYBRID_OFFSET_LAG = 2L;
  private static final int DEFAULT_PARTITION_COUNT = 2;

  /**
   * Creates a new store configured according to the W-dimensions of the test case.
   *
   * @param controllerClient controller client for store operations
   * @param clusterName the Venice cluster name
   * @param config the test case configuration
   * @param keySchemaStr Avro key schema
   * @param valueSchemaStr Avro value schema
   * @param storeName the store name to use
   */
  public static void createAndConfigureStore(
      ControllerClient controllerClient,
      String clusterName,
      TestCaseConfig config,
      String keySchemaStr,
      String valueSchemaStr,
      String storeName) {
    LOGGER.info("Creating store {} for test case {}", storeName, config.getTestCaseId());

    // Create the store
    NewStoreResponse response =
        controllerClient.createNewStore(storeName, "feature-matrix-test", keySchemaStr, valueSchemaStr);
    if (response.isError()) {
      throw new RuntimeException("Failed to create store " + storeName + ": " + response.getError());
    }

    // Build update params from W-dimensions
    UpdateStoreQueryParams params = buildUpdateParams(config);
    controllerClient.updateStore(storeName, params);

    LOGGER.info("Store {} configured successfully for test case {}", storeName, config.getTestCaseId());
  }

  /**
   * Builds UpdateStoreQueryParams from the W-dimensions of a test case.
   */
  public static UpdateStoreQueryParams buildUpdateParams(TestCaseConfig config) {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();

    // W1: Topology - configure hybrid if needed
    if (config.getTopology() == Topology.HYBRID || config.getTopology() == Topology.NEARLINE_ONLY) {
      params.setHybridRewindSeconds(HYBRID_REWIND_SECONDS);
      params.setHybridOffsetLagThreshold(HYBRID_OFFSET_LAG);
    }

    // W2: Native Replication
    params.setNativeReplicationEnabled(config.isNativeReplication());

    // W3: Active-Active
    params.setActiveActiveReplicationEnabled(config.isActiveActive());

    // W4: Write Compute
    params.setWriteComputationEnabled(config.isWriteCompute());

    // W5: Chunking
    params.setChunkingEnabled(config.isChunking());

    // W6: RMD Chunking
    params.setRmdChunkingEnabled(config.isRmdChunking());

    // W7: Incremental Push
    params.setIncrementalPushEnabled(config.isIncrementalPush());

    // W8: Compression
    params.setCompressionStrategy(toCompressionStrategy(config.getCompression()));

    // W9: Deferred Version Swap - handled at push time, not store level

    // W10: Target Region Push - handled at push time

    // W12: TTL Repush - handled at push time

    // W13: Separate RT Topic
    if (config.isSeparateRTTopic()) {
      params.setSeparateRealTimeTopicEnabled(true);
    }

    // Read Compute (R2)
    params.setReadComputationEnabled(config.isReadCompute());

    // Set partition count
    params.setPartitionCount(DEFAULT_PARTITION_COUNT);

    return params;
  }

  private static CompressionStrategy toCompressionStrategy(Compression compression) {
    switch (compression) {
      case NO_OP:
        return CompressionStrategy.NO_OP;
      case GZIP:
        return CompressionStrategy.GZIP;
      case ZSTD_WITH_DICT:
        return CompressionStrategy.ZSTD_WITH_DICT;
      default:
        return CompressionStrategy.NO_OP;
    }
  }

  /**
   * Deletes a store (cleanup after test case).
   */
  public static void deleteStore(ControllerClient controllerClient, String storeName) {
    LOGGER.info("Deleting store {}", storeName);
    controllerClient.disableAndDeleteStore(storeName);
  }
}
