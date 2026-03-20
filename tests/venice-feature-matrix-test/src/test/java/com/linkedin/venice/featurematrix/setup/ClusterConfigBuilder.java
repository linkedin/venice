package com.linkedin.venice.featurematrix.setup;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.RoutingStrategy;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Builds cluster configuration Properties from Server (S), Router (RT), and Controller (C) dimensions.
 * These configs are set at cluster startup time and cannot change per-store.
 */
public class ClusterConfigBuilder {
  private static final Logger LOGGER = LogManager.getLogger(ClusterConfigBuilder.class);

  /**
   * Builds server properties from the S-dimensions of a test case.
   */
  public static Properties buildServerProperties(TestCaseConfig config) {
    Properties props = new Properties();

    // S1: Parallel Batch Get
    props.setProperty(SERVER_ENABLE_PARALLEL_BATCH_GET, String.valueOf(config.isParallelBatchGet()));

    // S2: Fast Avro
    props.setProperty(SERVER_COMPUTE_FAST_AVRO_ENABLED, String.valueOf(config.isFastAvro()));

    // S3: AA/WC Parallel Processing
    if (config.isAawcParallel()) {
      props.setProperty(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, "true");
    }

    // S4: Blob Transfer
    props.setProperty(ENABLE_BLOB_TRANSFER, String.valueOf(config.isBlobTransfer()));

    // S5: Quota Enforcement
    props.setProperty(SERVER_QUOTA_ENFORCEMENT_ENABLED, String.valueOf(config.isQuotaEnforcement()));

    // S6: Adaptive Throttler
    props.setProperty(SERVER_ADAPTIVE_THROTTLER_ENABLED, String.valueOf(config.isAdaptiveThrottler()));

    LOGGER.info(
        "Built server properties: parallelBatchGet={}, fastAvro={}, aawcParallel={}, blobTransfer={}, quotaEnforcement={}, adaptiveThrottler={}",
        config.isParallelBatchGet(),
        config.isFastAvro(),
        config.isAawcParallel(),
        config.isBlobTransfer(),
        config.isQuotaEnforcement(),
        config.isAdaptiveThrottler());

    return props;
  }

  /**
   * Builds parent controller properties from the C-dimensions of a test case.
   */
  public static Properties buildParentControllerProperties(TestCaseConfig config) {
    Properties props = new Properties();

    // C1: AA as default for hybrid
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE,
        String.valueOf(config.isAaDefaultHybrid()));

    // C2: WC auto for hybrid + AA
    props.setProperty(
        ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES,
        String.valueOf(config.isWcAutoHybridAA()));

    // C3: Incremental push auto for hybrid + AA
    props.setProperty(
        ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES,
        String.valueOf(config.isIncPushAutoHybridAA()));

    // C4: Separate RT topic auto for incremental push
    props.setProperty(
        ENABLE_SEPARATE_REAL_TIME_TOPIC_FOR_STORE_WITH_INCREMENTAL_PUSH,
        String.valueOf(config.isSeparateRTAutoIncPush()));

    // C5: Deferred version swap service
    props.setProperty(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, String.valueOf(config.isDeferredSwapService()));
    if (config.isDeferredSwapService()) {
      props.setProperty(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, "1000");
    }

    // C6: Schema validation
    if (config.isSchemaValidation()) {
      props.setProperty(CONTROLLER_SCHEMA_VALIDATION_ENABLED, "true");
    }

    // C7: Backup version cleanup
    if (config.isBackupVersionCleanup()) {
      props.setProperty(CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS, String.valueOf(3600000L));
    }

    // C8: System store auto-materialization
    props.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(config.isSystemStoreAutoMat()));
    props.setProperty(
        CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE,
        String.valueOf(config.isSystemStoreAutoMat()));
    if (config.isSystemStoreAutoMat()) {
      props.setProperty(PUSH_STATUS_STORE_ENABLED, "true");
    }

    // C9: Superset schema generation
    if (config.isSupersetSchemaGen()) {
      props.setProperty(CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED, "true");
    }

    LOGGER.info(
        "Built parent controller properties: aaDefaultHybrid={}, wcAutoHybridAA={}, incPushAutoHybridAA={}, separateRTAutoIncPush={}, deferredSwapService={}, schemaValidation={}, backupVersionCleanup={}, systemStoreAutoMat={}, supersetSchemaGen={}",
        config.isAaDefaultHybrid(),
        config.isWcAutoHybridAA(),
        config.isIncPushAutoHybridAA(),
        config.isSeparateRTAutoIncPush(),
        config.isDeferredSwapService(),
        config.isSchemaValidation(),
        config.isBackupVersionCleanup(),
        config.isSystemStoreAutoMat(),
        config.isSupersetSchemaGen());

    return props;
  }

  /**
   * Builds child controller properties (typically same as parent for test purposes).
   */
  public static Properties buildChildControllerProperties(TestCaseConfig config) {
    Properties props = new Properties();

    // Mirror relevant parent settings to child controller
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE,
        String.valueOf(config.isAaDefaultHybrid()));
    props.setProperty(
        ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES,
        String.valueOf(config.isWcAutoHybridAA()));
    props.setProperty(
        ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES,
        String.valueOf(config.isIncPushAutoHybridAA()));

    return props;
  }

  /**
   * Builds router properties from the RT-dimensions of a test case.
   * Note: Router properties are typically set via VeniceClusterCreateOptions, not directly
   * on VeniceMultiRegionClusterCreateOptions. For this test framework, we pass them
   * through server properties or as additional cluster config.
   */
  public static Properties buildRouterProperties(TestCaseConfig config) {
    Properties props = new Properties();

    // RT1: Read Throttling
    props.setProperty(ROUTER_ENABLE_READ_THROTTLING, String.valueOf(config.isReadThrottling()));

    // RT2: Early Throttle
    if (config.isEarlyThrottle()) {
      props.setProperty(ROUTER_EARLY_THROTTLE_ENABLED, "true");
    }

    // RT3: Smart Long-tail Retry
    props.setProperty(ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, String.valueOf(config.isSmartLongTailRetry()));

    // RT4: Routing Strategy
    if (config.getRoutingStrategy() == RoutingStrategy.HELIX_ASSISTED) {
      props.setProperty(ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY, "LEAST_LOADED");
    }

    // RT5: Latency-based Routing
    if (config.isLatencyBasedRouting()) {
      props.setProperty(ROUTER_LATENCY_BASED_ROUTING_ENABLED, "true");
    }

    // RT6: Client Decompression
    props.setProperty(ROUTER_CLIENT_DECOMPRESSION_ENABLED, String.valueOf(config.isClientDecompression()));

    // RT7: HTTP/2 Inbound
    if (config.isHttp2Inbound()) {
      props.setProperty(ROUTER_HTTP2_INBOUND_ENABLED, "true");
    }

    // RT8: Connection Warming
    if (config.isConnectionWarming()) {
      props.setProperty(ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED, "true");
    }

    LOGGER.info(
        "Built router properties: readThrottling={}, earlyThrottle={}, smartLongTailRetry={}, routingStrategy={}, latencyBasedRouting={}, clientDecompression={}, http2Inbound={}, connectionWarming={}",
        config.isReadThrottling(),
        config.isEarlyThrottle(),
        config.isSmartLongTailRetry(),
        config.getRoutingStrategy(),
        config.isLatencyBasedRouting(),
        config.isClientDecompression(),
        config.isHttp2Inbound(),
        config.isConnectionWarming());

    return props;
  }
}
