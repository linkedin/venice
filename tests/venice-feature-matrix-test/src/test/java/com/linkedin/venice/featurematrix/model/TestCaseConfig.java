package com.linkedin.venice.featurematrix.model;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Represents a single test case configuration with all 42 dimension values.
 * Immutable after construction.
 */
public class TestCaseConfig {
  private final int testCaseId;
  private final Map<DimensionId, String> dimensions;

  // Write path
  private final Topology topology;
  private final boolean nativeReplication;
  private final boolean activeActive;
  private final boolean writeCompute;
  private final boolean chunking;
  private final boolean rmdChunking;
  private final boolean incrementalPush;
  private final Compression compression;
  private final boolean deferredSwap;
  private final boolean targetRegionPush;
  private final PushEngine pushEngine;
  private final boolean ttlRepush;
  private final boolean separateRTTopic;

  // Read path
  private final ClientType clientType;
  private final boolean readCompute;
  private final DaVinciStorageClass daVinciStorage;
  private final RoutingStrategy fastRouting;
  private final boolean longTailRetry;
  private final OptionalOnOff recordTransformer;

  // Server
  private final boolean parallelBatchGet;
  private final boolean fastAvro;
  private final boolean aawcParallel;
  private final boolean blobTransfer;
  private final boolean quotaEnforcement;
  private final boolean adaptiveThrottler;

  // Controller
  private final boolean aaDefaultHybrid;
  private final boolean wcAutoHybridAA;
  private final boolean incPushAutoHybridAA;
  private final boolean separateRTAutoIncPush;
  private final boolean deferredSwapService;
  private final boolean schemaValidation;
  private final boolean backupVersionCleanup;
  private final boolean systemStoreAutoMat;
  private final boolean supersetSchemaGen;

  // Router
  private final boolean readThrottling;
  private final boolean earlyThrottle;
  private final boolean smartLongTailRetry;
  private final RoutingStrategy routingStrategy;
  private final boolean latencyBasedRouting;
  private final boolean clientDecompression;
  private final boolean http2Inbound;
  private final boolean connectionWarming;

  public TestCaseConfig(int testCaseId, Map<DimensionId, String> dimensions) {
    this.testCaseId = testCaseId;
    this.dimensions = Collections.unmodifiableMap(new LinkedHashMap<>(dimensions));

    // Parse write path
    this.topology = parseTopology(get(DimensionId.W1_TOPOLOGY));
    this.nativeReplication = isOn(DimensionId.W2_NATIVE_REPLICATION);
    this.activeActive = isOn(DimensionId.W3_ACTIVE_ACTIVE);
    this.writeCompute = isOn(DimensionId.W4_WRITE_COMPUTE);
    this.chunking = isOn(DimensionId.W5_CHUNKING);
    this.rmdChunking = isOn(DimensionId.W6_RMD_CHUNKING);
    this.incrementalPush = isOn(DimensionId.W7_INCREMENTAL_PUSH);
    this.compression = parseCompression(get(DimensionId.W8_COMPRESSION));
    this.deferredSwap = isOn(DimensionId.W9_DEFERRED_SWAP);
    this.targetRegionPush = isOn(DimensionId.W10_TARGET_REGION_PUSH);
    this.pushEngine = parsePushEngine(get(DimensionId.W11_PUSH_ENGINE));
    this.ttlRepush = isOn(DimensionId.W12_TTL_REPUSH);
    this.separateRTTopic = isOn(DimensionId.W13_SEPARATE_RT_TOPIC);

    // Parse read path
    this.clientType = parseClientType(get(DimensionId.R1_CLIENT_TYPE));
    this.readCompute = isOn(DimensionId.R2_READ_COMPUTE);
    this.daVinciStorage = parseDaVinciStorage(get(DimensionId.R3_DAVINCI_STORAGE));
    this.fastRouting = parseRoutingStrategy(get(DimensionId.R4_FAST_ROUTING));
    this.longTailRetry = isOn(DimensionId.R5_LONG_TAIL_RETRY);
    this.recordTransformer = parseOptionalOnOff(get(DimensionId.R6_RECORD_TRANSFORMER));

    // Parse server
    this.parallelBatchGet = isOn(DimensionId.S1_PARALLEL_BATCH_GET);
    this.fastAvro = isOn(DimensionId.S2_FAST_AVRO);
    this.aawcParallel = isOn(DimensionId.S3_AAWC_PARALLEL);
    this.blobTransfer = isOn(DimensionId.S4_BLOB_TRANSFER);
    this.quotaEnforcement = isOn(DimensionId.S5_QUOTA_ENFORCEMENT);
    this.adaptiveThrottler = isOn(DimensionId.S6_ADAPTIVE_THROTTLER);

    // Parse controller
    this.aaDefaultHybrid = isOn(DimensionId.C1_AA_DEFAULT_HYBRID);
    this.wcAutoHybridAA = isOn(DimensionId.C2_WC_AUTO_HYBRID_AA);
    this.incPushAutoHybridAA = isOn(DimensionId.C3_INC_PUSH_AUTO_HYBRID_AA);
    this.separateRTAutoIncPush = isOn(DimensionId.C4_SEPARATE_RT_AUTO_INC_PUSH);
    this.deferredSwapService = isOn(DimensionId.C5_DEFERRED_SWAP_SERVICE);
    this.schemaValidation = isOn(DimensionId.C6_SCHEMA_VALIDATION);
    this.backupVersionCleanup = isOn(DimensionId.C7_BACKUP_VERSION_CLEANUP);
    this.systemStoreAutoMat = isOn(DimensionId.C8_SYSTEM_STORE_AUTO_MAT);
    this.supersetSchemaGen = isOn(DimensionId.C9_SUPERSET_SCHEMA_GEN);

    // Parse router (RT dimensions may be absent from PICT output — use production defaults)
    this.readThrottling =
        dimensions.containsKey(DimensionId.RT1_READ_THROTTLING) ? isOn(DimensionId.RT1_READ_THROTTLING) : true;
    this.earlyThrottle = isOn(DimensionId.RT2_EARLY_THROTTLE);
    this.smartLongTailRetry = dimensions.containsKey(DimensionId.RT3_SMART_LONG_TAIL_RETRY)
        ? isOn(DimensionId.RT3_SMART_LONG_TAIL_RETRY)
        : true;
    this.routingStrategy = dimensions.containsKey(DimensionId.RT4_ROUTING_STRATEGY)
        ? parseRoutingStrategy(get(DimensionId.RT4_ROUTING_STRATEGY))
        : RoutingStrategy.LEAST_LOADED;
    this.latencyBasedRouting = isOn(DimensionId.RT5_LATENCY_BASED_ROUTING);
    this.clientDecompression = dimensions.containsKey(DimensionId.RT6_CLIENT_DECOMPRESSION)
        ? isOn(DimensionId.RT6_CLIENT_DECOMPRESSION)
        : true;
    this.http2Inbound = isOn(DimensionId.RT7_HTTP2_INBOUND);
    this.connectionWarming = isOn(DimensionId.RT8_CONNECTION_WARMING);
  }

  private String get(DimensionId id) {
    return dimensions.getOrDefault(id, "off");
  }

  private boolean isOn(DimensionId id) {
    return "on".equalsIgnoreCase(get(id));
  }

  private static Topology parseTopology(String value) {
    switch (value) {
      case "Batch-only":
        return Topology.BATCH_ONLY;
      case "Hybrid":
        return Topology.HYBRID;
      case "Nearline-only":
        return Topology.NEARLINE_ONLY;
      default:
        throw new IllegalArgumentException("Unknown topology: " + value);
    }
  }

  private static Compression parseCompression(String value) {
    switch (value) {
      case "NO_OP":
        return Compression.NO_OP;
      case "GZIP":
        return Compression.GZIP;
      case "ZSTD_WITH_DICT":
        return Compression.ZSTD_WITH_DICT;
      default:
        throw new IllegalArgumentException("Unknown compression: " + value);
    }
  }

  private static PushEngine parsePushEngine(String value) {
    switch (value) {
      case "MapReduce":
        return PushEngine.MAP_REDUCE;
      case "Spark":
        return PushEngine.SPARK;
      default:
        throw new IllegalArgumentException("Unknown push engine: " + value);
    }
  }

  private static ClientType parseClientType(String value) {
    switch (value) {
      case "Thin":
        return ClientType.THIN;
      case "Fast":
        return ClientType.FAST;
      case "DaVinci":
        return ClientType.DA_VINCI;
      default:
        throw new IllegalArgumentException("Unknown client type: " + value);
    }
  }

  private static DaVinciStorageClass parseDaVinciStorage(String value) {
    switch (value) {
      case "MEMORY_BACKED_BY_DISK":
        return DaVinciStorageClass.MEMORY_BACKED_BY_DISK;
      case "MEMORY":
        return DaVinciStorageClass.MEMORY;
      case "N_A":
        return DaVinciStorageClass.N_A;
      default:
        throw new IllegalArgumentException("Unknown DaVinci storage: " + value);
    }
  }

  private static RoutingStrategy parseRoutingStrategy(String value) {
    switch (value) {
      case "LEAST_LOADED":
        return RoutingStrategy.LEAST_LOADED;
      case "HELIX_ASSISTED":
        return RoutingStrategy.HELIX_ASSISTED;
      case "N_A":
        return RoutingStrategy.N_A;
      default:
        throw new IllegalArgumentException("Unknown routing strategy: " + value);
    }
  }

  private static OptionalOnOff parseOptionalOnOff(String value) {
    switch (value) {
      case "on":
        return OptionalOnOff.ON;
      case "off":
        return OptionalOnOff.OFF;
      case "N_A":
        return OptionalOnOff.N_A;
      default:
        throw new IllegalArgumentException("Unknown optional on/off: " + value);
    }
  }

  // ============================================================
  // Getters
  // ============================================================

  public int getTestCaseId() {
    return testCaseId;
  }

  public Map<DimensionId, String> getDimensions() {
    return dimensions;
  }

  // Write path
  public Topology getTopology() {
    return topology;
  }

  public boolean isNativeReplication() {
    return nativeReplication;
  }

  public boolean isActiveActive() {
    return activeActive;
  }

  public boolean isWriteCompute() {
    return writeCompute;
  }

  public boolean isChunking() {
    return chunking;
  }

  public boolean isRmdChunking() {
    return rmdChunking;
  }

  public boolean isIncrementalPush() {
    return incrementalPush;
  }

  public Compression getCompression() {
    return compression;
  }

  public boolean isDeferredSwap() {
    return deferredSwap;
  }

  public boolean isTargetRegionPush() {
    return targetRegionPush;
  }

  public PushEngine getPushEngine() {
    return pushEngine;
  }

  public boolean isTtlRepush() {
    return ttlRepush;
  }

  public boolean isSeparateRTTopic() {
    return separateRTTopic;
  }

  // Read path
  public ClientType getClientType() {
    return clientType;
  }

  public boolean isReadCompute() {
    return readCompute;
  }

  public DaVinciStorageClass getDaVinciStorage() {
    return daVinciStorage;
  }

  public RoutingStrategy getFastRouting() {
    return fastRouting;
  }

  public boolean isLongTailRetry() {
    return longTailRetry;
  }

  public OptionalOnOff getRecordTransformer() {
    return recordTransformer;
  }

  // Server
  public boolean isParallelBatchGet() {
    return parallelBatchGet;
  }

  public boolean isFastAvro() {
    return fastAvro;
  }

  public boolean isAawcParallel() {
    return aawcParallel;
  }

  public boolean isBlobTransfer() {
    return blobTransfer;
  }

  public boolean isQuotaEnforcement() {
    return quotaEnforcement;
  }

  public boolean isAdaptiveThrottler() {
    return adaptiveThrottler;
  }

  // Controller
  public boolean isAaDefaultHybrid() {
    return aaDefaultHybrid;
  }

  public boolean isWcAutoHybridAA() {
    return wcAutoHybridAA;
  }

  public boolean isIncPushAutoHybridAA() {
    return incPushAutoHybridAA;
  }

  public boolean isSeparateRTAutoIncPush() {
    return separateRTAutoIncPush;
  }

  public boolean isDeferredSwapService() {
    return deferredSwapService;
  }

  public boolean isSchemaValidation() {
    return schemaValidation;
  }

  public boolean isBackupVersionCleanup() {
    return backupVersionCleanup;
  }

  public boolean isSystemStoreAutoMat() {
    return systemStoreAutoMat;
  }

  public boolean isSupersetSchemaGen() {
    return supersetSchemaGen;
  }

  // Router
  public boolean isReadThrottling() {
    return readThrottling;
  }

  public boolean isEarlyThrottle() {
    return earlyThrottle;
  }

  public boolean isSmartLongTailRetry() {
    return smartLongTailRetry;
  }

  public RoutingStrategy getRoutingStrategy() {
    return routingStrategy;
  }

  public boolean isLatencyBasedRouting() {
    return latencyBasedRouting;
  }

  public boolean isClientDecompression() {
    return clientDecompression;
  }

  public boolean isHttp2Inbound() {
    return http2Inbound;
  }

  public boolean isConnectionWarming() {
    return connectionWarming;
  }

  // ============================================================
  // Cluster config key: unique (S,RT,C) tuple for cluster grouping
  // ============================================================

  /**
   * Returns a string key representing the unique combination of Server, Router,
   * and Controller dimensions. Test cases sharing the same cluster config key
   * can share a cluster instance.
   */
  public String getClusterConfigKey() {
    StringBuilder sb = new StringBuilder();
    for (DimensionId dim: DimensionId.values()) {
      String group = dim.getComponentGroup();
      if ("SERVER".equals(group) || "ROUTER".equals(group) || "CONTROLLER".equals(group)) {
        sb.append(dim.name()).append("=").append(dimensions.get(dim)).append("|");
      }
    }
    return sb.toString();
  }

  /**
   * Returns a human-readable test name encoding all 42 dimensions.
   */
  public String getTestName() {
    return "TC" + testCaseId + "["
        + dimensions.entrySet()
            .stream()
            .map(e -> e.getKey().name() + "=" + e.getValue())
            .collect(Collectors.joining(","))
        + "]";
  }

  @Override
  public String toString() {
    return getTestName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestCaseConfig that = (TestCaseConfig) o;
    return testCaseId == that.testCaseId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(testCaseId);
  }
}
