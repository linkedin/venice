package com.linkedin.venice.featurematrix.model;

/**
 * Defines the 42 feature dimensions used in 3-way combinatorial testing.
 * Each enum constant represents one dimension, and the nested value enums
 * define the possible values for that dimension.
 */
public class FeatureDimensions {

  // ============================================================
  // Write Path Dimensions (W1-W13)
  // ============================================================
  public enum Topology {
    BATCH_ONLY, HYBRID, NEARLINE_ONLY
  }

  public enum OnOff {
    ON, OFF
  }

  public enum Compression {
    NO_OP, GZIP, ZSTD_WITH_DICT
  }

  public enum PushEngine {
    MAP_REDUCE, SPARK
  }

  // ============================================================
  // Read Path Dimensions (R1-R6)
  // ============================================================

  public enum ClientType {
    THIN, FAST, DA_VINCI
  }

  public enum DaVinciStorageClass {
    MEMORY_BACKED_BY_DISK, MEMORY, N_A
  }

  public enum RoutingStrategy {
    LEAST_LOADED, HELIX_ASSISTED, N_A
  }

  public enum OptionalOnOff {
    ON, OFF, N_A
  }

  // ============================================================
  // Dimension IDs for programmatic access
  // ============================================================

  public enum DimensionId {
    // Write Path
    W1_TOPOLOGY("W1_Topology"), W2_NATIVE_REPLICATION("W2_NativeReplication"), W3_ACTIVE_ACTIVE("W3_ActiveActive"),
    W4_WRITE_COMPUTE("W4_WriteCompute"), W5_CHUNKING("W5_Chunking"), W6_RMD_CHUNKING("W6_RmdChunking"),
    W7_INCREMENTAL_PUSH("W7_IncrementalPush"), W8_COMPRESSION("W8_Compression"), W9_DEFERRED_SWAP("W9_DeferredSwap"),
    W10_TARGET_REGION_PUSH("W10_TargetRegionPush"), W11_PUSH_ENGINE("W11_PushEngine"), W12_TTL_REPUSH("W12_TTLRepush"),
    W13_SEPARATE_RT_TOPIC("W13_SeparateRTTopic"),

    // Read Path
    R1_CLIENT_TYPE("R1_ClientType"), R2_READ_COMPUTE("R2_ReadCompute"), R3_DAVINCI_STORAGE("R3_DaVinciStorage"),
    R4_FAST_ROUTING("R4_FastRouting"), R5_LONG_TAIL_RETRY("R5_LongTailRetry"),
    R6_RECORD_TRANSFORMER("R6_RecordTransformer"),

    // Server
    S1_PARALLEL_BATCH_GET("S1_ParallelBatchGet"), S2_FAST_AVRO("S2_FastAvro"), S3_AAWC_PARALLEL("S3_AAWCParallel"),
    S4_BLOB_TRANSFER("S4_BlobTransfer"), S5_QUOTA_ENFORCEMENT("S5_QuotaEnforcement"),
    S6_ADAPTIVE_THROTTLER("S6_AdaptiveThrottler"),

    // Controller
    C1_AA_DEFAULT_HYBRID("C1_AADefaultHybrid"), C2_WC_AUTO_HYBRID_AA("C2_WCAutoHybridAA"),
    C3_INC_PUSH_AUTO_HYBRID_AA("C3_IncPushAutoHybridAA"), C4_SEPARATE_RT_AUTO_INC_PUSH("C4_SeparateRTAutoIncPush"),
    C5_DEFERRED_SWAP_SERVICE("C5_DeferredSwapService"), C6_SCHEMA_VALIDATION("C6_SchemaValidation"),
    C7_BACKUP_VERSION_CLEANUP("C7_BackupVersionCleanup"), C8_SYSTEM_STORE_AUTO_MAT("C8_SystemStoreAutoMat"),
    C9_SUPERSET_SCHEMA_GEN("C9_SupersetSchemaGen"),

    // Router
    RT1_READ_THROTTLING("RT1_ReadThrottling"), RT2_EARLY_THROTTLE("RT2_EarlyThrottle"),
    RT3_SMART_LONG_TAIL_RETRY("RT3_SmartLongTailRetry"), RT4_ROUTING_STRATEGY("RT4_RoutingStrategy"),
    RT5_LATENCY_BASED_ROUTING("RT5_LatencyBasedRouting"), RT6_CLIENT_DECOMPRESSION("RT6_ClientDecompression"),
    RT7_HTTP2_INBOUND("RT7_Http2Inbound"), RT8_CONNECTION_WARMING("RT8_ConnectionWarming");

    private final String pictColumnName;

    DimensionId(String pictColumnName) {
      this.pictColumnName = pictColumnName;
    }

    public String getPictColumnName() {
      return pictColumnName;
    }

    /** Returns the component group: WRITE, READ, SERVER, CONTROLLER, or ROUTER. */
    public String getComponentGroup() {
      String name = name();
      if (name.startsWith("W")) {
        return "WRITE";
      } else if (name.startsWith("R")) {
        return "READ";
      } else if (name.startsWith("S")) {
        return "SERVER";
      } else if (name.startsWith("C")) {
        return "CONTROLLER";
      } else {
        return "ROUTER";
      }
    }
  }
}
