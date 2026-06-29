package com.linkedin.venice.hadoop;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * This class carries the state for the duration of the VenicePushJob. Consider making breaking changes carefully.
 */
public class PushJobSetting implements Serializable {
  private static final long serialVersionUID = 1;

  // Job-setting inferred from job props
  public long jobStartTimeMs;
  public String jobId;
  public String jobExecutionId;
  public String jobServerName;
  // Path was not serializable till HDFS version 3.0.0, so we use URI instead:
  // https://issues.apache.org/jira/browse/HADOOP-13519
  public String sharedTmpDir;
  public String jobTmpDir;
  public boolean enableSSL;
  public Class<? extends VenicePushJob> vpjEntryClass;
  public String veniceControllerUrl;
  public String storeName;
  public String inputURI;
  public String sourceGridFabric;
  public int batchNumBytes;
  public boolean isIncrementalPush;
  public String incrementalPushVersion;
  public boolean isDuplicateKeyAllowed;
  public int controllerRetries;
  public int controllerStatusPollRetries;
  public long pollJobStatusIntervalMs;
  public long jobStatusInUnknownStateTimeoutMs;
  public long pushJobTimeoutOverrideMs;
  public boolean sendControlMessagesDirectly;
  public boolean isSourceETL;
  public boolean enableWriteCompute;
  public boolean isSourceKafka;
  /**
   * Broker URL for <b>consuming/reading</b> existing version data during a KIF (Kafka Input Format) repush.
   *
   * <p>This is the "input/source" side of a repush: the Kafka broker from which the previous version's
   * data is read. It is set from one of two sources:
   * <ol>
   *   <li>{@link RepushInfoResponse} returned by the controller (which resolves the fabric name
   *       from {@code KAFKA_INPUT_FABRIC} to a broker URL), or</li>
   *   <li>An explicit {@code VENICE_REPUSH_SOURCE_PUBSUB_BROKER} property provided by the caller.</li>
   * </ol>
   *
   * <p>This may point to a <em>different</em> fabric than {@link #pushDestinationPubsubBroker} when
   * the repush input fabric differs from the NR source fabric. For example, a repush may read v1
   * data from dc-1 but write v2 data to dc-0 (the NR source).
   *
   * @see #pushDestinationPubsubBroker the "output/destination" broker URL for producing new version data
   */
  public String repushSourcePubsubBroker;
  public String kafkaInputTopic;
  public int repushSourceVersion;
  public long rewindTimeInSecondsOverride;
  // Snapshot-at-T rewind-shortening knobs (per-VPJ). See VenicePushJobConstants#SNAPSHOT_AT_T_REWIND_ENABLED.
  public boolean snapshotAtTRewindEnabled;
  public long snapshotAtTMinRewindThresholdSeconds;
  public long snapshotAtTCutoffEpochSeconds;
  public long snapshotAtTRewindBufferSeconds;
  // Set to true when the snapshot-at-T rewind override is actually applied (after the threshold gate passes).
  public boolean snapshotAtTRewindApplied;
  // Per-region RT source brokers (colo id -> broker address) for the snapshot-at-T merge.
  public Map<Integer, String> snapshotAtTRtRegionBrokers;
  // When true, the snapshot-at-T batch+RT merge runs as a distributed Spark job instead of single-process.
  public boolean snapshotAtTDistributedMergeEnabled;
  // The store's schemas, fetched on the driver and broadcast to executors by the distributed merge job.
  public transient com.linkedin.venice.hadoop.snapshot.SnapshotAtTSchemaBundle snapshotAtTSchemaBundle;
  public boolean pushToSeparateRealtimeTopicEnabled;
  public boolean versionSeparateRealTimeTopicEnabled;
  public boolean kafkaInputCombinerEnabled;
  public boolean kafkaInputBuildNewDictEnabled;
  public BufferReplayPolicy validateRemoteReplayPolicy;
  public boolean suppressEndOfPushMessage;
  public boolean deferVersionSwap;
  public boolean extendedSchemaValidityCheckEnabled;
  /** Refer {@link VenicePushJobConstants#COMPRESSION_METRIC_COLLECTION_ENABLED} **/
  public boolean compressionMetricCollectionEnabled;
  public boolean repushTTLEnabled;
  public boolean isCompliancePush;
  // specify time to drop stale records.
  public long repushTTLStartTimeMs;
  // HDFS directory to cache RMD schemas
  public String rmdSchemaDir;
  public String valueSchemaDir;
  public String controllerD2ServiceName;
  public String parentControllerRegionD2ZkHosts;
  public String childControllerRegionD2ZkHosts;
  public boolean livenessHeartbeatEnabled;
  public String livenessHeartbeatStoreName;
  public boolean multiRegion;
  public boolean d2Routing;
  public String targetedRegions;
  public boolean isTargetedRegionPushEnabled;
  public boolean isTargetRegionPushWithDeferredSwapEnabled;
  public int targetRegionPushWithDeferredSwapWaitTime;
  public boolean isDegradedModePush;
  public Set<String> degradedDatacenters;
  public boolean isSystemSchemaReaderEnabled;
  public boolean isZstdDictCreationRequired;
  public boolean isZstdDictCreationSuccess;

  // Multiple compute engine support
  public Class<? extends DataWriterComputeJob> dataWriterComputeJobClass;

  // Store-config setting
  public String clusterName;
  public Schema storeKeySchema;
  public boolean isChunkingEnabled;
  public boolean isRmdChunkingEnabled;
  public long storeStorageQuota;
  /**
   * Names of the regions whose store-level storage mode is {@code DUAL_WRITE} for this push, resolved at job
   * setup by querying each region's store-level storage mode through the (parent) controller. The partition
   * writer loads one {@code ExternalStorageWriter} per region in this list and writes the dataset to that
   * region's external-storage endpoint; regions absent from the list stay Kafka-only. Reading the store-level
   * value per region (rather than the new version's value) avoids a race against the new version not yet
   * being materialized in child regions when VPJ resolves it. Only populated when the VPJ-side dual-write
   * writer-class is configured; otherwise stays empty (dual-write off).
   */
  public List<String> dualWriteTargetRegions = Collections.emptyList();
  public boolean isSchemaAutoRegisterFromPushJobEnabled;
  public CompressionStrategy storeCompressionStrategy;
  public boolean isStoreWriteComputeEnabled;
  public boolean isStoreIncrementalPushEnabled;
  public transient HybridStoreConfig hybridStoreConfig;
  public transient StoreResponse storeResponse;

  // Topic-properties
  // Kafka topic for new data push
  public String topic;
  /** Version part of the store-version / topic name */
  public int version;
  // Kafka topic partition count
  public int partitionCount;
  /**
   * Broker URL for <b>producing/writing</b> new version topic data.
   *
   * <p>This is the "output/destination" side of a push: the Kafka broker to which new version data
   * records are written. It is set from {@link com.linkedin.venice.controllerapi.VersionCreationResponse#getKafkaBootstrapServers()},
   * which returns the broker for the NR (Native Replication) source region. In NR mode, data is
   * first written to this broker, then replicated to other regions by the storage nodes.
   *
   * <p>For a cross-fabric repush (where the input fabric differs from the NR source), this URL
   * should point to the NR source fabric's broker — <em>not</em> the input fabric. For example,
   * if NR source = dc-0 and repush reads from dc-1, this URL should be dc-0's broker.
   *
   * @see #repushSourcePubsubBroker the "input/source" broker URL for consuming existing version data
   */
  public String pushDestinationPubsubBroker;
  public boolean sslToKafka;
  public CompressionStrategy topicCompressionStrategy;
  public String partitionerClass;
  public Map<String, String> partitionerParams;
  public boolean chunkingEnabled;
  public boolean rmdChunkingEnabled;
  public int maxRecordSizeBytes;
  public boolean enableUncompressedRecordSizeLimit;
  public String kafkaSourceRegion;
  public transient RepushInfoResponse repushInfoResponse;

  public boolean repushUseFallbackValueSchemaId;

  // Schema-properties
  public boolean isAvro = true;
  public int valueSchemaId; // Value schema id retrieved from backend for valueSchemaString
  public int rmdSchemaId = -1; // Replication metadata schema id retrieved from backend for
                               // replicationMetadataSchemaString
  public int derivedSchemaId = -1;
  public String keyField;
  public String valueField;
  public String rmdField;

  public Schema inputDataSchema;
  public String inputDataSchemaString;

  public Schema keySchema;
  public String keySchemaString;

  public Schema valueSchema;
  public String valueSchemaString;

  public String replicationMetadataSchemaString;

  public VsonSchema vsonInputKeySchema;
  public String vsonInputKeySchemaString;

  public VsonSchema vsonInputValueSchema;
  public String vsonInputValueSchemaString;

  public boolean generatePartialUpdateRecordFromInput;
  public ETLValueSchemaTransformation etlValueSchemaTransformation;
  public Map<Integer, String> newKmeSchemasFromController;

  /**
   * Schema-projection: target writer value schema to project superset
   * input records down to (see {@code TARGET_WRITER_VALUE_SCHEMA_ID_PROP}).
   * */
  public int targetWriterValueSchemaId = -1;
  public boolean projectInputToWriterSchema;
  public Schema writerValueSchema;
  public String writerValueSchemaString;

  // Additional inferred properties
  public boolean inputHasRecords;
  public long inputFileDataSizeInBytes;

  // KIF Source topic props
  public transient Version sourceKafkaInputVersionInfo;
  public CompressionStrategy sourceVersionCompressionStrategy;
  public boolean sourceVersionChunkingEnabled;

  public byte[] sourceDictionary;
  public byte[] topicDictionary;

  public PushJobSetting() {
    // Default for preserving backward compatibility
    this.jobStartTimeMs = System.currentTimeMillis();
  }

  public String materializedViewConfigFlatMap;

  public boolean isBatchWriteOptimizationForHybridStoreEnabled;
  public boolean isSortedIngestionEnabled;
  public boolean allowRegularPushWithTTLRepush;

}
