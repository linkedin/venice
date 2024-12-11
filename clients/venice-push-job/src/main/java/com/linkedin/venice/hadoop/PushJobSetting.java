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
import java.util.Map;
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
  public boolean enablePushJobStatusUpload;
  public int controllerRetries;
  public int controllerStatusPollRetries;
  public long pollJobStatusIntervalMs;
  public long jobStatusInUnknownStateTimeoutMs;
  public boolean sendControlMessagesDirectly;
  public boolean isSourceETL;
  public boolean enableWriteCompute;
  public boolean isSourceKafka;
  public String kafkaInputBrokerUrl;
  public String kafkaInputTopic;
  public int repushSourceVersion;
  public long rewindTimeInSecondsOverride;
  public boolean pushToSeparateRealtimeTopicEnabled;
  public boolean kafkaInputCombinerEnabled;
  public boolean kafkaInputBuildNewDictEnabled;
  public BufferReplayPolicy validateRemoteReplayPolicy;
  public boolean suppressEndOfPushMessage;
  public boolean deferVersionSwap;
  public boolean extendedSchemaValidityCheckEnabled;
  /** Refer {@link VenicePushJobConstants#COMPRESSION_METRIC_COLLECTION_ENABLED} **/
  public boolean compressionMetricCollectionEnabled;
  /** Refer {@link VenicePushJobConstants#USE_MAPPER_TO_BUILD_DICTIONARY} **/
  public boolean useMapperToBuildDict;
  public boolean repushTTLEnabled;
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
  public boolean isSystemSchemaReaderEnabled;
  public String systemSchemaClusterD2ServiceName;
  public String systemSchemaClusterD2ZKHost;
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
  // Kafka url will get from Venice backend for store push
  public String kafkaUrl;
  public boolean sslToKafka;
  public CompressionStrategy topicCompressionStrategy;
  public String partitionerClass;
  public Map<String, String> partitionerParams;
  public boolean chunkingEnabled;
  public boolean rmdChunkingEnabled;
  public int maxRecordSizeBytes;
  public String kafkaSourceRegion;
  public transient RepushInfoResponse repushInfoResponse;

  // Schema-properties
  public boolean isAvro = true;
  public int valueSchemaId; // Value schema id retrieved from backend for valueSchemaString
  public int derivedSchemaId = -1;
  public String keyField;
  public String valueField;

  public Schema inputDataSchema;
  public String inputDataSchemaString;

  public Schema keySchema;
  public String keySchemaString;

  public Schema valueSchema;
  public String valueSchemaString;

  public VsonSchema vsonInputKeySchema;
  public String vsonInputKeySchemaString;

  public VsonSchema vsonInputValueSchema;
  public String vsonInputValueSchemaString;

  public boolean generatePartialUpdateRecordFromInput;
  public ETLValueSchemaTransformation etlValueSchemaTransformation;

  // Additional inferred properties
  public boolean inputHasRecords;
  public long inputFileDataSizeInBytes;

  // KIF Source topic props
  public transient Version sourceKafkaInputVersionInfo;
  public CompressionStrategy sourceVersionCompressionStrategy;
  public boolean sourceVersionChunkingEnabled;

  public PushJobSetting() {
    // Default for preserving backward compatibility
    this.jobStartTimeMs = System.currentTimeMillis();
  }
}
