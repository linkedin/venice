package com.linkedin.venice.vpj;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.ValidateSchemaAndBuildDictMapper;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.mapreduce.datawriter.map.AbstractVeniceMapper;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.Time;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;


public final class VenicePushJobConstants {
  private VenicePushJobConstants() {
  }

  // Avro input configs
  public static final String LEGACY_AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String LEGACY_AVRO_VALUE_FIELD_PROP = "avro.value.field";

  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";
  public static final String DEFAULT_KEY_FIELD_PROP = "key";
  public static final String DEFAULT_VALUE_FIELD_PROP = "value";
  public static final boolean DEFAULT_SSL_ENABLED = false;
  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP = "kafka.source.key.schema";
  public static final String EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = "extended.schema.validity.check.enabled";
  public static final boolean DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = true;
  public static final String UPDATE_SCHEMA_STRING_PROP = "update.schema";

  // This is a temporary config used to rollout the native input format for Spark. This will be removed soon
  public static final String SPARK_NATIVE_INPUT_FORMAT_ENABLED = "spark.native.input.format.enabled";

  // Vson input configs
  // Vson files store key/value schema on file header. key / value fields are optional
  // and should be specified only when key / value schema is the partial of the files.
  public static final String FILE_KEY_SCHEMA = "key.schema";
  public static final String FILE_VALUE_SCHEMA = "value.schema";
  public static final String INCREMENTAL_PUSH = "incremental.push";
  public static final String GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT = "generate.partial.update.record.from.input";
  // veniceReducer will not fail fast and override the previous key if this is true and duplicate keys incur.
  public static final String PARTITION_COUNT = "partition.count";
  public static final String ALLOW_DUPLICATE_KEY = "allow.duplicate.key";
  public static final String POLL_STATUS_RETRY_ATTEMPTS = "poll.status.retry.attempts";
  public static final String CONTROLLER_REQUEST_RETRY_ATTEMPTS = "controller.request.retry.attempts";
  public static final String POLL_JOB_STATUS_INTERVAL_MS = "poll.job.status.interval.ms";
  public static final String JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = "job.status.in.unknown.state.timeout.ms";
  public static final String SEND_CONTROL_MESSAGES_DIRECTLY = "send.control.messages.directly";
  public static final String SOURCE_ETL = "source.etl";
  public static final String ETL_VALUE_SCHEMA_TRANSFORMATION = "etl.value.schema.transformation";
  public static final String SYSTEM_SCHEMA_READER_ENABLED = "system.schema.reader.enabled";
  public static final String SYSTEM_SCHEMA_CLUSTER_D2_SERVICE_NAME = "system.schema.cluster.d2.service.name";
  public static final String SYSTEM_SCHEMA_CLUSTER_D2_ZK_HOST = "system.schema.cluster.d2.zk.host";

  /**
   *  Config to enable/disable the feature to collect extra metrics wrt compression.
   *  Enabling this collects metrics for all compression strategies regardless of
   *  the configured compression strategy. This means: zstd dictionary will be
   *  created even if {@link CompressionStrategy#ZSTD_WITH_DICT} is not the configured
   *  store compression strategy (refer {@link VenicePushJob#shouldBuildZstdCompressionDictionary})
   *  <br><br>
   *
   *  This config also gets evaluated in {@link VenicePushJob#evaluateCompressionMetricCollectionEnabled}
   *  <br><br>
   */
  public static final String COMPRESSION_METRIC_COLLECTION_ENABLED = "compression.metric.collection.enabled";
  public static final boolean DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED = false;

  /**
   * Config to enable/disable using mapper to do the below which are currently done in VPJ driver <br>
   * 1. validate schema, <br>
   * 2. collect the input data size <br>
   * 3. build dictionary (if needed: refer {@link VenicePushJob#shouldBuildZstdCompressionDictionary}) <br><br>
   *
   * This new mapper was added because the sample collection for Zstd dictionary is currently
   * in-memory and to help play around with the sample size and also to support future enhancements
   * if needed. <br><br>
   *
   * Currently, the plan is to only have it available for MapReduce compute engine and remove it eventually as we
   * remove the MapReduce-related codes.
   */
  public static final String USE_MAPPER_TO_BUILD_DICTIONARY = "use.mapper.to.build.dictionary";
  public static final boolean DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY = false;

  /**
   * Known <a href="https://github.com/luben/zstd-jni/issues/253">zstd lib issue</a> which
   * crashes if the input sample is too small. So adding a preventive check to skip training
   * the dictionary in such cases using a minimum limit of 20. Keeping it simple and hard coding
   * it as if this check doesn't prevent some edge cases then we can disable the feature itself
   */
  public static final int MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY = 20;

  /**
   * Configs to pass to {@link AbstractVeniceMapper} based on the input configs and Dictionary
   * training status
   */
  public static final String ZSTD_DICTIONARY_CREATION_REQUIRED = "zstd.dictionary.creation.required";
  public static final String ZSTD_DICTIONARY_CREATION_SUCCESS = "zstd.dictionary.creation.success";

  /**
   * Location and key to store the output of {@link ValidateSchemaAndBuildDictMapper} and retrieve it back
   * when USE_MAPPER_TO_BUILD_DICTIONARY is enabled
   */
  // used to send the dir details from VPJ driver to mapper
  public static final String VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY =
      "validate.schema.and.build.dict.mapper.output.directory";

  public static final String VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_PREFIX = "mapper-output-";
  public static final String VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_EXTENSION = ".avro";

  // keys inside the avro file
  public static final String KEY_ZSTD_COMPRESSION_DICTIONARY = "zstdDictionary";
  public static final String KEY_INPUT_FILE_DATA_SIZE = "inputFileDataSize";

  /**
   * Configs used to enable Kafka Input.
   */
  public static final String SOURCE_KAFKA = "source.kafka";
  /**
   * TODO: consider to automatically discover the source topic for the specified store.
   * We need to be careful in the following scenarios:
   * 1. Not all the prod colos are using the same current version if the previous push experiences a partial failure.
   * 2. We might want to re-push from a backup version, which should be unlikely.
   */
  public static final String KAFKA_INPUT_TOPIC = "kafka.input.topic";
  public static final String KAFKA_INPUT_FABRIC = "kafka.input.fabric";
  public static final String KAFKA_INPUT_BROKER_URL = "kafka.input.broker.url";
  // Optional
  public static final String KAFKA_INPUT_MAX_RECORDS_PER_MAPPER = "kafka.input.max.records.per.mapper";
  public static final String KAFKA_INPUT_COMBINER_ENABLED = "kafka.input.combiner.enabled";
  // Whether to build a new dict in the repushed version or not while the original version has already enabled dict
  // compression.
  public static final String KAFKA_INPUT_COMPRESSION_BUILD_NEW_DICT_ENABLED =
      "kafka.input.compression.build.new.dict.enabled";

  public static final String KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED = "kafka.input.source.topic.chunking.enabled";
  /**
   * Optional.
   * If we want to use a different rewind time from the default store-level rewind time config for Kafka Input re-push,
   * the following property needs to specified explicitly.
   *
   * This property comes to play when the default rewind time configured in store-level is too short or too long.
   * 1. If the default rewind time config is too short (for example 0 or several mins), it could cause data gap with
   * re-push since the push job itself could take several hours, and we would like to make sure the re-pushed version
   * will contain the same dataset as the source version.
   * 2. If the default rewind time config is too long (such as 28 days), it will be a big waste to rewind so much time
   * since the time gap between the source version and the re-push version should be comparable to the re-push time
   * if the whole ingestion pipeline is not lagging.
   *
   * There are some challenges to automatically detect the right rewind time for re-push because of the following reasons:
   * 1. For Venice Aggregate use case, some colo could be lagging behind other prod colos, so if the re-push source is
   * from a fast colo, too short rewind time could cause a data gap in the slower colos. Ideally, it is good to use
   * the slowest colo as the re-push source.
   * 2. For Venice non-Aggregate use case, the ingestion pipeline will include the following several phases:
   * 2.1 Customer's Kafka aggregation and mirroring pipeline to replicate the same data to all prod colos.
   * 2.2 Venice Ingestion pipeline to consume the local real-time topic.
   * We have visibility to 2.2, but not 2.1, so we may need to work with customer to understand how 2.1 can be measured
   * or use a long enough rewind time to mitigate all the potential issues.
   *
   * Make this property available in generic since it should be useful for ETL+VPJ use case as well.
   */
  public static final String REWIND_TIME_IN_SECONDS_OVERRIDE = "rewind.time.in.seconds.override";

  /**
   * A time stamp specified to rewind to before replaying data. This config is ignored if rewind.time.in.seconds.override
   * is provided. This config at time of push will be leveraged to fill in the rewind.time.in.seconds.override by taking
   * System.currentTime - rewind.epoch.time.in.seconds.override and storing the result in rewind.time.in.seconds.override.
   * With this in mind, a push policy of REWIND_FROM_SOP should be used in order to get a behavior that makes sense to a user.
   * A timestamp that is in the future is not valid and will result in an exception.
   */
  public static final String REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE = "rewind.epoch.time.in.seconds.override";

  /**
   * This config is a boolean which suppresses submitting the end of push message after data has been sent and does
   * not poll for the status of the job to complete. Using this flag means that a user must manually mark the job success
   * or failed.
   */
  public static final String SUPPRESS_END_OF_PUSH_MESSAGE = "suppress.end.of.push.message";

  /**
   * This config is a boolean which waits for an external signal to trigger version swap after buffer replay is complete.
   */
  public static final String DEFER_VERSION_SWAP = "defer.version.swap";

  /**
   * This config specifies the prefix for d2 zk hosts config. Configs of type {@literal <prefix>.<regionName>} are
   * expected to be defined.
   */
  public static final String D2_ZK_HOSTS_PREFIX = "d2.zk.hosts.";

  /**
   * This config specifies the region identifier where parent controller is running
   */
  public static final String PARENT_CONTROLLER_REGION_NAME = "parent.controller.region.name";

  /**
   * Relates to the above argument. An overridable amount of buffer to be applied to the epoch (as the rewind isn't
   * perfectly instantaneous). Defaults to 1 minute.
   */
  public static final String REWIND_EPOCH_TIME_BUFFER_IN_SECONDS_OVERRIDE =
      "rewind.epoch.time.buffer.in.seconds.override";

  /**
   * In single-region mode, this must be a comma-separated list of child controller URLs or {@literal d2://<d2ServiceNameForChildController>}
   * In multi-region mode, it must be a comma-separated list of parent controller URLs or {@literal d2://<d2ServiceNameForParentController>}
   */
  public static final String VENICE_DISCOVER_URL_PROP = "venice.discover.urls";

  /**
   * An identifier of the data center which is used to determine the Kafka URL and child controllers that push jobs communicate with
   */
  public static final String SOURCE_GRID_FABRIC = "source.grid.fabric";

  public static final String ENABLE_WRITE_COMPUTE = "venice.write.compute.enable";
  public static final String ENABLE_SSL = "venice.ssl.enable";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String INPUT_PATH_LAST_MODIFIED_TIME = "input.path.last.modified.time";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";

  /**
   * ignore hdfs files with prefix "_" and "."
   */
  public static final PathFilter PATH_FILTER = p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");
  public static final String GLOB_FILTER_PATTERN = "[^_.]*";

  // Configs to control temp paths and their permissions
  public static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";
  public static final String TEMP_DIR_PREFIX = "tmp.dir.prefix";
  // World-readable and world-writable
  public static final FsPermission PERMISSION_777 = FsPermission.createImmutable((short) 0777);
  // Only readable and writable by the user running VPJ - restricted access
  public static final FsPermission PERMISSION_700 = FsPermission.createImmutable((short) 0700);

  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
  public static final String DERIVED_SCHEMA_ID_PROP = "derived.schema.id";
  public static final String TOPIC_PROP = "venice.kafka.topic";
  public static final String HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX = "hadoop-dict-build-conf.";
  public static final String SSL_PREFIX = "ssl";

  public static final String STORAGE_QUOTA_PROP = "storage.quota";
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage_engine_overhead_ratio";
  @Deprecated
  public static final String VSON_PUSH = "vson.push";
  public static final String KAFKA_SECURITY_PROTOCOL = "SSL";
  public static final String COMPRESSION_STRATEGY = "compression.strategy";
  public static final String KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY = "kafka.input.source.compression.strategy";
  public static final String SSL_CONFIGURATOR_CLASS_CONFIG = "ssl.configurator.class";
  public static final String SSL_KEY_STORE_PROPERTY_NAME = "ssl.key.store.property.name";
  public static final String SSL_TRUST_STORE_PROPERTY_NAME = "ssl.trust.store.property.name";
  public static final String SSL_KEY_STORE_PASSWORD_PROPERTY_NAME = "ssl.key.store.password.property.name";
  public static final String SSL_KEY_PASSWORD_PROPERTY_NAME = "ssl.key.password.property.name";

  // Configs that uniquely identify a VenicePushJob execution.
  /**
   * This will define the execution servers url for easy access to the job execution during debugging. This can be any
   * string that is meaningful to the execution environment.
   */
  public static final String JOB_EXEC_URL = "job.execution.url";
  /**
   * The short name of the server where the job runs
   */
  public static final String JOB_SERVER_NAME = "job.server.name";
  /**
   * The execution ID of the execution if this job is a part of a multi-step flow. Each step in the flow can have the
   * same execution id
   */
  public static final String JOB_EXEC_ID = "job.execution.id";

  /**
   * Config to enable the service that uploads push job statuses to the controller using
   * {@code ControllerClient.uploadPushJobStatus()}, the job status is then packaged and sent to dedicated Kafka channel.
   */
  public static final String PUSH_JOB_STATUS_UPLOAD_ENABLE = "push.job.status.upload.enable";
  public static final String REDUCER_SPECULATIVE_EXECUTION_ENABLE = "reducer.speculative.execution.enable";

  /**
   * The interval of number of messages upon which certain info is printed in the reducer logs.
   */
  public static final String TELEMETRY_MESSAGE_INTERVAL = "telemetry.message.interval";

  /**
   * Config to control the Compression Level for ZSTD Dictionary Compression.
   */
  public static final String ZSTD_COMPRESSION_LEVEL = "zstd.compression.level";
  public static final int DEFAULT_BATCH_BYTES_SIZE = 1000000;
  public static final boolean SORTED = true;
  /**
   * The rewind override when performing re-push to prevent data loss; if the store has higher rewind config setting than
   * 1 days, adopt the store config instead; otherwise, override the rewind config to 1 day if push job config doesn't
   * try to override it.
   */
  public static final long DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE = Time.SECONDS_PER_DAY;
  /**
   * Config to control the TTL behaviors in repush.
   */
  public static final String REPUSH_TTL_ENABLE = "repush.ttl.enable";
  public static final String REPUSH_TTL_POLICY = "repush.ttl.policy";
  public static final String REPUSH_TTL_SECONDS = "repush.ttl.seconds";
  public static final String REPUSH_TTL_START_TIMESTAMP = "repush.ttl.start.timestamp";
  public static final String RMD_SCHEMA_DIR = "rmd.schema.dir";
  public static final String VALUE_SCHEMA_DIR = "value.schema.dir";
  public static final int NOT_SET = -1;

  /**
   * Config to enable single targeted region push mode in VPJ.
   * In this mode, the VPJ will only push data to a single region.
   * The single region is decided by the store config in {@link StoreInfo#getNativeReplicationSourceFabric()}}.
   * For multiple targeted regions push, may use the advanced mode. See {@link #TARGETED_REGION_PUSH_LIST}.
   */
  public static final String TARGETED_REGION_PUSH_ENABLED = "targeted.region.push.enabled";

  /**
   * This is experimental config to specify a list of regions used for targeted region push in VPJ.
   * {@link #TARGETED_REGION_PUSH_ENABLED} has to be enabled to use this config.
   * In this mode, the VPJ will only push data to the provided regions.
   * The input is comma separated list of region names, e.g. "dc-0,dc-1,dc-2".
   * For single targeted region push, see {@link #TARGETED_REGION_PUSH_ENABLED}.
   */
  public static final String TARGETED_REGION_PUSH_LIST = "targeted.region.push.list";

  public static final boolean DEFAULT_IS_DUPLICATED_KEY_ALLOWED = false;

  /**
   * Config used only for tests. Should not be used at regular runtime.
   */
  public static final String MAP_REDUCE_PARTITIONER_CLASS_CONFIG = "map.reduce.partitioner.class";

  /**
   * Placeholder for version number that is yet to be created.
   */
  public static final int UNCREATED_VERSION_NUMBER = -1;
  public static final long DEFAULT_POLL_STATUS_INTERVAL_MS = 5 * Time.MS_PER_MINUTE;

  /**
   * The default total time we wait before failing a job if the job status stays in UNKNOWN state.
   */
  public static final long DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = 30 * Time.MS_PER_MINUTE;
  public static final String NON_CRITICAL_EXCEPTION = "This exception does not fail the push job. ";

  /** Sample size to collect for building dictionary: Can be assigned a max of 2GB as {@link ZstdDictTrainer} in ZSTD library takes in sample size as int */
  public static final String COMPRESSION_DICTIONARY_SAMPLE_SIZE = "compression.dictionary.sample.size";
  public static final int DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE = 200 * BYTES_PER_MB; // 200MB
  /** Maximum final dictionary size TODO add more details about the current limits */
  public static final String COMPRESSION_DICTIONARY_SIZE_LIMIT = "compression.dictionary.size.limit";

  // Compute engine abstraction
  /**
   * Config to set the class for the DataWriter job. When using KIF, we currently will continue to fall back to MR mode.
   * The class must extend {@link DataWriterComputeJob} and have a zero-arg constructor.
   */
  public static final String DATA_WRITER_COMPUTE_JOB_CLASS = "data.writer.compute.job.class";

  public static final String PUSH_TO_SEPARATE_REALTIME_TOPIC = "push.to.separate.realtime.topic";
}
