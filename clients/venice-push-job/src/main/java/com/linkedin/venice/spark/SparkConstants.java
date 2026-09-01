package com.linkedin.venice.spark;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class SparkConstants {
  // Required column names for input dataframes
  public static final String KEY_COLUMN_NAME = "key";
  public static final String VALUE_COLUMN_NAME = "value";
  public static final String RMD_COLUMN_NAME = "rmd";

  // Internal column names, hence begins with "_"
  public static final String PARTITION_COLUMN_NAME = "__partition__";
  public static final String RECORD_COUNT_COLUMN_NAME = "__record_count__";
  public static final String FAILED_EXTERNAL_STORAGE_REGIONS_COLUMN_NAME = "__failed_external_storage_regions__";
  public static final String EXTERNAL_STORAGE_WRITE_TIME_MS_COLUMN_NAME = "__external_storage_write_time_ms__";
  public static final String VENICE_WRITE_TIME_MS_COLUMN_NAME = "__venice_write_time_ms__";
  public static final String SCHEMA_ID_COLUMN_NAME = "__schema_id__";
  public static final String RMD_VERSION_ID_COLUMN_NAME = "__replication_metadata_version_id__";
  public static final String OFFSET_COLUMN_NAME = "__offset__";
  public static final String MESSAGE_TYPE_COLUMN_NAME = "__message_type__";
  public static final String CHUNKED_KEY_SUFFIX_COLUMN_NAME = "__chunked_key_suffix__";

  public static final StructType DEFAULT_SCHEMA = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

  /**
   * Task output emitted once per Spark partition by the partition writer. Everything the driver needs from a
   * data-writer task that must not be collected via accumulators travels through these columns: Spark
   * speculative execution can run two attempts for the same partition and accumulator updates from both
   * attempts are visible on the driver, which would double count. Exactly one successful task output row per
   * partition survives {@code collect()}, so the row-based values stay exact.
   *
   * <p>The two timing columns are per-task wall-clock durations (see
   * {@link DataWriterTaskTracker#trackExternalStorageWriteTime}),
   * summed by the driver across partitions. They are a sum of task durations, not the push's wall-clock time.
   */
  public static final StructType PARTITION_RECORD_COUNT_SCHEMA = new StructType(
      new StructField[] { new StructField(PARTITION_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(RECORD_COUNT_COLUMN_NAME, LongType, false, Metadata.empty()),
          new StructField(
              FAILED_EXTERNAL_STORAGE_REGIONS_COLUMN_NAME,
              DataTypes.createArrayType(StringType),
              false,
              Metadata.empty()),
          new StructField(EXTERNAL_STORAGE_WRITE_TIME_MS_COLUMN_NAME, LongType, false, Metadata.empty()),
          new StructField(VENICE_WRITE_TIME_MS_COLUMN_NAME, LongType, false, Metadata.empty()) });

  public static final StructType DEFAULT_SCHEMA_WITH_PARTITION = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(PARTITION_COLUMN_NAME, IntegerType, false, Metadata.empty()) });

  // Schema with schema IDs - used for Kafka repush with per-record schema tracking
  public static final StructType DEFAULT_SCHEMA_WITH_SCHEMA_ID = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(SCHEMA_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(RMD_VERSION_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()) });

  // Schema for chunk assembly - includes offset, message_type, and chunked_key_suffix needed for sorting and assembly
  public static final StructType SCHEMA_FOR_CHUNK_ASSEMBLY = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(SCHEMA_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(RMD_VERSION_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(OFFSET_COLUMN_NAME, LongType, false, Metadata.empty()),
          new StructField(MESSAGE_TYPE_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(CHUNKED_KEY_SUFFIX_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

  /**
   * Configs with this prefix will be set when building the spark session. These will get applied to all Spark jobs that
   * get triggered as a part of VPJ. It can be used to configure arbitrary cluster properties like cluster address.
   */
  public static final String SPARK_SESSION_CONF_PREFIX = "venice.spark.session.conf.";

  public static final String SPARK_APP_NAME_CONFIG = "spark.app.name";
  public static final String SPARK_CASE_SENSITIVE_CONFIG = "spark.sql.caseSensitive";

  public static final String SPARK_CLUSTER_CONFIG = "venice.spark.cluster";
  public static final String SPARK_LEADER_CONFIG = "spark.master";
  public static final String DEFAULT_SPARK_CLUSTER = "local[*]";

  /**
   * Configs with this prefix will be set when building the data writer spark job and passed as job properties. These
   * will only get applied on the DataWriter Spark jobs. It is useful when there are custom input formats which need
   * additional configs to be able to read the data.
   */
  public static final String SPARK_DATA_WRITER_CONF_PREFIX = "spark.data.writer.conf.";

  public static final String REPLICATION_METADATA_PAYLOAD = "__replication_metadata_payload__";
  public static final String MESSAGE_TYPE = "__message_type__";
  public static final String OFFSET = "__offset__";

  public static final StructType RAW_PUBSUB_INPUT_TABLE_SCHEMA = new StructType(
      new StructField[] { new StructField("__region__", StringType, false, Metadata.empty()),
          new StructField(PARTITION_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(OFFSET, LongType, true, Metadata.empty()), // offset in the topic
          new StructField(MESSAGE_TYPE, IntegerType, false, Metadata.empty()), // enum of put/delete/update
          new StructField(SCHEMA_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()), // serialized key
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()), // serialized value
          new StructField(RMD_VERSION_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
          new StructField(REPLICATION_METADATA_PAYLOAD, BinaryType, false, Metadata.empty()),
          // chunked key suffix (null if not chunked)
          new StructField(CHUNKED_KEY_SUFFIX_COLUMN_NAME, BinaryType, true, Metadata.empty()) });
}
