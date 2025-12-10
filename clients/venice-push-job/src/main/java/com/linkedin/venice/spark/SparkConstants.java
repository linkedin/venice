package com.linkedin.venice.spark;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

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
  public static final String SCHEMA_ID_COLUMN_NAME = "__schema_id__";
  public static final String RMD_VERSION_ID_COLUMN_NAME = "__replication_metadata_version_id__";

  public static final StructType DEFAULT_SCHEMA = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

  public static final StructType DEFAULT_SCHEMA_WITH_PARTITION = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
          new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
          new StructField(PARTITION_COLUMN_NAME, IntegerType, false, Metadata.empty()) });

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
          new StructField(REPLICATION_METADATA_PAYLOAD, BinaryType, false, Metadata.empty()) });
}
