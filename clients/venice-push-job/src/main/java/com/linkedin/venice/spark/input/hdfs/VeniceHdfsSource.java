package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


/**
 * This is the entrypoint of the Avro input source. It is used by Spark to create a DataFrame from a directory on
 * HDFS. The directory must contain either Avro or Vson files. The format of input files must be homogenous, i.e., it
 * cannot contain mixed formats or schemas.
 */
@SuppressWarnings("unused")
public class VeniceHdfsSource implements TableProvider {
  /**
   * Records the {@code configs} map (i.e. the DataFrameReader/reader options Spark resolved for this DataSource V2
   * table) most recently passed to {@link #getTable}. This is the only config channel visible to
   * {@link VeniceHdfsInputTable}, {@link VeniceHdfsInputScanBuilder}, and executor partition readers -- unlike
   * {@code SparkSession.conf()}, which is visible on the driver but never threaded through to this map. Exposed
   * purely for tests to verify (or disprove) that a given config actually reached the custom DataSource, since
   * there is no public Spark API to inspect a DataFrameReader's options after the fact.
   */
  @VisibleForTesting
  public static volatile Map<String, String> lastReceivedConfigs = null;

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return DEFAULT_SCHEMA;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> configs) {
    recordLastReceivedConfigs(configs);
    Properties properties = new Properties();
    properties.putAll(configs);
    return new VeniceHdfsInputTable(new VeniceProperties(properties));
  }

  /**
   * Writes {@link #lastReceivedConfigs}. Kept as a dedicated static method (rather than assigning the field
   * directly from the instance method {@link #getTable}) to avoid SpotBugs' ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD.
   */
  @VisibleForTesting
  public static void recordLastReceivedConfigs(Map<String, String> configs) {
    lastReceivedConfigs = configs;
  }
}
