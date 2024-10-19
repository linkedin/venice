package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;

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
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return DEFAULT_SCHEMA;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> configs) {
    Properties properties = new Properties();
    properties.putAll(configs);
    return new VeniceHdfsInputTable(new VeniceProperties(properties));
  }
}
