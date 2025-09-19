package com.linkedin.venice.spark.input.pubsub.raw;

import static com.linkedin.venice.spark.SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


/**
 * This is the entrypoint of the PubSub input source. It is used by Spark to create a DataFrame from a PubSub topic
 * The Topic must conform to Venice PubSub Version-Topic Schema.
 * This source does not support Realtime or versioned realtime topics at this point.
 */
@SuppressWarnings("unused")
public class VeniceRawPubsubSource implements TableProvider {
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return RAW_PUBSUB_INPUT_TABLE_SCHEMA;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> configs) {
    Properties properties = new Properties();
    properties.putAll(configs);
    return new VeniceRawPubsubInputTable(new VeniceProperties(properties));
  }
}
