package com.linkedin.venice.spark.input.pubsub.table;

import static com.linkedin.venice.spark.SparkConstants.*;

import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


public class VenicePubsubSource implements TableProvider {
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return KAFKA_INPUT_TABLE_SCHEMA;
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return TableProvider.super.inferPartitioning(options);
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> sparkConfigs) {
    Properties properties = new Properties();
    properties.putAll(sparkConfigs);
    // the properties here is the entry point for all the configurations
    // we receive from the outer layer.
    // schem and partitioning are useless and should be discarded?
    //

    // VeniceProperties consumerProperties = KafkaInputUtils.getConsumerProperties(properties);

    return new VenicePubsubInputTable(properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }
}
