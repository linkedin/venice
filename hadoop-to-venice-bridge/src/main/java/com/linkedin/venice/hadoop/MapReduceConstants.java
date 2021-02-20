package com.linkedin.venice.hadoop;

public class MapReduceConstants {
  public static final String COUNTER_GROUP_QUOTA = "Quota";
  public static final String COUNTER_TOTAL_KEY_SIZE = "total key size";
  public static final String COUNTER_TOTAL_UNCOMPRESSED_VALUE_SIZE = "total uncompressed value size";
  public static final String COUNTER_TOTAL_VALUE_SIZE = "total value size";

  public static final String COUNTER_GROUP_KAFKA = "Kafka";
  public static final String COUNTER_OUTPUT_RECORDS = "Output Records";
  public static final String EMPTY_RECORD = "empty record";
  public static final String AUTHORIZATION_FAILURES = "authorization failures";

  public static final String COUNTER_GROUP_KAFKA_BROKER = "Kafka metrics aggregated per broker";
  private static final String KAFKA_PRODUCER_METRIC_FOR_BROKER = "%s for %s";

  public static final String COUNTER_GROUP_DATA_QUALITY = "Data quality";
  public static final String DUPLICATE_KEY_WITH_IDENTICAL_VALUE = "duplicate key with identical value";
  public static final String DUPLICATE_KEY_WITH_DISTINCT_VALUE = "duplicate key with distinct value";

  public static String getKafkaProducerMetricForBrokerCounterName(String metric, String broker) {
    return String.format(KAFKA_PRODUCER_METRIC_FOR_BROKER, metric, broker);
  }

  private MapReduceConstants() {}
}
