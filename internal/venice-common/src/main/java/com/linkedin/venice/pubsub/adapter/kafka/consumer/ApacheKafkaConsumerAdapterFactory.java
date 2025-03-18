package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
//import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_BYTES_CONFIG;
//import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_WAIT_MS_CONFIG;
//import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MIN_BYTES_CONFIG;
//import static com.linkedin.venice.ConfigKeys.KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG;
//import static com.linkedin.venice.ConfigKeys.KAFKA_MAX_POLL_RECORDS_CONFIG;


public class ApacheKafkaConsumerAdapterFactory extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
  private static final String NAME = "ApacheKafkaConsumerAdapter";
  private static final VeniceProperties DEFAULT_KAFKA_CONSUMER_PROPERTIES =
      new VeniceProperties(getDefaultKafkaConsumerProperties());

  /**
   * Constructor for ApacheKafkaConsumerAdapterFactory used mainly for reflective instantiation.
   */
  public ApacheKafkaConsumerAdapterFactory() {
    // no-op
  }

  @Override
  public ApacheKafkaConsumerAdapter create(PubSubConsumerAdapterContext context) {
    return new ApacheKafkaConsumerAdapter(new ApacheKafkaConsumerConfig(context));
  }

  @Override
  public VeniceProperties getDefaultProperties() {
    return DEFAULT_KAFKA_CONSUMER_PROPERTIES;
  }

  private static Properties getDefaultKafkaConsumerProperties() {
    Properties properties = new Properties();
    // properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig.getKafkaBootstrapServers());
    // properties.setProperty(KAFKA_FETCH_MIN_BYTES_CONFIG,
    // String.valueOf(serverConfig.getKafkaFetchMinSizePerSecond()));
    // properties.setProperty(KAFKA_FETCH_MAX_BYTES_CONFIG,
    // String.valueOf(serverConfig.getKafkaFetchMaxSizePerSecond()));
    // properties.setProperty(KAFKA_MAX_POLL_RECORDS_CONFIG, Integer.toString(serverConfig.getKafkaMaxPollRecords()));
    // properties.setProperty(KAFKA_FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxTimeMS()));
    // properties.setProperty(KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG,
    // String.valueOf(serverConfig.getKafkaFetchPartitionMaxSizePerSecond()));
    // properties.setProperty(PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES,
    // String.valueOf(serverConfig.getPubSubConsumerPollRetryTimes()));
    // properties.setProperty(PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS,
    // String.valueOf(serverConfig.getPubSubConsumerPollRetryBackoffMs()));
    return properties;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
