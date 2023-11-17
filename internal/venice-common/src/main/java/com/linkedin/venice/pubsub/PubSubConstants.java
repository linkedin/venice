package com.linkedin.venice.pubsub;

/**
 * Constants used by pub-sub components.
 */
public class PubSubConstants {

  // If true, the producer will use default configuration values for optimized high throughput
  // producing if they are not explicitly set.
  public static final String PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS =
      "pubsub.producer.use.high.throughput.defaults";

  // Timeout for consumer APIs which do not have a timeout parameter
  public static final String PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS = "pubsub.consumer.api.default.timeout.ms";
  public static final int PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE = 60_000; // 1 minute

  // Number of times to retry poll() upon failure
  public static final String PUBSUB_CONSUMER_POLL_RETRY_TIMES = "pubsub.consumer.poll.retry.times";
  public static final int PUBSUB_CONSUMER_POLL_RETRY_TIMES_DEFAULT_VALUE = 3;
  // Backoff time in milliseconds between poll() retries
  public static final String PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS = "pubsub.consumer.poll.retry.backoff.ms";
  public static final int PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT_VALUE = 0;

  public static final long PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE = 600;
  public static final long UNKNOWN_TOPIC_RETENTION = Long.MIN_VALUE;

  public static final String PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES = "pubsub.consumer.topic.query.retry.times";
  public static final int PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES_DEFAULT_VALUE = 5;

  public static final String PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS =
      "pubsub.consumer.topic.query.retry.interval.ms";
  public static final int PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS_DEFAULT_VALUE = 1000;

  public static final String PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE = "pubsub.consumer.check.topic.existence";
  public static final boolean PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE_DEFAULT_VALUE = true;
}
