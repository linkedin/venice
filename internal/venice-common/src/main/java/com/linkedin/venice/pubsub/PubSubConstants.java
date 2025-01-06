package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.utils.Time;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


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

  public static final String PUBSUB_CONSUMER_POSITION_RESET_STRATEGY = "pubsub.consumer.position.reset.strategy";
  public static final String PUBSUB_CONSUMER_POSITION_RESET_STRATEGY_DEFAULT_VALUE = "earliest";

  public static final long PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE = 300;
  public static final long PUBSUB_TOPIC_UNKNOWN_RETENTION = Long.MIN_VALUE;

  public static final String PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES = "pubsub.consumer.topic.query.retry.times";
  public static final int PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES_DEFAULT_VALUE = 5;

  public static final String PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS =
      "pubsub.consumer.topic.query.retry.interval.ms";
  public static final int PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS_DEFAULT_VALUE = 1000;

  // PubSub admin APIs default timeout
  public static final String PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS = "pubsub.admin.api.default.timeout.ms";
  public static final int PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE = 120_000; // 2 minutes

  public static final String PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE = "pubsub.consumer.check.topic.existence";
  public static final boolean PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE_DEFAULT_VALUE = false;

  /**
   * Default setting is that no log compaction should happen for hybrid store version topics
   * if the messages are produced within 24 hours; otherwise servers could encounter MISSING
   * data DIV errors for reprocessing jobs which could potentially generate lots of
   * duplicate keys.
   */
  public static final long DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS = 24 * Time.MS_PER_HOUR;

  public static final List<Class<? extends Throwable>> CREATE_TOPIC_RETRIABLE_EXCEPTIONS =
      Collections.unmodifiableList(Arrays.asList(PubSubOpTimeoutException.class, PubSubClientRetriableException.class));

  /**
   * Default value of sleep interval for polling topic deletion status from ZK.
   */
  public static final int PUBSUB_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS_DEFAULT_VALUE = 2 * Time.MS_PER_SECOND;
  public static final long UNKNOWN_LATEST_OFFSET = -404L;

  private static final Duration PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE_DEFAULT = Duration.ofMinutes(1);
  private static Duration PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE =
      PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE_DEFAULT;

  public static Duration getPubsubOffsetApiTimeoutDurationDefaultValue() {
    return PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE;
  }

  /**
   * Package-private for use in tests. DO NOT CALL IN MAIN CODE!
   */
  static void setPubsubOffsetApiTimeoutDurationDefaultValue(Duration duration) {
    PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE = duration;
  }

  /**
   * Package-private for use in tests. DO NOT CALL IN MAIN CODE!
   */
  static void resetPubsubOffsetApiTimeoutDurationDefaultValue() {
    PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE = PUBSUB_OFFSET_API_TIMEOUT_DURATION_DEFAULT_VALUE_DEFAULT;
  }

  public static final long PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION = -1;

  public static final int PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT = 3;

  public static final int PUBSUB_TOPIC_MANAGER_METADATA_FETCHER_CONSUMER_POOL_SIZE_DEFAULT_VALUE = 2;

  public static final int PUBSUB_FAST_OPERATION_TIMEOUT_MS = Time.MS_PER_SECOND;

  public static final int PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE = 30 * Time.MS_PER_SECOND;

  public static final int PUBSUB_TOPIC_DELETE_RETRY_TIMES = 3;

  public static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;

  public static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = 5 * Time.MS_PER_DAY;

  public static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;

  public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 3;
}
