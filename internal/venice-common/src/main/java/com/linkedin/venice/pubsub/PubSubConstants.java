package com.linkedin.venice.pubsub;

/**
 * Constants used by pub-sub components.
 */
public class PubSubConstants {

  // If true, the producer will use default configuration values for optimized high throughput producing if they are not
  // explicitly set.
  public static final String PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS =
      "pubsub.producer.use.high.throughput.defaults";
}
