package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;


/**
 * This is a wrapper class on top of InternalBootstrappingVeniceChangelogConsumerImpl. This confines
 * the usage of this class for clients to the methods exposed on the interface.  This is meant
 * to prevent users from doing seek() calls which would render the local state inconsistent.
 *
 * @param <K>
 * @param <V>
 */
public class LocalBootstrappingVeniceChangelogConsumer<K, V>
    extends InternalLocalBootstrappingVeniceChangelogConsumer<K, V> {
  public LocalBootstrappingVeniceChangelogConsumer(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubPositionDeserializer pubSubPositionDeserializer,
      String consumerId) {
    super(changelogClientConfig, pubSubConsumer, pubSubPositionDeserializer, consumerId);
  }
}
