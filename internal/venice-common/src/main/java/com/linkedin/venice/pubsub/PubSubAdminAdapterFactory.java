package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * Generic factory interface for creating PubSub system-specific admin instances.
 * <p>
 * Concrete implementations of this interface are expected to provide the logic for creating
 * and instantiating admin components tailored to a specific PubSub system (e.g., Kafka, Pulsar).
 * <p>
 * Implementations must provide a public no-arg constructor for reflective instantiation.
 */
public abstract class PubSubAdminAdapterFactory<ADAPTER extends PubSubAdminAdapter> implements Closeable {
  /**
   * Constructor for PubSubAdminAdapterFactory used mainly for reflective instantiation.
   */
  public PubSubAdminAdapterFactory() {
    // no-op
  }

  /**
   *
   * @param veniceProperties            A copy of venice properties. Relevant producer configs will be extracted from
   *                                    veniceProperties using prefix matching. For example, to construct kafka producer
   *                                    configs that start with "kafka." prefix will be used.
   * @param pubSubTopicRepository       A repo to cache created {@link PubSubTopic}s.
   * @return                            Returns an instance of an admin adapter
   */
  public abstract ADAPTER create(VeniceProperties veniceProperties, PubSubTopicRepository pubSubTopicRepository);

  public abstract String getName();
}
