package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * Generic admin factory interface.
 *
 * A pus-sub specific concrete implementation of this interface should be provided to be able to create
 * and instantiate admins for that system.
 */
public interface PubSubAdminAdapterFactory<ADAPTER extends PubSubAdminAdapter> extends Closeable {
  /**
   *
   * @param veniceProperties            A copy of venice properties. Relevant producer configs will be extracted from
   *                                    veniceProperties using prefix matching. For example, to construct kafka producer
   *                                    configs that start with "kafka." prefix will be used.
   * @param pubSubTopicRepository       A repo to cache created {@link PubSubTopic}s.
   * @return                            Returns an instance of an admin adapter
   */
  ADAPTER create(VeniceProperties veniceProperties, PubSubTopicRepository pubSubTopicRepository);

  String getName();
}
