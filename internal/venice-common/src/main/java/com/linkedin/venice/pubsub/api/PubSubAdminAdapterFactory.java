package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Optional;


/**
 * Generic admin factory interface.
 *
 * A pus-sub specific concrete implementation of this interface should be provided to be able to create
 * and instantiate admins for that system.
 * TODO: change KafkaAdminWrapper into PubSubAdminAdaptor
 */
public interface PubSubAdminAdapterFactory<ADAPTER extends KafkaAdminWrapper> extends Closeable {
  /**
   *
   * @param veniceProperties            A copy of venice properties. Relevant producer configs will be extracted from
   *                                    veniceProperties using prefix matching. For example, to construct kafka producer
   *                                    configs that start with "kafka." prefix will be used.
   * @param optionalMetricsRepository   Specify using instrumented admin client or not. If it is null, use
   *                                    non-instrumented admin client.
   * @param statsNamePrefix             Name prefix for admin related metrics when instrumented admin client is enabled.
   * @param pubSubTopicRepository       A repo to cache created {@link PubSubTopic}s.
   * @param targetBrokerAddress         Broker address to use when creating an admin.
   *                                    If this value is null, local broker address present in veniceProperties will be used.
   * @return                            Returns an instance of an admin adapter
   */
  ADAPTER create(
      VeniceProperties veniceProperties,
      Optional<MetricsRepository> optionalMetricsRepository,
      String statsNamePrefix,
      PubSubTopicRepository pubSubTopicRepository,
      String targetBrokerAddress);

  String getName();
}
