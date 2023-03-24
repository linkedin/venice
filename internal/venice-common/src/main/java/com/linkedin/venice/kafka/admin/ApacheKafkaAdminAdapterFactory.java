package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka admin clients.
 *
 * A kafka based admin client created using this factory is for managing and inspecting topics, brokers, configurations and ACLs.
 */

public class ApacheKafkaAdminAdapterFactory implements PubSubAdminAdapterFactory<KafkaAdminWrapper> {
  private static final String NAME = "ApacheKafkaAdmin";

  @Override
  public KafkaAdminWrapper create(VeniceProperties veniceProperties, PubSubTopicRepository pubSubTopicRepository) {
    ApacheKafkaAdminConfig adminConfig = new ApacheKafkaAdminConfig(veniceProperties);
    KafkaAdminWrapper kafkaAdminWrapper = new KafkaAdminClient();
    kafkaAdminWrapper.initialize(adminConfig.getAdminProperties(), pubSubTopicRepository);
    return kafkaAdminWrapper;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() {
  }

}
