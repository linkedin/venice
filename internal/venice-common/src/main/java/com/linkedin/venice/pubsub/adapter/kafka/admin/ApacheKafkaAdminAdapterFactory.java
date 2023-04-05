package com.linkedin.venice.pubsub.adapter.kafka.admin;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka admin clients.
 *
 * A kafka based admin client created using this factory is for managing and inspecting topics, brokers, configurations and ACLs.
 */

public class ApacheKafkaAdminAdapterFactory implements PubSubAdminAdapterFactory<PubSubAdminAdapter> {
  private static final String NAME = "ApacheKafkaAdmin";

  @Override
  public PubSubAdminAdapter create(VeniceProperties veniceProperties, PubSubTopicRepository pubSubTopicRepository) {
    ApacheKafkaAdminConfig adminConfig = new ApacheKafkaAdminConfig(veniceProperties);
    PubSubAdminAdapter pubSubAdminAdapter =
        new ApacheKafkaAdminAdapter(adminConfig.getAdminProperties(), pubSubTopicRepository);
    return pubSubAdminAdapter;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
