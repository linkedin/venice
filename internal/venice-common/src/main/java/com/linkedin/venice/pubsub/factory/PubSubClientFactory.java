package com.linkedin.venice.pubsub.factory;

import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;


public interface PubSubClientFactory {
  PubSubConsumer getConsumer(Properties props, PubSubMessageDeserializer kafkaPubSubMessageDeserializer);

  KafkaAdminWrapper getWriteOnlyPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository);

  KafkaAdminWrapper getReadOnlyPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository);

  KafkaAdminWrapper getPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository);

  Properties setupSSL(Properties properties);

  String getPubSubBootstrapServers();

  Optional<MetricsParameters> getMetricsParameters();

  PubSubClientFactory clone(String bootstrapServers, Optional<MetricsParameters> metricsParameters);
}
