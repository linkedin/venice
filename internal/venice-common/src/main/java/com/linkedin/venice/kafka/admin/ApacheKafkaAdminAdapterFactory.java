package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka producers.
 *
 * A producer created using this factory is usually used to send data to a single pub-sub topic.
 */

public class ApacheKafkaAdminAdapterFactory implements PubSubAdminAdapterFactory<KafkaAdminWrapper> {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerConfig.class);

  private static final String NAME = "ApacheKafkaAdmin";

  @Override
  public KafkaAdminWrapper create(
      VeniceProperties veniceProperties,
      Optional<MetricsRepository> optionalMetricsRepository,
      String statsNamePrefix,
      PubSubTopicRepository pubSubTopicRepository,
      String brokerAddressToOverride) {
    ApacheKafkaAdminConfig adminConfig = new ApacheKafkaAdminConfig(veniceProperties, brokerAddressToOverride);
    KafkaAdminWrapper kafkaAdminWrapper = new KafkaAdminClient();
    kafkaAdminWrapper.initialize(adminConfig.getAdminProperties(), pubSubTopicRepository);
    if (optionalMetricsRepository.isPresent()) {
      // Use Kafka bootstrap server to identify which Kafka admin client stats it is
      final String kafkaAdminStatsName =
          String.format("%s_%s_%s", statsNamePrefix, KafkaAdminClient.class, brokerAddressToOverride);
      kafkaAdminWrapper =
          new InstrumentedKafkaAdmin(kafkaAdminWrapper, optionalMetricsRepository.get(), kafkaAdminStatsName);
      LOGGER.info(
          "Created instrumented Kafka admin client for Kafka cluster with bootstrap "
              + "server {} and has stats name prefix {}" + adminConfig.getAdminProperties(),
          brokerAddressToOverride,
          statsNamePrefix);
    } else {
      LOGGER.info(
          "Created non-instrumented Kafka admin client for Kafka cluster with bootstrap server {}",
          brokerAddressToOverride);
    }
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
