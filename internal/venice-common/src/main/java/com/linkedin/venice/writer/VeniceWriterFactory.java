package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.stats.VeniceWriterStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory used to create {@link VeniceWriter}.
 */
public class VeniceWriterFactory {
  private static final Logger LOGGER = LogManager.getLogger(VeniceWriterFactory.class);
  private final Properties properties;
  private final PubSubProducerAdapterFactory producerAdapterFactory;

  public VeniceWriterFactory(Properties properties) {
    this(properties, PubSubClientsFactory.createProducerFactory(properties), null);
  }

  public VeniceWriterFactory(Properties properties, MetricsRepository metricsRepository) {
    this(properties, PubSubClientsFactory.createProducerFactory(properties), metricsRepository);
  }

  public VeniceWriterFactory(
      Properties properties,
      PubSubProducerAdapterFactory producerAdapterFactory,
      MetricsRepository metricsRepository) {
    this.properties = properties;
    if (metricsRepository != null) {
      new VeniceWriterStats(metricsRepository);
    }
    // For now, if VeniceWriterFactory caller does not pass PubSubProducerAdapterFactory, use Kafka factory as default.
    // Eventually we'll force VeniceWriterFactory creators to inject PubSubProducerAdapterFactory.
    if (producerAdapterFactory == null) {
      LOGGER.info("No PubSubProducerAdapterFactory provided. Using ApacheKafkaProducerAdapterFactory as default.");
      producerAdapterFactory = new ApacheKafkaProducerAdapterFactory();
    }
    this.producerAdapterFactory = producerAdapterFactory;
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    VeniceProperties props = new VeniceProperties(this.properties);
    return new VeniceWriter<>(
        options,
        props,
        producerAdapterFactory.create(props, options.getTopicName(), options.getBrokerAddress()));
  }

  // visible for testing
  PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }
}
