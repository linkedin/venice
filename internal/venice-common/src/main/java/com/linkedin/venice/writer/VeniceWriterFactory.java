package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterDelegator;
import com.linkedin.venice.stats.VeniceWriterStats;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory used to create {@link VeniceWriter}.
 */
public class VeniceWriterFactory {
  private static final Logger LOGGER = LogManager.getLogger(VeniceWriterFactory.class);
  private final Lazy<VeniceProperties> venicePropertiesLazy;
  private final PubSubProducerAdapterFactory producerAdapterFactory;
  private final MetricsRepository metricsRepository;
  private final String defaultBrokerAddress;

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
    this.metricsRepository = metricsRepository;
    this.venicePropertiesLazy = Lazy.of(() -> new VeniceProperties(properties));
    if (metricsRepository != null) {
      new VeniceWriterStats(metricsRepository);
    }
    // For now, if VeniceWriterFactory caller does not pass PubSubProducerAdapterFactory, use Kafka factory as default.
    // Eventually we'll force VeniceWriterFactory creators to inject PubSubProducerAdapterFactory.
    if (producerAdapterFactory == null) {
      LOGGER.info("No PubSubProducerAdapterFactory provided. Using ApacheKafkaProducerAdapterFactory as default.");
      producerAdapterFactory = new ApacheKafkaProducerAdapterFactory();
    }
    defaultBrokerAddress = venicePropertiesLazy.get().getString(ConfigKeys.PUBSUB_BROKER_ADDRESS, "");
    this.producerAdapterFactory = producerAdapterFactory;
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    VeniceProperties props = venicePropertiesLazy.get();
    String targetBrokerAddress = options.getBrokerAddress() == null ? defaultBrokerAddress : options.getBrokerAddress();
    Objects.requireNonNull(
        targetBrokerAddress,
        "Broker address is required to create a VeniceWriter. Please provide it in the options.");
    PubSubProducerAdapterContext.Builder producerContext =
        new PubSubProducerAdapterContext.Builder().setVeniceProperties(props)
            .setProducerName(options.getTopicName())
            .setBrokerAddress(options.getBrokerAddress())
            .setMetricsRepository(metricsRepository)
            .setPubSubMessageSerializer(options.getPubSubMessageSerializer())
            .setProducerCompressionEnabled(options.isProducerCompressionEnabled());

    Supplier<PubSubProducerAdapter> producerAdapterSupplier =
        () -> producerAdapterFactory.create(producerContext.build());
    int producerThreadCnt = options.getProducerThreadCount();
    if (producerThreadCnt > 1) {
      return new VeniceWriter<>(
          options,
          props,
          new PubSubProducerAdapterConcurrentDelegator(
              options.getTopicName(),
              producerThreadCnt,
              options.getProducerQueueSize(),
              producerAdapterSupplier));
    }

    int producerCnt = options.getProducerCount();
    if (producerCnt > 1) {
      List<PubSubProducerAdapter> producers = new ArrayList<>(producerCnt);
      for (int i = 0; i < producerCnt; ++i) {
        producers.add(producerAdapterSupplier.get());
      }
      return new VeniceWriter<>(options, props, new PubSubProducerAdapterDelegator(producers));
    }

    return new VeniceWriter<>(options, props, producerAdapterSupplier.get());
  }

  @VisibleForTesting
  PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }
}
