package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterDelegator;
import com.linkedin.venice.stats.VeniceWriterStats;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
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
  private final Lazy<VeniceProperties> venicePropertiesWithCompressionDisabledLazy;
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
    this.venicePropertiesLazy = Lazy.of(() -> new VeniceProperties(properties));
    this.venicePropertiesWithCompressionDisabledLazy = Lazy.of(() -> {
      // Make a deep copy and update it
      Properties propertiesWithCompressionDisabled = new Properties();
      properties.forEach((k, v) -> propertiesWithCompressionDisabled.put(k, v));
      propertiesWithCompressionDisabled.put(ApacheKafkaProducerConfig.KAFKA_COMPRESSION_TYPE, "none");
      return new VeniceProperties(propertiesWithCompressionDisabled);
    });
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
    VeniceProperties props = options.isProducerCompressionEnabled()
        ? venicePropertiesLazy.get()
        : venicePropertiesWithCompressionDisabledLazy.get();

    Supplier<PubSubProducerAdapter> producerAdapterSupplier =
        () -> producerAdapterFactory.create(props, options.getTopicName(), options.getBrokerAddress());

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

  // visible for testing
  PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }
}
