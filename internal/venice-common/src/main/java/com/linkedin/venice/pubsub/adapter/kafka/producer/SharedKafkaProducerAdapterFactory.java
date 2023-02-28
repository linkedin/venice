package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CLIENT_ID;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerConfig.SHARED_KAFKA_PRODUCER_CONFIG_PREFIX;
import static com.linkedin.venice.writer.VeniceWriter.CLOSE_TIMEOUT_MS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_CLOSE_TIMEOUT_MS;

import com.linkedin.venice.pubsub.adapter.PubSubSharedProducerAdapter;
import com.linkedin.venice.pubsub.adapter.PubSubSharedProducerFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of {@link PubSubSharedProducerFactory} for creating Apache Kafka producers in shared mode.
 */
public class SharedKafkaProducerAdapterFactory extends PubSubSharedProducerFactory {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaProducerAdapterFactory.class);
  private static final String NAME = "ApacheKafkaSharedProducer";
  private final ApacheKafkaProducerAdapterFactory internalProducerAdapterFactory;
  private final MetricsRepository metricsRepository;
  private final Set<String> producerMetricsToBeReported;

  /**
   * @param properties -- List of properties to construct a kafka producer
   * @param sharedProducerPoolCount  -- producer pool sizes
   * @param internalProducerAdapterFactory -- factory to create a KafkaProducerAdapter instances
   * @param metricsRepository -- metric repository
   * @param producerMetricsToBeReported -- a comma separated list of KafkaProducer metrics that will export as ingraph metrics
   */
  public SharedKafkaProducerAdapterFactory(
      Properties properties,
      int sharedProducerPoolCount,
      ApacheKafkaProducerAdapterFactory internalProducerAdapterFactory,
      MetricsRepository metricsRepository,
      Set<String> producerMetricsToBeReported) {
    super(sharedProducerPoolCount, properties, metricsRepository);
    this.internalProducerAdapterFactory = internalProducerAdapterFactory;
    this.metricsRepository = metricsRepository;
    this.producerMetricsToBeReported = producerMetricsToBeReported;

    // extract and configs for kafka shared producers
    VeniceProperties veniceWriterProperties = new VeniceProperties(properties);
    producerCloseTimeout = veniceWriterProperties.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);
    // replace all properties starting with SHARED_KAFKA_PRODUCER_CONFIG_PREFIX with KAFKA_CONFIG_PREFIX.
    Properties sharedProducerProperties =
        veniceWriterProperties.clipAndFilterNamespace(SHARED_KAFKA_PRODUCER_CONFIG_PREFIX).toProperties();
    for (Map.Entry<Object, Object> entry: sharedProducerProperties.entrySet()) {
      this.producerProperties.put(KAFKA_CONFIG_PREFIX + entry.getKey(), entry.getValue());
    }
    LOGGER.info("Shared kafka producer factory has been initialized");
  }

  @Override
  public PubSubSharedProducerAdapter createSharedProducer(int id) {
    String producerName = "shared-producer-" + id;
    LOGGER.info("Creating a shared kafka producer: {}", producerName);
    producerProperties.put(KAFKA_CLIENT_ID, producerName);
    ApacheKafkaProducerAdapter producerAdapter =
        internalProducerAdapterFactory.create(new VeniceProperties(producerProperties), producerName, null);
    return new PubSubSharedProducerAdapter(this, producerAdapter, metricsRepository, producerMetricsToBeReported, id);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
