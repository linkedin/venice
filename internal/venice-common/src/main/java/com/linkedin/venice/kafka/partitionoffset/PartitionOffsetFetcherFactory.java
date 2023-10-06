package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {
  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory,
      VeniceProperties veniceProperties,
      String pubSubBootstrapServers,
      PubSubAdminAdapter pubSubAdminAdapter,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository) {
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    PubSubConsumerAdapter pubSubConsumerAdapter =
        pubSubConsumerAdapterFactory.create(veniceProperties, false, pubSubMessageDeserializer, pubSubBootstrapServers);
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        pubSubAdminAdapter,
        pubSubConsumerAdapter,
        kafkaOperationTimeoutMs,
        pubSubBootstrapServers);
    if (optionalMetricsRepository.isPresent()) {
      return new InstrumentedPartitionOffsetFetcher(
          partitionOffsetFetcher,
          new PartitionOffsetFetcherStats(
              optionalMetricsRepository.get(),
              "PartitionOffsetFetcherStats_" + pubSubBootstrapServers),
          new SystemTime());
    } else {
      return partitionOffsetFetcher;
    }
  }
}
