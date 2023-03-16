package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.factory.PubSubClientFactory;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;


public class PartitionOffsetFetcherFactory {
  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      PubSubClientFactory pubSubClientFactory,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository) {
    KafkaPubSubMessageDeserializer kafkaPubSubMessageDeserializer = new KafkaPubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        kafkaAdminWrapper,
        Lazy.of(() -> pubSubClientFactory.getConsumer(new Properties(), kafkaPubSubMessageDeserializer)),
        kafkaOperationTimeoutMs,
        pubSubClientFactory.getPubSubBootstrapServers());
    if (optionalMetricsRepository.isPresent()) {
      return new InstrumentedPartitionOffsetFetcher(
          partitionOffsetFetcher,
          new PartitionOffsetFetcherStats(
              optionalMetricsRepository.get(),
              "PartitionOffsetFetcherStats_" + pubSubClientFactory.getPubSubBootstrapServers()),
          new SystemTime());
    } else {
      return partitionOffsetFetcher;
    }
  }
}
