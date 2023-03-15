package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
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
      KafkaClientFactory kafkaClientFactory,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository) {
    KafkaPubSubMessageDeserializer kafkaPubSubMessageDeserializer = new KafkaPubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        kafkaAdminWrapper,
        Lazy.of(() -> kafkaClientFactory.getConsumer(new Properties(), kafkaPubSubMessageDeserializer)),
        kafkaOperationTimeoutMs,
        kafkaClientFactory.getKafkaBootstrapServers());
    if (optionalMetricsRepository.isPresent()) {
      return new InstrumentedPartitionOffsetFetcher(
          partitionOffsetFetcher,
          new PartitionOffsetFetcherStats(
              optionalMetricsRepository.get(),
              "PartitionOffsetFetcherStats_" + kafkaClientFactory.getKafkaBootstrapServers()),
          new SystemTime());
    } else {
      return partitionOffsetFetcher;
    }
  }
}
