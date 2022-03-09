package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.SystemTime;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {
  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      KafkaClientFactory kafkaClientFactory,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository
  ) {
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        Lazy.of(() -> kafkaClientFactory.getRawBytesKafkaConsumer()),
        Lazy.of(() -> kafkaClientFactory.getRecordKafkaConsumer()),
        kafkaAdminWrapper,
        kafkaOperationTimeoutMs
    );
    if (optionalMetricsRepository.isPresent()) {
      return new InstrumentedPartitionOffsetFetcher(
          partitionOffsetFetcher,
          new PartitionOffsetFetcherStats(optionalMetricsRepository.get(), "PartitionOffsetFetcherStats_" + kafkaClientFactory.getKafkaBootstrapServers()),
          new SystemTime()
      );
    } else {
      return partitionOffsetFetcher;
    }
  }
}
