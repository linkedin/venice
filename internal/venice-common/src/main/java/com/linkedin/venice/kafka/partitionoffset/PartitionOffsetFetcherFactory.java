package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.SystemTime;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {
  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      TopicManager.ConsumerSupplier consumerSupplier,
      TopicManager.AdminSupplier adminSupplier,
      String pubSubBootstrapServers,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository) {
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        consumerSupplier,
        adminSupplier,
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
