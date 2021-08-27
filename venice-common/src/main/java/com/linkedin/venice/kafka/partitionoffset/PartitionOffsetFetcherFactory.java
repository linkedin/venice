package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.Lazy;
import com.linkedin.venice.utils.SystemTime;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {

  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      KafkaClientFactory kafkaClientFactory,
      Optional<MetricsRepository> optionalMetricsRepository,
      long kafkaOperationTimeoutMs
  ) {
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(kafkaClientFactory.getKafkaRawBytesConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(kafkaClientFactory.getKafkaRecordConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaAdminClient(optionalMetricsRepository)),
        kafkaOperationTimeoutMs
    );
    if (optionalMetricsRepository.isPresent()) {
      return new InstrumentedPartitionOffsetFetcher(
          partitionOffsetFetcher,
          new PartitionOffsetFetcherStats(
                  optionalMetricsRepository.get(),
                  "PartitionOffsetFetcherStats_" + TehutiUtils.fixMalformedMetricName(kafkaClientFactory.getKafkaBootstrapServers())
          ),
          new SystemTime()
      );
    } else {
      return partitionOffsetFetcher;
    }
  }

  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      KafkaClientFactory kafkaClientFactory,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs,
      Optional<MetricsRepository> optionalMetricsRepository
  ) {
    PartitionOffsetFetcher partitionOffsetFetcher = new PartitionOffsetFetcherImpl(
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(kafkaClientFactory.getKafkaRawBytesConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(kafkaClientFactory.getKafkaRecordConsumerProps())),
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
