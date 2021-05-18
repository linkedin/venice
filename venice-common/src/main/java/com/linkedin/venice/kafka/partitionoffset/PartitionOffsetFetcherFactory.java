package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.utils.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {

  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      KafkaClientFactory kafkaClientFactory,
      Optional<MetricsRepository> optionalMetricsRepository,
      long kafkaOperationTimeoutMs
  ) {
    return new PartitionOffsetFetcherImpl(
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(KafkaClientFactory.getKafkaRawBytesConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(KafkaClientFactory.getKafkaRecordConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaAdminClient(optionalMetricsRepository)),
        kafkaOperationTimeoutMs
    );
  }

  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      KafkaClientFactory kafkaClientFactory,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs
  ) {
    return new PartitionOffsetFetcherImpl(
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(KafkaClientFactory.getKafkaRawBytesConsumerProps())),
        Lazy.of(() -> kafkaClientFactory.getKafkaConsumer(KafkaClientFactory.getKafkaRecordConsumerProps())),
        kafkaAdminWrapper,
        kafkaOperationTimeoutMs
    );
  }
}
