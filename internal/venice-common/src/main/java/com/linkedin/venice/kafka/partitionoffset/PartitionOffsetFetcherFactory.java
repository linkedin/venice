package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.SystemTime;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;


public class PartitionOffsetFetcherFactory {
  public static PartitionOffsetFetcher createDefaultPartitionOffsetFetcher(
      ConsumerSupplier consumerSupplier,
      AdminSupplier adminSupplier,
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

  public interface AdminSupplier {
    PubSubAdminAdapter get(PubSubTopic pubSubTopic);
  }

  public interface ConsumerSupplier {
    PubSubConsumerAdapter get(PubSubTopic pubSubTopic);
  }
}
