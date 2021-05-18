package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


@Threadsafe
public interface PartitionOffsetFetcher extends Closeable {

  Map<Integer, Long> getLatestOffsets(String topic);

  long getLatestOffset(String topic, int partition);

  long getLatestOffsetAndRetry(String topic, int partition, int retries);

  Map<Integer, Long> getOffsetsByTime(String topic, Map<TopicPartition, Long> timestampsToSearch, long timestamp);

  Map<Integer, Long> getOffsetsByTime(String topic, long timestamp);

  long getOffsetByTime(String topic, int partition, long timestamp);

  long getLatestProducerTimestampAndRetry(String topic, int partition, int retries);

  List<PartitionInfo> partitionsFor(String topic);

  long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp);

  @Override
  void close();
}
