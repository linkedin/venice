package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.*;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@State(Scope.Benchmark)
public class SharedKafkaConsumerBenchmark {
  enum ConsumerType {
    TOPIC_WISE, PARTITION_WISE
  }

  protected static final long NON_EXISTING_TOPIC_CLEANUP_DELAY_MS = 1000;

  @Param({ "TOPIC_WISE", "PARTITION_WISE" })
  String consumerTypeString;

  @Param({ "1", "10", "100" })
  int numberOfTopicPartitionsPerBatch;

  @Param({ "100", "1000", "10000", "100000" })
  int totalNumberOfRecords;

  SharedKafkaConsumer consumer;
  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> input;

  @Setup
  public void setUp() {
    KafkaConsumerWrapper consumerDelegate = mock(KafkaConsumerWrapper.class);
    KafkaConsumerService consumerService = mock(KafkaConsumerService.class);
    KafkaConsumerServiceStats consumerServiceStats = mock(KafkaConsumerServiceStats.class);
    doReturn(consumerServiceStats).when(consumerService).getStats();

    ConsumerType consumerType = ConsumerType.valueOf(consumerTypeString);
    switch (consumerType) {
      case TOPIC_WISE:
        this.consumer = new TopicWiseSharedKafkaConsumer(
            consumerDelegate,
            consumerService,
            NON_EXISTING_TOPIC_CLEANUP_DELAY_MS,
            null);
        break;
      case PARTITION_WISE:
        this.consumer = new PartitionWiseSharedKafkaConsumer(
            consumerDelegate,
            consumerService,
            NON_EXISTING_TOPIC_CLEANUP_DELAY_MS,
            null);
        break;
    }

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> records =
        new HashMap<>(numberOfTopicPartitionsPerBatch);
    int numberOfRecordsPerTopicPartition = totalNumberOfRecords / numberOfTopicPartitionsPerBatch;
    for (int tp = 0; tp < numberOfTopicPartitionsPerBatch; tp++) {
      TopicPartition topicPartition = new TopicPartition("topic", tp);
      List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordList =
          new ArrayList<>(numberOfRecordsPerTopicPartition);
      for (int offset = 0; offset < numberOfRecordsPerTopicPartition; offset++) {
        recordList.add(
            new ConsumerRecord<>(
                topicPartition.topic(),
                topicPartition.partition(),
                offset,
                new KafkaKey(MessageType.PUT, new byte[0]),
                new KafkaMessageEnvelope()));
      }
      records.put(topicPartition, recordList);
    }
    this.input = new ConsumerRecords<>(records);

    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    for (TopicPartition topicPartition: records.keySet()) {
      this.consumer.subscribe(topicPartition.topic(), topicPartition.partition(), 0);
      switch (consumerType) {
        case TOPIC_WISE:
          consumer.attach(topicPartition.topic(), storeIngestionTask);
          break;
        case PARTITION_WISE:
          ((PartitionWiseSharedKafkaConsumer) consumer)
              .addIngestionTaskForTopicPartition(topicPartition, storeIngestionTask);
          break;
      }
    }
  }

  @TearDown
  public void cleanUp() {
    this.consumer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 5, time = 5)
  @Measurement(iterations = 5, time = 10)
  @Fork(10)
  public void sanitizeTopicsWithoutCorrespondingIngestionTask(Blackhole blackhole) {
    consumer.sanitizeTopicsWithoutCorrespondingIngestionTask(input);
    blackhole.consume(input);
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(SharedKafkaConsumerBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .build();
    new Runner(opt).run();
  }
}
