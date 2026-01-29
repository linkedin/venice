package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_MAX_SPLITS_PER_PARTITION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_SPLIT_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_TIME_WINDOW_IN_MINUTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.splitter.PubSubTopicPartitionSplitStrategy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Utility class for planning {@link com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit}s
 * for both MapReduce and Spark ingestion jobs.
 * <p>
 * This class encapsulates the common logic for:
 * <ul>
 *   <li>Building a {@link com.linkedin.venice.pubsub.manager.TopicManager} from job configuration.</li>
 *   <li>Reading split parameters (split type, max records per split, max splits per partition, time window, etc.).</li>
 *   <li>Determining the number of partitions for a topic and generating splits using the appropriate
 *       {@link PubSubTopicPartitionSplitStrategy}.</li>
 * </ul>
 */
public class PubSubSplitPlanner {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  public List<PubSubPartitionSplit> plan(VeniceProperties jobConfig) {
    String brokerUrl = jobConfig.getString(KAFKA_INPUT_BROKER_URL);

    try (TopicManager tm = createTopicManager(jobConfig, brokerUrl)) {
      PartitionSplitStrategy partitionSplitStrategy = PartitionSplitStrategy
          .valueOf(jobConfig.getString(PUBSUB_INPUT_SPLIT_STRATEGY, DEFAULT_PUBSUB_INPUT_SPLIT_STRATEGY));
      long recordsPerSplit =
          jobConfig.getLong(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, DEFAULT_PUBSUB_INPUT_MAX_RECORDS_PER_MAPPER);
      int maxSplits = jobConfig.getInt(PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION, DEFAULT_MAX_SPLITS_PER_PARTITION);
      long timeWindowMinutes =
          jobConfig.getLong(PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES, DEFAULT_PUBSUB_INPUT_TIME_WINDOW_IN_MINUTES);
      long timeWindowMs = Duration.ofMinutes(timeWindowMinutes).toMillis();

      PubSubTopic topic = TOPIC_REPOSITORY.getTopic(jobConfig.getString(KAFKA_INPUT_TOPIC));
      return doPlan(tm, topic, partitionSplitStrategy, recordsPerSplit, maxSplits, timeWindowMs);
    }
  }

  @VisibleForTesting
  TopicManager createTopicManager(VeniceProperties consumerProps, String brokerUrl) {
    PubSubClientsFactory pubSubClientsFactory = new PubSubClientsFactory(consumerProps);
    TopicManagerContext ctx = new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> consumerProps)
        .setPubSubTopicRepository(TOPIC_REPOSITORY)
        .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProps))
        .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
        .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory())
        .setTopicMetadataFetcherThreadPoolSize(1)
        .setTopicMetadataFetcherConsumerPoolSize(1)
        .setVeniceComponent(VeniceComponent.PUSH_JOB)
        .build();
    return new TopicManagerRepository(ctx, brokerUrl).getLocalTopicManager();
  }

  private List<PubSubPartitionSplit> doPlan(
      TopicManager tm,
      PubSubTopic topic,
      PartitionSplitStrategy partitionSplitStrategy,
      long recordsPerSplit,
      int maxSplitsPerPartition,
      long timeWindowMs) {
    int partitionCount = RetryUtils.executeWithMaxAttempt(
        () -> tm.getPartitionCount(topic),
        10,
        Duration.ofMinutes(1),
        Collections.singletonList(Exception.class));

    List<PubSubPartitionSplit> out = new ArrayList<>();
    SplitRequest.Builder reqBuilder = new SplitRequest.Builder().topicManager(tm)
        .splitType(partitionSplitStrategy)
        .maxSplits(maxSplitsPerPartition)
        .recordsPerSplit(recordsPerSplit)
        .timeWindowInMs(timeWindowMs);
    for (int p = 0; p < partitionCount; p++) {
      PubSubTopicPartition tp = new PubSubTopicPartitionImpl(topic, p);
      out.addAll(partitionSplitStrategy.split(reqBuilder.pubSubTopicPartition(tp).build()));
    }
    return out;
  }
}
