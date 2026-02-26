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
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
 *
 * <p>Split planning is optimized in two ways:
 * <ol>
 *   <li><b>Batch position fetching</b>: Start and end positions for all partitions are fetched in
 *       two bulk calls instead of 2N individual calls, drastically reducing pubsub queries.</li>
 *   <li><b>Parallel split computation</b>: After batch-fetching positions, per-partition split
 *       computation (which is pure CPU work) is parallelized across available processors.</li>
 * </ol>
 */
public class PubSubSplitPlanner {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSplitPlanner.class);
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

    if (partitionCount <= 0) {
      return Collections.emptyList();
    }

    // Batch-fetch start and end positions for ALL partitions in 2 bulk calls
    // instead of 2 * partitionCount individual calls.
    Map<PubSubTopicPartition, PubSubPosition> startPositions = tm.getStartPositionsForTopicWithRetries(topic);
    Map<PubSubTopicPartition, PubSubPosition> endPositions = tm.getEndPositionsForTopicWithRetries(topic);

    // Index by partition number for reliable lookup regardless of PubSubTopicPartition.equals() impl.
    Map<Integer, PubSubPosition> startByPartition = new HashMap<>(startPositions.size());
    startPositions.forEach((tp, pos) -> startByPartition.put(tp.getPartitionNumber(), pos));
    Map<Integer, PubSubPosition> endByPartition = new HashMap<>(endPositions.size());
    endPositions.forEach((tp, pos) -> endByPartition.put(tp.getPartitionNumber(), pos));

    LOGGER.info(
        "Batch-fetched positions for {} partitions of topic: {}. Planning splits using {} strategy with {} threads.",
        partitionCount,
        topic,
        partitionSplitStrategy,
        Math.min(partitionCount, Runtime.getRuntime().availableProcessors()));

    // After batch-fetching, per-partition split computation is pure CPU work
    // (offset arithmetic in advancePosition). Parallelize across available processors.
    int threads = Math.min(partitionCount, Runtime.getRuntime().availableProcessors());
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<List<PubSubPartitionSplit>>> futures = new ArrayList<>(partitionCount);
      for (int p = 0; p < partitionCount; p++) {
        PubSubTopicPartition tp = new PubSubTopicPartitionImpl(topic, p);
        PubSubPosition start = startByPartition.get(p);
        PubSubPosition end = endByPartition.get(p);

        if (start == null || end == null) {
          throw new VeniceException(
              "Missing position data (start=" + start + ", end=" + end + ") for partition " + p + " of topic " + topic);
        }

        long records = RetryUtils.executeWithMaxAttempt(
            () -> tm.diffPosition(tp, end, start),
            3,
            Duration.ofSeconds(5),
            Collections.singletonList(Exception.class));

        SplitRequest req = new SplitRequest.Builder().topicManager(tm)
            .splitType(partitionSplitStrategy)
            .maxSplits(maxSplitsPerPartition)
            .recordsPerSplit(recordsPerSplit)
            .timeWindowInMs(timeWindowMs)
            .pubSubTopicPartition(tp)
            .startPosition(start)
            .endPosition(end)
            .numberOfRecords(records)
            .build();

        futures.add(executor.submit(() -> partitionSplitStrategy.split(req)));
      }

      List<PubSubPartitionSplit> out = new ArrayList<>();
      for (Future<List<PubSubPartitionSplit>> f: futures) {
        out.addAll(f.get());
      }
      return out;
    } catch (ExecutionException e) {
      throw new VeniceException("Failed to plan splits for topic: " + topic, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Split planning interrupted for topic: " + topic, e);
    } finally {
      executor.shutdownNow();
    }
  }
}
