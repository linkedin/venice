package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.utils.IndexedMap;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link KafkaConsumerService} is used to manage a pool of consumption-related resources connected to a specific Kafka
 * cluster.
 *
 * The reasons to have this pool are:
 * 1. To reduce the unnecessary overhead of having one consumer per store-version, each of which includes the internal
 *    IO threads/connections to brokers and internal buffers;
 * 2. To reduce the GC overhead when there are a lot of store versions bootstrapping/ingesting at the same time;
 * 3. To have a predictable and configurable upper bound on the total amount of resources occupied by consumers become,
 *    no matter how many store-versions are being hosted in the same instance;
 *
 * The responsibilities of this class include:
 * 1. Setting up a fixed size pool of consumption unit, where each unit contains exactly one:
 *    a) {@link SharedKafkaConsumer}
 *    b) {@link ConsumptionTask}
 *    c) {@link ConsumerSubscriptionCleaner}
 * 2. Receive various calls to interrogate or mutate consumer state, and delegate them to the correct unit, by
 *    maintaining a mapping of which unit belongs to which version-topic and subscribed topic-partition. Notably,
 *    the {@link #startConsumptionIntoDataReceiver(PubSubTopicPartition, long, ConsumedDataReceiver)} function allows the
 *    caller to start funneling consumed data into a receiver (i.e. into another task).
 * 3. Provide a single abstract function that must be overridden by subclasses in order to implement a consumption
 *    load balancing strategy: {@link #pickConsumerForPartition(PubSubTopic, PubSubTopicPartition)}
 *
 * @see AggKafkaConsumerService which wraps one instance of this class per Kafka cluster.
 */
public abstract class KafkaConsumerService extends AbstractVeniceService {
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final ExecutorService consumerExecutor;
  protected final String kafkaUrl;
  private final Logger LOGGER;

  protected KafkaConsumerServiceStats stats;
  protected final IndexedMap<SharedKafkaConsumer, ConsumptionTask> consumerToConsumptionTask;
  protected final Map<PubSubTopic, Map<PubSubTopicPartition, SharedKafkaConsumer>> versionTopicToTopicPartitionToConsumer =
      new VeniceConcurrentHashMap<>();

  /**
   * @param statsOverride injection of stats, for test purposes
   */
  protected KafkaConsumerService(
      final PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory,
      final Properties consumerProperties,
      final long readCycleDelayMs,
      final int numOfConsumersPerKafkaCluster,
      final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler,
      final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      final String kafkaClusterAlias,
      final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker,
      final boolean liveConfigBasedKafkaThrottlingEnabled,
      final KafkaPubSubMessageDeserializer pubSubDeserializer,
      final Time time,
      final KafkaConsumerServiceStats statsOverride,
      final boolean isKafkaConsumerOffsetCollectionEnabled) {
    this.kafkaUrl = consumerProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    this.LOGGER = LogManager.getLogger(KafkaConsumerService.class.getSimpleName() + " [" + kafkaUrl + "]");

    // Initialize consumers and consumerExecutor
    consumerExecutor = Executors.newFixedThreadPool(
        numOfConsumersPerKafkaCluster,
        new DaemonThreadFactory("venice-shared-consumer-for-" + kafkaUrl));
    this.consumerToConsumptionTask = new IndexedHashMap<>(numOfConsumersPerKafkaCluster);
    this.stats = statsOverride != null
        ? statsOverride
        : createKafkaConsumerServiceStats(
            metricsRepository,
            kafkaClusterAlias,
            this::getMaxElapsedTimeSinceLastPollInConsumerPool);
    for (int i = 0; i < numOfConsumersPerKafkaCluster; ++i) {
      /**
       * We need to assign a unique client id across all the storage nodes, otherwise, they will fail into the same throttling bucket.
       */
      consumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, getUniqueClientId(kafkaUrl, i));
      SharedKafkaConsumer pubSubConsumer = new SharedKafkaConsumer(
          pubSubConsumerAdapterFactory.create(
              new VeniceProperties(consumerProperties),
              isKafkaConsumerOffsetCollectionEnabled,
              pubSubDeserializer,
              null),
          stats,
          this::recordPartitionsPerConsumerSensor,
          this::handleUnsubscription);

      Supplier<Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> pollFunction =
          liveConfigBasedKafkaThrottlingEnabled
              ? () -> kafkaClusterBasedRecordThrottler.poll(pubSubConsumer, kafkaUrl, readCycleDelayMs)
              : () -> pubSubConsumer.poll(readCycleDelayMs);
      final IntConsumer bandwidthThrottlerFunction = totalBytes -> bandwidthThrottler.maybeThrottle(totalBytes);
      final IntConsumer recordsThrottlerFunction = recordsCount -> recordsThrottler.maybeThrottle(recordsCount);
      final ConsumerSubscriptionCleaner cleaner = new ConsumerSubscriptionCleaner(
          sharedConsumerNonExistingTopicCleanupDelayMS,
          1000,
          topicExistenceChecker,
          pubSubConsumer::getAssignment,
          stats::recordDetectedDeletedTopicNum,
          pubSubConsumer::batchUnsubscribe,
          time);

      ConsumptionTask consumptionTask = new ConsumptionTask(
          this.kafkaUrl,
          i,
          readCycleDelayMs,
          pollFunction,
          bandwidthThrottlerFunction,
          recordsThrottlerFunction,
          this.stats,
          cleaner);
      consumerToConsumptionTask.putByIndex(pubSubConsumer, consumptionTask, i);
    }

    LOGGER.info("KafkaConsumerService was initialized with {} consumers.", numOfConsumersPerKafkaCluster);
  }

  /** May be overridden to clean up state in sub-classes */
  void handleUnsubscription(SharedKafkaConsumer consumer, PubSubTopicPartition topicPartition) {
  }

  private String getUniqueClientId(String kafkaUrl, int suffix) {
    return Utils.getHostName() + "_" + kafkaUrl + "_" + suffix;
  }

  public SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    Map<PubSubTopicPartition, SharedKafkaConsumer> map = versionTopicToTopicPartitionToConsumer.get(versionTopic);
    if (map == null) {
      return null;
    }
    return map.get(topicPartition);
  }

  /**
   * This function assigns a consumer for the given {@link StoreIngestionTask} and returns the assigned consumer.
   *
   * Must be idempotent and thus return previously a assigned consumer (for the same params) if any exists.
   */
  public SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    Map<PubSubTopicPartition, SharedKafkaConsumer> topicPartitionToConsumerMap =
        versionTopicToTopicPartitionToConsumer.computeIfAbsent(versionTopic, k -> new VeniceConcurrentHashMap<>());
    return topicPartitionToConsumerMap
        .computeIfAbsent(topicPartition, k -> pickConsumerForPartition(versionTopic, topicPartition));
  }

  protected abstract SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition);

  protected void removeTopicPartitionFromConsumptionTask(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition topicPartition) {
    consumerToConsumptionTask.get(consumer).removeDataReceiver(topicPartition);
  }

  /**
   * Stop all subscription associated with the given version topic.
   */
  public void unsubscribeAll(PubSubTopic versionTopic) {
    versionTopicToTopicPartitionToConsumer.compute(versionTopic, (k, topicPartitionToConsumerMap) -> {
      if (topicPartitionToConsumerMap != null) {
        topicPartitionToConsumerMap.forEach((topicPartition, sharedConsumer) -> {
          sharedConsumer.unSubscribe(topicPartition);
          removeTopicPartitionFromConsumptionTask(sharedConsumer, topicPartition);
        });
      }
      return null;
    });
  }

  /**
   * Stop specific subscription associated with the given version topic.
   */
  void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter consumer = getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
    if (consumer != null) {
      consumer.unSubscribe(pubSubTopicPartition);
      consumerToConsumptionTask.get(consumer).removeDataReceiver(pubSubTopicPartition);
      versionTopicToTopicPartitionToConsumer.compute(versionTopic, (k, topicPartitionToConsumerMap) -> {
        if (topicPartitionToConsumerMap != null) {
          topicPartitionToConsumerMap.remove(pubSubTopicPartition);
          return topicPartitionToConsumerMap.isEmpty() ? null : topicPartitionToConsumerMap;
        } else {
          return null;
        }
      });
    }
  }

  void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub) {
    Map<PubSubConsumerAdapter, Set<PubSubTopicPartition>> consumerUnSubTopicPartitionSet = new HashMap<>();
    PubSubConsumerAdapter consumer;
    for (PubSubTopicPartition topicPartition: topicPartitionsToUnSub) {
      consumer = getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
      if (consumer != null) {
        Set<PubSubTopicPartition> topicPartitionSet =
            consumerUnSubTopicPartitionSet.computeIfAbsent(consumer, k -> new HashSet<>());
        topicPartitionSet.add(topicPartition);
      }
    }
    /**
     * Leverage {@link PubSubConsumerAdapter#batchUnsubscribe(Set)}.
     */
    consumerUnSubTopicPartitionSet.forEach((c, tpSet) -> {
      c.batchUnsubscribe(tpSet);
      ConsumptionTask task = consumerToConsumptionTask.get(c);
      tpSet.forEach(tp -> {
        task.removeDataReceiver(tp);
        versionTopicToTopicPartitionToConsumer.compute(versionTopic, (k, topicPartitionToConsumerMap) -> {
          if (topicPartitionToConsumerMap != null) {
            topicPartitionToConsumerMap.remove(tp);
            return topicPartitionToConsumerMap.isEmpty() ? null : topicPartitionToConsumerMap;
          } else {
            return null;
          }
        });
      });
    });
  }

  @Override
  public boolean startInner() {
    consumerToConsumptionTask.values().forEach(consumerExecutor::submit);
    consumerExecutor.shutdown();
    LOGGER.info("KafkaConsumerService started for {}", kafkaUrl);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    consumerToConsumptionTask.values().forEach(ConsumptionTask::stop);

    int timeOutInSeconds = 1;
    long gracefulShutdownBeginningTime = System.currentTimeMillis();
    boolean gracefulShutdownSuccess = consumerExecutor.awaitTermination(timeOutInSeconds, TimeUnit.SECONDS);
    long gracefulShutdownDuration = System.currentTimeMillis() - gracefulShutdownBeginningTime;
    if (gracefulShutdownSuccess) {
      LOGGER.info("consumerExecutor terminated gracefully in {} ms.", gracefulShutdownDuration);
    } else {
      LOGGER.warn(
          "consumerExecutor timed out after {} ms while awaiting graceful termination. Will force shutdown.",
          gracefulShutdownDuration);
      long forcefulShutdownBeginningTime = System.currentTimeMillis();
      consumerExecutor.shutdownNow();
      boolean forcefulShutdownSuccess = consumerExecutor.awaitTermination(timeOutInSeconds, TimeUnit.SECONDS);
      long forcefulShutdownDuration = System.currentTimeMillis() - forcefulShutdownBeginningTime;
      if (forcefulShutdownSuccess) {
        LOGGER.info("consumerExecutor terminated forcefully in {} ms.", forcefulShutdownDuration);
      } else {
        LOGGER.warn(
            "consumerExecutor timed out after {} ms while awaiting forceful termination.",
            forcefulShutdownDuration);
      }
    }

    consumerToConsumptionTask.keySet().forEach(SharedKafkaConsumer::close);
  }

  public boolean hasAnySubscriptionFor(PubSubTopic versionTopic) {
    Map<PubSubTopicPartition, SharedKafkaConsumer> subscriptions =
        versionTopicToTopicPartitionToConsumer.get(versionTopic);
    if (subscriptions == null) {
      return false;
    }
    return !subscriptions.isEmpty();
  }

  private KafkaConsumerServiceStats createKafkaConsumerServiceStats(
      MetricsRepository metricsRepository,
      String kafkaClusterAlias,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool) {
    String nameWithKafkaClusterAlias = "kafka_consumer_service_for_" + kafkaClusterAlias;
    return new KafkaConsumerServiceStats(
        metricsRepository,
        nameWithKafkaClusterAlias,
        getMaxElapsedTimeSinceLastPollInConsumerPool);
  }

  private long getMaxElapsedTimeSinceLastPollInConsumerPool() {
    long maxElapsedTimeSinceLastPollInConsumerPool = -1;
    int slowestTaskId = -1;
    long elapsedTimeSinceLastPoll;
    for (ConsumptionTask task: consumerToConsumptionTask.values()) {
      elapsedTimeSinceLastPoll = LatencyUtils.getElapsedTimeInMs(task.getLastSuccessfulPollTimestamp());
      if (elapsedTimeSinceLastPoll > maxElapsedTimeSinceLastPollInConsumerPool) {
        maxElapsedTimeSinceLastPollInConsumerPool = elapsedTimeSinceLastPoll;
        slowestTaskId = task.getTaskId();
      }
    }
    if (maxElapsedTimeSinceLastPollInConsumerPool > Time.MS_PER_MINUTE) {
      String slowestTaskIdString = kafkaUrl + slowestTaskId;
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(slowestTaskIdString)) {
        // log the slowest consumer id if it couldn't make any progress in a minute!
        LOGGER.warn(
            "Shared consumer ({} - task {}) couldn't make any progress for over {} ms!",
            kafkaUrl,
            slowestTaskId,
            maxElapsedTimeSinceLastPollInConsumerPool);
      }
    }
    return maxElapsedTimeSinceLastPollInConsumerPool;
  }

  public void startConsumptionIntoDataReceiver(
      PubSubTopicPartition topicPartition,
      long lastReadOffset,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver) {
    PubSubTopic versionTopic = consumedDataReceiver.destinationIdentifier();
    SharedKafkaConsumer consumer = assignConsumerFor(versionTopic, topicPartition);

    if (consumer == null) {
      // Defensive code. Shouldn't happen except in case of a regression.
      throw new VeniceException(
          "Shared consumer must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaUrl);
    }

    ConsumptionTask consumptionTask = consumerToConsumptionTask.get(consumer);
    if (consumptionTask == null) {
      // Defensive coding. Should never happen except in case of a regression.
      throw new IllegalStateException(
          "There should be a " + ConsumptionTask.class.getSimpleName() + " assigned for this "
              + SharedKafkaConsumer.class.getSimpleName());
    }
    /**
     * N.B. it's important to set the {@link ConsumedDataReceiver} prior to subscribing, otherwise the
     * {@link KafkaConsumerService.ConsumptionTask} will not be able to funnel the messages.
     */
    consumptionTask.setDataReceiver(topicPartition, consumedDataReceiver);
    consumer.subscribe(consumedDataReceiver.destinationIdentifier(), topicPartition, lastReadOffset);
  }

  interface KCSConstructor {
    KafkaConsumerService construct(
        PubSubConsumerAdapterFactory consumerFactory,
        Properties consumerProperties,
        long readCycleDelayMs,
        int numOfConsumersPerKafkaCluster,
        EventThrottler bandwidthThrottler,
        EventThrottler recordsThrottler,
        KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
        MetricsRepository metricsRepository,
        String kafkaClusterAlias,
        long sharedConsumerNonExistingTopicCleanupDelayMS,
        TopicExistenceChecker topicExistenceChecker,
        boolean liveConfigBasedKafkaThrottlingEnabled,
        KafkaPubSubMessageDeserializer pubSubDeserializer,
        Time time,
        KafkaConsumerServiceStats stats,
        boolean isKafkaConsumerOffsetCollectionEnabled);
  }

  final void recordPartitionsPerConsumerSensor() {
    int totalPartitions = 0;
    int minPartitionsPerConsumer = Integer.MAX_VALUE;
    int maxPartitionsPerConsumer = Integer.MIN_VALUE;

    int subscribedPartitionCount;
    for (SharedKafkaConsumer consumer: consumerToConsumptionTask.keySet()) {
      subscribedPartitionCount = consumer.getAssignmentSize();
      totalPartitions += subscribedPartitionCount;
      minPartitionsPerConsumer = Math.min(minPartitionsPerConsumer, subscribedPartitionCount);
      maxPartitionsPerConsumer = Math.max(maxPartitionsPerConsumer, subscribedPartitionCount);
    }
    int avgPartitionsPerConsumer = totalPartitions / consumerToConsumptionTask.size();

    stats.recordAvgPartitionsPerConsumer(avgPartitionsPerConsumer);
    stats.recordMaxPartitionsPerConsumer(maxPartitionsPerConsumer);
    stats.recordMinPartitionsPerConsumer(minPartitionsPerConsumer);
  }

  public long getOffsetLagFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    return getSomeOffsetFor(
        versionTopic,
        pubSubTopicPartition,
        PubSubConsumerAdapter::getOffsetLag,
        stats::recordOffsetLagIsAbsent,
        stats::recordOffsetLagIsPresent);
  }

  public long getLatestOffsetFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    return getSomeOffsetFor(
        versionTopic,
        pubSubTopicPartition,
        PubSubConsumerAdapter::getLatestOffset,
        stats::recordLatestOffsetIsAbsent,
        stats::recordLatestOffsetIsPresent);
  }

  private long getSomeOffsetFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      OffsetGetter offsetGetter,
      Runnable sensorIfAbsent,
      Runnable sensorIfPresent) {
    PubSubConsumerAdapter consumer = getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
    if (consumer == null) {
      sensorIfAbsent.run();
      return -1;
    } else {
      long result = offsetGetter.apply(consumer, pubSubTopicPartition);
      if (result < 0) {
        sensorIfAbsent.run();
      } else {
        sensorIfPresent.run();
      }
      return result;
    }
  }

  private interface OffsetGetter {
    long apply(PubSubConsumerAdapter consumer, PubSubTopicPartition pubSubTopicPartition);
  }

  /**
   * This consumer assignment strategy specify how consumers from consumer pool are allocated. Now we support two basic
   * strategies with topic-wise and partition-wise for supporting consumer shared in topic and topic-partition granularity,
   * respectively. Each strategy will have a specific extension of {@link KafkaConsumerService}.
   */
  public enum ConsumerAssignmentStrategy {
    TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY(TopicWiseKafkaConsumerService::new),
    PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY(PartitionWiseKafkaConsumerService::new);

    final KCSConstructor constructor;

    ConsumerAssignmentStrategy(KCSConstructor constructor) {
      this.constructor = constructor;
    }
  }
}
