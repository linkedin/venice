package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.utils.IndexedMap;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A service that monitors Kafka topic partitions for inactivity and automatically pauses/resumes them
 * to prevent blocking of active topic partitions.
 *
 * <p>This checker runs periodically to:
 * <ul>
 *   <li>Identify topic partitions that haven't been successfully polled within a configured threshold</li>
 *   <li>Pause inactive topic partitions to prevent them from blocking consumption of active topic partitions</li>
 *   <li>Resume previously paused topic partitions that have become active again</li>
 * </ul>
 *
 * <p>The service maintains state about which topic partitions are currently paused for each consumer
 * and makes intelligent decisions about when to pause/resume based on the current activity status.
 *
 * <p>This is particularly useful in scenarios where some topic partitions may become temporarily
 * unavailable or slow, preventing the consumer from making progress on other healthy topic partitions.
 *
 * <p>The checker uses per-topic-partition statistics from {@link ConsumptionTask} to determine activity,
 * specifically looking at the last successful poll timestamp for each topic partition.
 */
public class InactiveTopicPartitionChecker extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(InactiveTopicPartitionChecker.class);

  /** Map tracking which topic partitions are currently paused for each consumer */
  private final Map<SharedKafkaConsumer, Set<PubSubTopicPartition>> consumerToPausedTopicPartitionsMap =
      new VeniceConcurrentHashMap<>();

  /** Map of consumers to their associated consumption tasks */
  private final IndexedMap<SharedKafkaConsumer, ConsumptionTask> consumerToConsumptionTask;

  /** Interval in milliseconds between inactive topic partition checks */
  private final long inactiveTopicPartitionCheckIntervalInMs;

  /** Threshold in milliseconds after which a topic partition is considered inactive */
  private final long inactiveTopicPartitionThresholdInMs;

  /** Executor service for scheduling periodic inactive topic partition checks */
  private final ScheduledExecutorService executorService;

  /**
   * Creates a new InactiveTopicPartitionChecker.
   *
   * @param consumerToConsumptionTask map of consumers to their consumption tasks,
   *                                  used to check topic partition activity status
   * @param inactiveTopicPartitionCheckIntervalInSeconds how often to check for inactive topic partitions (in milliseconds)
   * @param inactiveTopicPartitionThresholdInSeconds threshold after which a topic partition is considered inactive (in milliseconds)
   */
  public InactiveTopicPartitionChecker(
      IndexedMap<SharedKafkaConsumer, ConsumptionTask> consumerToConsumptionTask,
      long inactiveTopicPartitionCheckIntervalInSeconds,
      long inactiveTopicPartitionThresholdInSeconds) {
    this.consumerToConsumptionTask = consumerToConsumptionTask;
    this.inactiveTopicPartitionCheckIntervalInMs =
        TimeUnit.SECONDS.toMillis(inactiveTopicPartitionCheckIntervalInSeconds);
    this.inactiveTopicPartitionThresholdInMs = TimeUnit.SECONDS.toMillis(inactiveTopicPartitionThresholdInSeconds);
    this.executorService =
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("InactiveTopicPartitionChecker"));
  }

  /**
   * Starts the inactive topic partition checker service.
   *
   * <p>This method schedules a periodic task that will run every
   * {@code inactiveTopicPartitionCheckIntervalInMs} milliseconds to check for
   * and handle inactive topic partitions.
   *
   * @return true if the service started successfully
   * @throws Exception if there's an error starting the service
   */
  @Override
  public boolean startInner() throws Exception {
    // Start the scheduled task to check inactive topic partitions
    executorService.scheduleWithFixedDelay(
        this::checkInactiveTopicPartition,
        getInactiveTopicPartitionCheckIntervalInMs(),
        getInactiveTopicPartitionCheckIntervalInMs(),
        TimeUnit.MILLISECONDS);
    return true;
  }

  /**
   * Stops the inactive topic partition checker service.
   *
   * <p>This method gracefully shuts down the scheduled executor service,
   * waiting up to 5 seconds for running tasks to complete before forcing shutdown.
   *
   * @throws Exception if there's an error stopping the service
   */
  @Override
  public void stopInner() throws Exception {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Checks for inactive topic partitions and handles them appropriately.
   *
   * <p>This method is called periodically by the scheduled executor service and performs
   * the following operations for each consumer:
   *
   * <ol>
   *   <li><b>Detection:</b> Identifies currently inactive topic partitions by comparing
   *       the per-topic-partition last successful poll timestamp against the configured threshold.
   *       Skips topic partitions that are already paused to avoid redundant checks.</li>
   *   <li><b>Resume:</b> Resumes previously paused topic partitions that are no longer inactive</li>
   *   <li><b>Pause:</b> Pauses newly detected inactive topic partitions and adds them to the tracking set</li>
   * </ol>
   *
   * <p>A topic partition is considered inactive if:
   * <ul>
   *   <li>The time since its last successful poll exceeds {@code inactiveTopicPartitionThresholdInMs}</li>
   *   <li>It has never been successfully polled (lastSuccessfulPollTimestamp == -1)</li>
   * </ul>
   *
   * <p>The method includes comprehensive error handling to ensure that exceptions
   * don't break the periodic execution schedule.
   *
   * <p><b>Note:</b> This implementation uses per-topic-partition statistics from {@link ConsumptionTask#getPartitionStats(PubSubTopicPartition)}
   * to get accurate activity information for each topic partition individually, rather than using a global
   * last poll timestamp for the entire consumer.
   */
  void checkInactiveTopicPartition() {
    try {
      long currentTime = System.currentTimeMillis();
      LOGGER.info("Start checking inactive topic partitions.");
      // Process each consumer: check for inactive topic-partitions, then pause/resume as needed
      for (Map.Entry<SharedKafkaConsumer, ConsumptionTask> entry: getConsumerToConsumptionTask().entrySet()) {
        Map<String, Long> inactiveTopicPartitionToPreviousPollTimestampMap = new VeniceConcurrentHashMap<>();
        SharedKafkaConsumer consumer = entry.getKey();
        ConsumptionTask task = entry.getValue();
        Set<PubSubTopicPartition> previouslyPausedTopicPartitions =
            getConsumerToPausedTopicPartitionsMap().computeIfAbsent(consumer, x -> new HashSet<>());
        Set<PubSubTopicPartition> assignedTopicPartitions = consumer.getAssignment();
        Set<PubSubTopicPartition> currentlyInactiveTopicPartitions = new HashSet<>();
        // Step 1: Check all assigned topic-partitions for inactivity and collect currently inactive ones
        for (PubSubTopicPartition topicPartition: assignedTopicPartitions) {
          if (previouslyPausedTopicPartitions.contains(topicPartition)) {
            continue;
          }
          long lastSuccessfulPollTimestamp = task.getPartitionStats(topicPartition).getLastSuccessfulPollTimestamp();
          long timeSinceLastPoll = currentTime - lastSuccessfulPollTimestamp;
          if (lastSuccessfulPollTimestamp == -1 || timeSinceLastPoll > getInactiveTopicPartitionThresholdInMs()) {
            currentlyInactiveTopicPartitions.add(topicPartition);
            inactiveTopicPartitionToPreviousPollTimestampMap.put(topicPartition.toString(), timeSinceLastPoll);
          }
        }
        if (!inactiveTopicPartitionToPreviousPollTimestampMap.isEmpty()) {
          LOGGER.info(
              "Detected inactive topic partitions: {} for consumer task: {} exceed threshold: {}ms",
              inactiveTopicPartitionToPreviousPollTimestampMap,
              task.getTaskIdStr(),
              getInactiveTopicPartitionThresholdInMs());

        }

        // Step 2: Resume topic-partitions that are no longer inactive
        for (PubSubTopicPartition topicPartition: previouslyPausedTopicPartitions) {
          if (!currentlyInactiveTopicPartitions.contains(topicPartition)) {
            consumer.resume(topicPartition);
          }
        }
        if (!previouslyPausedTopicPartitions.isEmpty()) {
          LOGGER.info(
              "Resumed previously paused topic partitions: {} for consumer: {}",
              previouslyPausedTopicPartitions,
              task.getTaskIdStr());
        }
        previouslyPausedTopicPartitions.clear();

        // Step 3: Pause newly inactive topic-partitions
        for (PubSubTopicPartition topicPartition: currentlyInactiveTopicPartitions) {
          if (!previouslyPausedTopicPartitions.contains(topicPartition)) {
            consumer.pause(topicPartition);
          }
          previouslyPausedTopicPartitions.add(topicPartition);
        }
        if (!previouslyPausedTopicPartitions.isEmpty()) {
          LOGGER.info(
              "Paused inactive topic partitions: {} for consumer: {}",
              previouslyPausedTopicPartitions,
              task.getTaskIdStr());
        }
      }
      LOGGER.info("Completed checking inactive topic partitions.");
    } catch (Exception e) {
      // Log the exception to avoid breaking the scheduled execution
      LOGGER.warn("Error during inactive topic partition check: {}", e.getMessage(), e);
    }
  }

  long getInactiveTopicPartitionCheckIntervalInMs() {
    return inactiveTopicPartitionCheckIntervalInMs;
  }

  long getInactiveTopicPartitionThresholdInMs() {
    return inactiveTopicPartitionThresholdInMs;
  }

  IndexedMap<SharedKafkaConsumer, ConsumptionTask> getConsumerToConsumptionTask() {
    return consumerToConsumptionTask;
  }

  Map<SharedKafkaConsumer, Set<PubSubTopicPartition>> getConsumerToPausedTopicPartitionsMap() {
    return consumerToPausedTopicPartitionsMap;
  }
}
