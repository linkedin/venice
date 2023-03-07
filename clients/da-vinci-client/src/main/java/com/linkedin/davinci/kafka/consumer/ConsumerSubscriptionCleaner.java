package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Encapsulates the logic for deciding if a given topic-partition should be unsubscribed and, in that case, executing
 * the unsubscription. It is intended to operate on just on consumer instance, though it does not have direct access to
 * it. Rather, it can only get its current assignment and unsubscribe some topic-partitions from it.
 */
public class ConsumerSubscriptionCleaner {
  private static final Logger LOGGER = LogManager.getLogger(ConsumerSubscriptionCleaner.class);

  /**
   * This is used to control how much time we should wait before cleaning up the corresponding ingestion task
   * when an non-existing topic is discovered.
   * The reason to introduce this config is that `consumer#listTopics` could only guarantee eventual consistency, so
   * `consumer#listTopics` not returning the topic doesn't mean the topic doesn't exist in Kafka.
   * If `consumer#listTopics` still doesn't return the topic after the configured delay, Venice SN will unsubscribe the topic,
   * and fail the corresponding ingestion job.
   */
  private final long nonExistingTopicCleanupDelayMS;

  /**
   * This field is to maintain a mapping between the non-existing topic and the discovered time for the first time.
   * The function: {@link #getTopicPartitionsToUnsubscribe(Set)} ()} can add a new topic to this map if it discovers
   * a new non-existing topic.
   * There are two ways to clean up one entry from this map:
   * 1. The non-existing topic starts showing up.
   * 2. The non-existing period lasts longer than {@link #nonExistingTopicCleanupDelayMS}, and the corresponding task
   *    will fail.
   */
  private final Object2LongMap<String> nonExistingTopicDiscoverTimestampMap = new Object2LongOpenHashMap<>();

  private final TopicExistenceChecker topicExistenceChecker;

  private final Supplier<Set<PubSubTopicPartition>> currentAssignmentSupplier;

  private final IntConsumer recordNumberOfTopicsToUnsub;

  private final Consumer<Set<PubSubTopicPartition>> batchUnsubscribeFunction;

  private final Time time;

  /**
   * After this number of poll requests, shared consumer will check whether all the subscribed topics exist or not.
   * And it will clean up the subscription to the topics, which don't exist any more.
   */
  private final int sanitizeTopicSubscriptionAfterPollTimes;

  private int pollTimesSinceLastSanitization = 0;

  public ConsumerSubscriptionCleaner(
      long nonExistingTopicCleanupDelayMS,
      int sanitizeTopicSubscriptionAfterPollTimes,
      TopicExistenceChecker topicExistenceChecker,
      Supplier<Set<PubSubTopicPartition>> assignmentSupplier,
      IntConsumer recordNumberOfTopicsToUnsub,
      Consumer<Set<PubSubTopicPartition>> batchUnsubscribeFunction,
      Time time) {
    this.nonExistingTopicCleanupDelayMS = nonExistingTopicCleanupDelayMS;
    this.sanitizeTopicSubscriptionAfterPollTimes = sanitizeTopicSubscriptionAfterPollTimes;
    this.topicExistenceChecker = topicExistenceChecker;
    this.currentAssignmentSupplier = assignmentSupplier;
    this.recordNumberOfTopicsToUnsub = recordNumberOfTopicsToUnsub;
    this.batchUnsubscribeFunction = batchUnsubscribeFunction;
    this.time = time;
    this.nonExistingTopicDiscoverTimestampMap.defaultReturnValue(-1);
  }

  /**
   * This function is used to detect whether there is any subscription to the non-existing topics.
   * If yes, this function will remove the topic partition subscriptions to these non-existing topics.
   */
  Set<PubSubTopicPartition> getTopicPartitionsToUnsubscribe(
      Set<PubSubTopicPartition> returnSetOfTopicPartitionsToUnsub) {
    returnSetOfTopicPartitionsToUnsub.clear();
    if (++pollTimesSinceLastSanitization < sanitizeTopicSubscriptionAfterPollTimes) {
      /**
       * The reasons only to sanitize the subscription periodically:
       * 1. This is a heavy operation.
       * 2. The behavior of subscripting non-existing topics is not common.
       * 3. The subscription to the non-existing topics will only cause some inefficiency because of the logging in each
       *    poll request, but not correctness.
       */
      return returnSetOfTopicPartitionsToUnsub;
    }
    pollTimesSinceLastSanitization = 0;

    // Get the subscriptions
    Set<PubSubTopicPartition> currentAssignment = currentAssignmentSupplier.get();
    if (currentAssignment.isEmpty()) {
      return returnSetOfTopicPartitionsToUnsub;
    }
    Set<String> nonExistingTopics = new HashSet<>();
    long currentTimestamp = time.getMilliseconds();
    for (PubSubTopicPartition pubSubTopicPartition: currentAssignment) {
      String topic = pubSubTopicPartition.getPubSubTopic().getName();
      boolean isExistingTopic = topicExistenceChecker.checkTopicExists(topic);
      if (!isExistingTopic) {
        nonExistingTopics.add(topic);
      } else {
        /**
         * Check whether we should remove any topic from {@link #nonExistingTopicDiscoverTimestampMap} detected previously.
         * Since this logic will be executed before comparing the diff with the delay threshold, so it is possible that
         * even the delay is exhausted here, we will still resume the ingestion.
         */
        long previousDiscoveryTimestamp = nonExistingTopicDiscoverTimestampMap.removeLong(topic);
        if (previousDiscoveryTimestamp != nonExistingTopicDiscoverTimestampMap.defaultReturnValue()) {
          long diff = currentTimestamp - previousDiscoveryTimestamp;
          LOGGER.info(
              "The non-existing topic detected previously: {} show up after {} ms and it will be removed from nonExistingTopicDiscoverTimestampMap",
              topic,
              diff);
        }
      }
    }
    Set<String> topicsToUnsubscribe = new HashSet<>(nonExistingTopics);
    if (!nonExistingTopics.isEmpty()) {
      LOGGER.error("Detected the following non-existing topics: {}", nonExistingTopics);
      for (String topic: nonExistingTopics) {
        long firstDetectedTimestamp = nonExistingTopicDiscoverTimestampMap.getLong(topic);
        if (firstDetectedTimestamp == nonExistingTopicDiscoverTimestampMap.defaultReturnValue()) {
          // The first time to detect this non-existing topic.
          nonExistingTopicDiscoverTimestampMap.put(topic, currentTimestamp);
          firstDetectedTimestamp = currentTimestamp;
        }
        /**
         * Calculate the delay, and compare it with {@link nonExistingTopicCleanupDelayMS},
         * if the delay is over the threshold, will fail the attached ingestion task
         */
        long diff = currentTimestamp - firstDetectedTimestamp;
        if (diff >= nonExistingTopicCleanupDelayMS) {
          LOGGER.error(
              "The non-existing topic hasn't showed up after {} ms, so we will fail the attached ingestion task",
              diff);
          nonExistingTopicDiscoverTimestampMap.removeLong(topic);
        } else {
          /**
           * We shouldn't unsubscribe the non-existing topic now since currently the delay hasn't been exhausted yet.
           */
          topicsToUnsubscribe.remove(topic);
        }
      }
    }

    recordNumberOfTopicsToUnsub.accept(topicsToUnsubscribe.size());

    // Get the current subscription for this topic and unsubscribe them
    Set<PubSubTopicPartition> newAssignment = new HashSet<>();
    for (PubSubTopicPartition pubSubTopicPartition: currentAssignment) {
      if (topicsToUnsubscribe.contains(pubSubTopicPartition.getPubSubTopic().getName())) {
        returnSetOfTopicPartitionsToUnsub.add(pubSubTopicPartition);
      } else {
        newAssignment.add(pubSubTopicPartition);
      }
    }
    if (newAssignment.size() != currentAssignment.size()) {
      batchUnsubscribeFunction.accept(returnSetOfTopicPartitionsToUnsub);
    }

    return returnSetOfTopicPartitionsToUnsub;
  }

  void unsubscribe(Set<PubSubTopicPartition> toRemove) {
    if (toRemove.isEmpty()) {
      return;
    }
    Set<PubSubTopicPartition> currentAssignment = currentAssignmentSupplier.get();
    Set<PubSubTopicPartition> newAssignment = new HashSet<>(currentAssignment);
    newAssignment.removeAll(toRemove);
    if (newAssignment.size() == currentAssignment.size()) {
      // nothing changed.
      return;
    }
    batchUnsubscribeFunction.accept(toRemove);
  }
}
