package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In parent controller, {@link TopicCleanupServiceForParentController} will remove all the deprecated topics:
 * topic with low retention policy.
 */
public class TopicCleanupServiceForParentController extends TopicCleanupService {
  private static final Logger LOGGER = LogManager.getLogger(TopicCleanupServiceForParentController.class);
  private static final Map<String, Integer> storeToCountdownForDeletion = new HashMap<>();

  public TopicCleanupServiceForParentController(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    super(admin, multiClusterConfigs);
  }

  @Override
  protected void cleanupVeniceTopics() {
    Set<String> parentFabrics = multiClusterConfigs.getParentFabrics();
    if (!parentFabrics.isEmpty()) {
      for (String parentFabric: parentFabrics) {
        String kafkaBootstrapServers = multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(parentFabric);
        cleanupVeniceTopics(getTopicManager(kafkaBootstrapServers));
      }
    } else {
      cleanupVeniceTopics(getTopicManager());
    }
  }

  private void cleanupVeniceTopics(TopicManager topicManager) {
    Map<String, Map<String, Long>> allStoreTopics = getAllVeniceStoreTopicsRetentions(topicManager);
    allStoreTopics.forEach((storeName, topics) -> {
      topics.forEach((topic, retention) -> {
        if (getAdmin().isTopicTruncatedBasedOnRetention(retention)) {
          // Topic may be deleted after delay
          int remainingFactor = storeToCountdownForDeletion.merge(
              topic + "_" + topicManager.getKafkaBootstrapServers(),
              delayFactor,
              (oldVal, givenVal) -> oldVal - 1);
          if (remainingFactor > 0) {
            LOGGER.info(
                "Retention policy for topic: {} is: {} ms, and it is deprecated, will delete it after {} ms.",
                topic,
                retention,
                remainingFactor * sleepIntervalBetweenTopicListFetchMs);
          } else {
            LOGGER.info(
                "Retention policy for topic: {} is: {} ms, and it is deprecated, will delete it now.",
                topic,
                retention);
            storeToCountdownForDeletion.remove(topic + "_" + topicManager.getKafkaBootstrapServers());
            try {
              topicManager.ensureTopicIsDeletedAndBlockWithRetry(topic);
            } catch (ExecutionException e) {
              LOGGER.warn("ExecutionException caught when trying to delete topic: {}", topic);
              // No op, will try again in the next cleanup cycle.
            }
          }
        }
      });
    });
  }
}
