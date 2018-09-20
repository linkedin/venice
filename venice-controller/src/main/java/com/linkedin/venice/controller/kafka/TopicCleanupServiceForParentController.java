package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * In parent controller, {@link TopicCleanupServiceForParentController} will remove all the deprecated topics:
 * topic with low retention policy.
 */
public class TopicCleanupServiceForParentController extends TopicCleanupService {
  private static final Logger LOGGER = Logger.getLogger(TopicCleanupServiceForParentController.class);
  private static final Map<String, Integer> storeToCountdownForDeletion = new HashMap<>();

  public TopicCleanupServiceForParentController(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    super(admin, multiClusterConfigs);
  }

  @Override
  protected void cleanupVeniceTopics() {
    Map<String, Map<String, Long>> allStoreTopics = getAllVeniceStoreTopics();
    allStoreTopics.forEach((storeName, topics) -> {
      topics.forEach((topic, retention) -> {
        if (getAdmin().isTopicTruncatedBasedOnRetention(retention)) {
          // Topic may be deleted after delay
          int remainingFactor = storeToCountdownForDeletion.merge(topic, delayFactor, (oldVal, givenVal) -> oldVal - 1);
          if (remainingFactor > 0) {
            LOGGER.info("Retention policy for topic: " + topic + " is: " + retention + " ms, and it is deprecated, will delete it"
                + " after " + remainingFactor * TimeUnit.MILLISECONDS.toSeconds(sleepIntervalBetweenTopicListFetchMs) + " seconds.");
          } else {
            LOGGER.info("Retention policy for topic: " + topic + " is: " + retention + " ms, and it is deprecated, will delete it now.");
            storeToCountdownForDeletion.remove(topic);
            getTopicManager().ensureTopicIsDeletedAndBlock(topic);
            LOGGER.info("Topic: " + topic + " was deleted");
          }
        }
      });
    });
  }
}
