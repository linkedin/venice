package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * In parent controller, {@link TopicCleanupServiceForParentController} will remove all the deprecated topics:
 * topic with low retention policy.
 */
public class TopicCleanupServiceForParentController extends TopicCleanupService {
  private static final Logger LOGGER = Logger.getLogger(TopicCleanupServiceForParentController.class);

  public TopicCleanupServiceForParentController(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    super(admin, multiClusterConfigs);
  }

  @Override
  protected void cleanupVeniceTopics() {
    Map<String, Map<String, Long>> allStoreTopics = getAllVeniceStoreTopics();
    allStoreTopics.forEach((storeName, topics) -> {
      topics.forEach((topic, retention) -> {
        if (getAdmin().isTopicTruncatedBasedOnRetention(retention)) {
          LOGGER.info("Retention policy for topic: " + topic + " is: " + retention + " ms, and it is deprecated, will delete it");
          getTopicManager().ensureTopicIsDeletedAndBlock(topic);
          LOGGER.info("Topic: " + topic + " was deleted");
        }
      });
    });
  }
}
