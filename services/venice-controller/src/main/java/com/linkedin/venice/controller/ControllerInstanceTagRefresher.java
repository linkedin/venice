package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.SafeHelixManager;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.model.InstanceConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * PreConnectCallback is called after the participant registers its info in the cluster but before marking itself as
 * LIVEINSTANCE. If the instance is already a participant of the cluster, then we need this logic to update Instance tags
 * in the case of K8S assigning this controller participant to another cluster
 */
public class ControllerInstanceTagRefresher implements PreConnectCallback {
  private static final Logger LOGGER = LogManager.getLogger(ControllerInstanceTagRefresher.class);
  private SafeHelixManager helixManager;
  private final List<String> instanceTagList;

  public ControllerInstanceTagRefresher(SafeHelixManager helixManager, List<String> instanceTagList) {
    this.helixManager = helixManager;
    this.instanceTagList = instanceTagList;
  }

  @Override
  public void onPreConnect() {
    try {
      String instanceName = helixManager.getInstanceName();
      ConfigAccessor configAccessor = helixManager.getConfigAccessor();
      InstanceConfig instanceConfig = configAccessor.getInstanceConfig(helixManager.getClusterName(), instanceName);

      if (instanceConfig == null) {
        throw new VeniceException(
            "InstanceConfig not found for instance: " + instanceName + ". This should not happen.");
      }

      Set<String> currentTags = new HashSet<>(instanceConfig.getTags());
      Set<String> expectedTags = new HashSet<>(instanceTagList);

      // Determine tags to add and remove
      Set<String> tagsToAdd = new HashSet<>(expectedTags);
      tagsToAdd.removeAll(currentTags);

      Set<String> tagsToRemove = new HashSet<>(currentTags);
      tagsToRemove.removeAll(expectedTags);

      // Apply changes if there are any differences
      if (!tagsToAdd.isEmpty() || !tagsToRemove.isEmpty()) {
        LOGGER.info(
            "Instance '{}' tag differences detected. Adding: {}, Removing: {}",
            instanceName,
            tagsToAdd,
            tagsToRemove);

        // Add new tags
        for (String tag: tagsToAdd) {
          instanceConfig.addTag(tag);
        }

        // Remove obsolete tags
        for (String tag: tagsToRemove) {
          instanceConfig.removeTag(tag);
        }

        // Persist the updated configuration
        configAccessor.setInstanceConfig(helixManager.getClusterName(), instanceName, instanceConfig);
        LOGGER.info(
            "Replace instance tags of instance '{}' from {} to {}",
            instanceName,
            currentTags,
            instanceConfig.getTags());
      } else {
        LOGGER.info("Instance '{}' already contains all expected tags: {}", instanceName, currentTags);
      }
    } catch (Exception e) {
      throw new VeniceException("PreConnectCallback failed to apply instance tags", e);
    }
  }
}
