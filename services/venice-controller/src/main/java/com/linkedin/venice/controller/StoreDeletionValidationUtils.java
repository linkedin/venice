package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class for Venice store deletion validation operations.
 * Contains methods to validate store deletion and other store-related validation logic.
 * 
 * This class provides a clean separation of store validation logic from the main
 * VeniceHelixAdmin class, making it easier to test and maintain.
 */
public class StoreDeletionValidationUtils {
  private static final Logger LOGGER = LogManager.getLogger(StoreDeletionValidationUtils.class);

  private StoreDeletionValidationUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Validates that a store has been completely deleted from the Venice cluster.
   * This method performs comprehensive checks across multiple subsystems to ensure
   * no lingering resources remain that would prevent safe store recreation.
   *
   * Resources checked:
   * 1. Store configuration in ZooKeeper
   * 2. Store metadata in store repository
   * 3. System stores (META_STORE and DAVINCI_PUSH_STATUS_STORE)
   * 4. Helix resources
   * 5. Kafka topics (version, RT, and system store topics)
   *
   * @param admin the Venice admin instance for performing validation operations
   * @param clusterName the name of the cluster to check
   * @param storeName the name of the store to validate deletion for
   * @return StoreDeletedValidation indicating whether the store is fully deleted or what resources remain
   */
  public static StoreDeletedValidation validateStoreDeleted(Admin admin, String clusterName, String storeName) {
    final StoreDeletedValidation result = new StoreDeletedValidation(clusterName, storeName);

    try {
      // 1. Check if Store Config still exists in ZooKeeper
      if (checkStoreConfig(admin, result, clusterName, storeName)) {
        return result;
      }

      // 2. Check if Store metadata still exists in store repository
      if (checkStoreMetadata(admin, result, clusterName, storeName)) {
        return result;
      }

      // 3. Check system store metadata
      if (checkSystemStoreMetadata(admin, result, clusterName, storeName)) {
        return result;
      }

      // 4. Check Helix resources for main store
      if (checkMainStoreHelixResources(admin, result, clusterName, storeName)) {
        return result;
      }

      // 5. Check Kafka topics: version topics, RT topics, and system store topics
      if (checkForAnyExistingTopicResources(admin, result, clusterName, storeName)) {
        return result;
      }

      // 6. Check Helix resources for system stores
      if (checkSystemStoreHelixResources(admin, result, clusterName, storeName)) {
        return result;
      }

      // If we reach here, all validations passed
      return result;

    } catch (Exception e) {
      // Handle any unexpected exceptions gracefully
      LOGGER.error("Error during store deletion validation for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted(String.format("Error during store deletion validation: %s", e.getMessage()));
      return result;
    }
  }

  /**
   * Checks if store configuration still exists in ZooKeeper.
   */
  private static boolean checkStoreConfig(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      final ZkStoreConfigAccessor storeConfigAccessor =
          admin.getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
      final StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
      if (storeConfig != null) {
        result.setStoreNotDeleted("Store config still exists in storeConfigRepo.");
        return true;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check store config for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check store config: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if store metadata still exists in the store repository.
   */
  private static boolean checkStoreMetadata(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      final Store store = admin.getStore(clusterName, storeName);
      if (store != null) {
        result.setStoreNotDeleted("Store metadata still exists in storeRepository.");
        return true;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check store metadata for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check store metadata: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if system store metadata still exists.
   */
  private static boolean checkSystemStoreMetadata(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      final List<VeniceSystemStoreType> systemStoreTypesToCheck = new ArrayList<>();
      for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
        if (type != VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE) { // Not per-store
          systemStoreTypesToCheck.add(type);
        }
      }

      for (VeniceSystemStoreType systemStoreType: systemStoreTypesToCheck) {
        final String systemStoreName = systemStoreType.getSystemStoreName(storeName);
        final Store systemStore = admin.getStore(clusterName, systemStoreName);
        if (systemStore != null) {
          result.setStoreNotDeleted(
              String.format("System store metadata still exists in storeRepository: %s", systemStoreName));
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check system store metadata for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check system store metadata: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if PubSub topics related to the store still exist.
   */
  private static boolean checkForAnyExistingTopicResources(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      final TopicManager topicManager = admin.getTopicManager();
      final Set<PubSubTopic> pubSubTopics = topicManager.listTopics();

      final List<VeniceSystemStoreType> systemStoreTypesToCheck = new ArrayList<>();
      for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
        if (type != VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE) {
          systemStoreTypesToCheck.add(type);
        }
      }

      for (PubSubTopic topic: pubSubTopics) {
        // Check if this topic is related to the store (includes main store and system store topics)
        if (isStoreRelatedTopic(topic, storeName, systemStoreTypesToCheck)) {
          result.setStoreNotDeleted(String.format("PubSub topic still exists: %s", topic.getName()));
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check PubSub topics for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check PubSub topics: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if Helix resources for the main store still exist.
   */
  private static boolean checkMainStoreHelixResources(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      // Since we can't use isResourceStillAlive directly with store name,
      // and containsHelixResource isn't available in Admin interface,
      // we'll check if any PubSub topics exist for this store (which would indicate Helix resources)
      final TopicManager topicManager = admin.getTopicManager();
      final Set<PubSubTopic> pubSubTopics = topicManager.listTopics();

      for (PubSubTopic topic: pubSubTopics) {
        // Check for version topics that belong to this exact store
        if (storeName.equals(topic.getStoreName()) && topic.isVersionTopic()) {
          // If a topic exists, try to check if its Helix resource exists
          try {
            if (admin.isResourceStillAlive(topic.getName())) {
              result.setStoreNotDeleted(
                  String.format("Helix resource still exists for version topic: %s", topic.getName()));
              return true;
            }
          } catch (Exception e) {
            // If isResourceStillAlive throws an exception, the resource likely doesn't exist
            LOGGER.debug(
                "Helix resource check failed for topic: {} (likely doesn't exist): {}",
                topic.getName(),
                e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check main store Helix resources for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check main store Helix resources: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if Helix resources for system stores still exist.
   */
  private static boolean checkSystemStoreHelixResources(
      Admin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      final List<VeniceSystemStoreType> systemStoreTypesToCheck = new ArrayList<>();
      for (VeniceSystemStoreType type: VeniceSystemStoreType.values()) {
        if (type != VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE) {
          systemStoreTypesToCheck.add(type);
        }
      }

      final TopicManager topicManager = admin.getTopicManager();
      final Set<PubSubTopic> pubSubTopics = topicManager.listTopics();

      for (VeniceSystemStoreType systemStoreType: systemStoreTypesToCheck) {
        final String systemStoreName = systemStoreType.getSystemStoreName(storeName);

        for (PubSubTopic topic: pubSubTopics) {
          // Check for system store version topics that belong to this store
          if (systemStoreName.equals(topic.getStoreName()) && topic.isVersionTopic()) {
            // If a topic exists, try to check if its Helix resource exists
            try {
              if (admin.isResourceStillAlive(topic.getName())) {
                result.setStoreNotDeleted(
                    String.format("Helix resource still exists for system store topic: %s", topic.getName()));
                return true;
              }
            } catch (Exception e) {
              // If isResourceStillAlive throws an exception, the resource likely doesn't exist
              LOGGER.debug(
                  "Helix resource check failed for topic: {} (likely doesn't exist): {}",
                  topic.getName(),
                  e.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER
          .warn("Failed to check system store Helix resources for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check system store Helix resources: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Determines if a PubSub topic is related to the specified store.
   * This includes version topics, real-time topics, view topics, and system store topics.
   * Uses PubSubTopic's built-in store name validation for better accuracy.
   * 
   * @param topic the PubSub topic to check
   * @param storeName the name of the store 
   * @param systemStoreTypes the list of system store types to check against
   * @return true if the topic is related to the store, false otherwise
   */
  public static boolean isStoreRelatedTopic(
      PubSubTopic topic,
      String storeName,
      List<VeniceSystemStoreType> systemStoreTypes) {

    // Use PubSubTopic's built-in store name validation for better accuracy
    if (storeName.equals(topic.getStoreName())) {
      return true;
    }

    // Check for system store topics
    for (VeniceSystemStoreType systemStoreType: systemStoreTypes) {
      final String systemStoreName = systemStoreType.getSystemStoreName(storeName);
      if (systemStoreName.equals(topic.getStoreName())) {
        return true;
      }
    }

    return false;
  }
}
