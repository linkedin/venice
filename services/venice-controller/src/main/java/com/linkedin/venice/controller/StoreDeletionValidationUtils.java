package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
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
  public static StoreDeletedValidation validateStoreDeleted(
      VeniceHelixAdmin admin,
      String clusterName,
      String storeName) {
    StoreDeletedValidation result = new StoreDeletedValidation(clusterName, storeName);

    try {
      // 1. Check if Store Config still exists in ZooKeeper
      if (checkStoreConfig(admin, result, clusterName, storeName)) {
        return result;
      }

      // 2. Check if Store metadata still exists in store repository (main store and system stores)
      if (checkAllStoreMetadata(admin, result, clusterName, storeName)) {
        return result;
      }

      // Cache the expensive listTopics() call since it's used by multiple validation methods
      TopicManager topicManager = admin.getTopicManager();
      Set<PubSubTopic> pubSubTopics;
      try {
        pubSubTopics = topicManager.listTopics();
      } catch (Exception e) {
        LOGGER.warn("Failed to list topics for store: {} in cluster: {}", storeName, clusterName, e);
        result.setStoreNotDeleted("Failed to list topics: " + e.getMessage());
        return result;
      }

      // 3. Check Helix resources for main store and system stores
      if (checkAllHelixResources(admin, result, clusterName, storeName)) {
        return result;
      }

      // 4. Check Kafka topics: version topics, RT topics, and system store topics
      if (checkForAnyExistingTopicResources(admin, result, clusterName, storeName, pubSubTopics)) {
        return result;
      }

      // 5. Check ACLs
      String storeAcls = admin.getAclForStore(clusterName, storeName);

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
      VeniceHelixAdmin admin,
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
   * Checks if store metadata still exists in the store repository for both main store and system stores.
   */
  private static boolean checkAllStoreMetadata(
      VeniceHelixAdmin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      // Check main store metadata
      final Store store = admin.getStore(clusterName, storeName);
      if (store != null) {
        result.setStoreNotDeleted("Store metadata still exists in storeRepository.");
        return true;
      }

      // Check system store metadata
      for (VeniceSystemStoreType systemStoreType: VeniceSystemStoreType.USER_SYSTEM_STORES) {
        final String systemStoreName = systemStoreType.getSystemStoreName(storeName);
        final Store systemStore = admin.getStore(clusterName, systemStoreName);
        if (systemStore != null) {
          result.setStoreNotDeleted(
              String.format("System store metadata still exists in storeRepository: %s", systemStoreName));
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check store metadata for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check store metadata: " + e.getMessage());
      return true;
    }
    return false;
  }

  /**
   * Checks if PubSub topics related to the store still exist.
   */
  private static boolean checkForAnyExistingTopicResources(
      VeniceHelixAdmin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName,
      Set<PubSubTopic> pubSubTopics) {
    try {
      for (PubSubTopic topic: pubSubTopics) {
        String topicStoreName = topic.getStoreName();

        // Check if it's the main store
        boolean isRelated = storeName.equals(topicStoreName);

        // Check if it's any system store
        if (!isRelated) {
          for (VeniceSystemStoreType systemStoreType: VeniceSystemStoreType.USER_SYSTEM_STORES) {
            String systemStoreName = systemStoreType.getSystemStoreName(storeName);
            if (systemStoreName.equals(topicStoreName)) {
              isRelated = true;
              break;
            }
          }
        }

        if (isRelated) {
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
   * Checks if Helix resources for both main store and system stores still exist.
   * This method directly queries Helix for all resources and filters for store-related ones.
   * It does not depend on existing PubSub topics.
   */
  private static boolean checkAllHelixResources(
      VeniceHelixAdmin admin,
      StoreDeletedValidation result,
      String clusterName,
      String storeName) {
    try {
      List<String> allResources = admin.getAllLiveHelixResources(clusterName);

      for (String resourceName: allResources) {
        // Only check version topics for Helix resources
        if (Version.isVersionTopic(resourceName)) {
          String resourceStoreName = Version.parseStoreFromVersionTopic(resourceName);

          // Check if it's the main store
          boolean isRelated = storeName.equals(resourceStoreName);

          // Check if it's any system store
          if (!isRelated) {
            for (VeniceSystemStoreType systemStoreType: VeniceSystemStoreType.USER_SYSTEM_STORES) {
              String systemStoreName = systemStoreType.getSystemStoreName(storeName);
              if (systemStoreName.equals(resourceStoreName)) {
                isRelated = true;
                break;
              }
            }
          }

          if (isRelated) {
            result.setStoreNotDeleted(String.format("Helix resource still exists: %s", resourceName));
            return true;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to check Helix resources for store: {} in cluster: {}", storeName, clusterName, e);
      result.setStoreNotDeleted("Failed to check Helix resources: " + e.getMessage());
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
   * @return true if the topic is related to the store, false otherwise
   */
  public static boolean isStoreRelatedTopic(PubSubTopic topic, String storeName) {
    String topicStoreName = topic.getStoreName();

    // Check if it's the main store
    if (storeName.equals(topicStoreName)) {
      return true;
    }

    // Check if it's any system store
    for (VeniceSystemStoreType systemStoreType: VeniceSystemStoreType.USER_SYSTEM_STORES) {
      String systemStoreName = systemStoreType.getSystemStoreName(storeName);
      if (systemStoreName.equals(topicStoreName)) {
        return true;
      }
    }

    return false;
  }
}
