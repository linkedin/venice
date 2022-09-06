package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Extend RoutingTableChangeListener to leverage customized view data for hybrid store quota.
 */
public class HelixHybridStoreQuotaRepository implements RoutingTableChangeListener {
  private static final Logger logger = LogManager.getLogger(HelixHybridStoreQuotaRepository.class);
  private final SafeHelixManager manager;
  private final Map<PropertyType, List<String>> dataSource;

  private RoutingTableProvider routingTableProvider;

  // lock object protects resourceToStatusMap.
  private final Lock lock = new ReentrantLock();
  private Map<String, HybridStoreQuotaStatus> resourceToStatusMap; // Topic to quota state

  public HelixHybridStoreQuotaRepository(SafeHelixManager manager) {
    this.manager = manager;
    resourceToStatusMap = new HashMap<>();
    dataSource = new HashMap<>();
    dataSource
        .put(PropertyType.CUSTOMIZEDVIEW, Collections.singletonList(HelixPartitionState.HYBRID_STORE_QUOTA.name()));
  }

  /**
   * Get list of quota exceeded stores from local memory cache.
   *
   */
  public List<String> getHybridQuotaViolatedStores() {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      List<String> hybridQuotaViolatedStores = resourceToStatusMap.keySet()
          .stream()
          .filter(
              originalResources -> resourceToStatusMap.get(originalResources)
                  .equals(HybridStoreQuotaStatus.QUOTA_VIOLATED))
          .collect(Collectors.toList());
      return hybridQuotaViolatedStores;
    }
  }

  /**
   * Get hybrid store quota status from local memory cache for specific resource name.
   * @param resourceName
   *
   * @return
   */
  public HybridStoreQuotaStatus getHybridStoreQuotaStatus(@Nonnull String resourceName) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
      if (resourceToStatusMap.containsKey(resourceName)) {
        return resourceToStatusMap.get(resourceName);
      }
      String errorMessage = "Resource '" + resourceName + "' does not exist";
      logger.warn(errorMessage);
      return HybridStoreQuotaStatus.UNKNOWN;
    }
  }

  public void refresh() {
    try {
      logger.info("Refresh started for cluster " + manager.getClusterName() + "'s" + getClass().getSimpleName());
      routingTableProvider = new RoutingTableProvider(manager.getOriginalManager(), dataSource);
      routingTableProvider.addRoutingTableChangeListener(this, null);
      // We only support HYBRID_STORE_QUOTA in this class.
      onRoutingTableChange(
          routingTableProvider
              .getRoutingTableSnapshot(PropertyType.CUSTOMIZEDVIEW, HelixPartitionState.HYBRID_STORE_QUOTA.name()),
          null);
      logger.info("Refresh finished for cluster" + manager.getClusterName() + "'s" + getClass().getSimpleName());
    } catch (Exception e) {
      String errorMessage = "Cannot refresh routing table from Helix for cluster " + manager.getClusterName();
      logger.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }

  public void clear() {
    if (routingTableProvider != null) {
      routingTableProvider.removeRoutingTableChangeListener(this);
    }
  }

  private void onHybridStoreQuotaViewChange(RoutingTableSnapshot routingTableSnapshot) {
    Collection<CustomizedView> customizedViewCollection = routingTableSnapshot.getCustomizeViews();
    if (customizedViewCollection == null) {
      logger.warn("There is no existing customized view");
      return;
    }
    /**
     * onDataChange logic for hybrid quota status
     */
    if (routingTableSnapshot.getCustomizedStateType().equals(HelixPartitionState.HYBRID_STORE_QUOTA.name())) {
      Set<String> resourcesInCustomizedView =
          customizedViewCollection.stream().map(CustomizedView::getResourceName).collect(Collectors.toSet());
      Map<String, HybridStoreQuotaStatus> newResourceToStatusMap = new HashMap<>();
      for (CustomizedView customizedView: customizedViewCollection) {
        String resourceName = customizedView.getResourceName();
        HybridStoreQuotaStatus status = HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED;
        for (String partitionName: customizedView.getPartitionSet()) {
          Map<String, String> instanceStateMap = customizedView.getStateMap(partitionName);
          // Iterate through all instances' execution status
          for (Map.Entry<String, String> entry: instanceStateMap.entrySet()) {
            String instanceState = entry.getValue();
            try {
              status = HybridStoreQuotaStatus.valueOf(instanceState);
            } catch (Exception e) {
              String instanceName = entry.getKey();
              logger.warn("Instance:" + instanceName + " unrecognized status:" + instanceState);
              continue;
            }
            if (status.equals(HybridStoreQuotaStatus.QUOTA_VIOLATED)) {
              break;
            }
          }
          if (status.equals(HybridStoreQuotaStatus.QUOTA_VIOLATED)) {
            break;
          }
        }
        newResourceToStatusMap.put(resourceName, status);
      }
      Set<String> deletedResourceNames;
      try (AutoCloseableLock ignore = AutoCloseableLock.of(lock)) {
        deletedResourceNames = resourceToStatusMap.keySet()
            .stream()
            .filter(originalResources -> !newResourceToStatusMap.containsKey(originalResources))
            .collect(Collectors.toSet());
        this.resourceToStatusMap = newResourceToStatusMap;
      }
      logger.info("Updated resource execution status map.");
      logger.info(
          "Hybrid store quota view is changed. The number of active resources is " + resourcesInCustomizedView.size()
              + ", and the number of deleted resource is " + deletedResourceNames.size());
    }
  }

  @Override
  public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
    if (routingTableSnapshot == null) {
      logger.warn("Routing table snapshot should not be null");
      return;
    }
    PropertyType helixPropertyType = routingTableSnapshot.getPropertyType();
    switch (helixPropertyType) {
      case CUSTOMIZEDVIEW:
        logger.debug("Received Helix routing table change on Customized View");
        onHybridStoreQuotaViewChange(routingTableSnapshot);
        break;
      default:
        logger.warn("Received Helix routing table change on invalid type " + helixPropertyType);
    }
  }
}
