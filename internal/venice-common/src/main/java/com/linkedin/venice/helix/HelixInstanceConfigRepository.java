package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This repository is used to store the instance config per instance.
 * So far it only stores the fault zone type info, and we could add more if necessary in the future.
 */
public class HelixInstanceConfigRepository
    implements VeniceResource, InstanceConfigChangeListener, ClusterConfigChangeListener {
  private static final Logger LOGGER = LogManager.getLogger(HelixInstanceConfigRepository.class);

  public static final int DEFAULT_INSTANCE_GROUP_ID = 0;

  private final SafeHelixManager manager;
  private final PropertyKey.Builder keyBuilder;
  private String faultZoneType;

  private Map<String, Integer> instanceGroupIdMapping = Collections.emptyMap();
  private int groupCount = 1;
  /**
   * This field is used to record the unknown instance call in {@link #getInstanceGroupId}.
   * Since this function will be invoked very frequently, we don't want to log every unknown instance to bloat the log file
   * in case this issue is happening very frequently.
   * The current implementation is to do 1% logging.
   */
  private final AtomicLong unknownInstanceCall = new AtomicLong();

  private List<InstanceConfig> instanceConfigs;

  public HelixInstanceConfigRepository(SafeHelixManager manager) {
    this.manager = manager;
    this.keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    ClusterConfig clusterConfig = manager.getConfigAccessor().getClusterConfig(manager.getClusterName());
    this.faultZoneType = clusterConfig.getFaultZoneType();
  }

  @Override
  public void refresh() {
    try {
      /**
       * Here only sets up the callback listener, but didn't initialize the instance configs since Helix lib will guarantee
       * an initialized callback will be triggered even there is no instance config changed.
       */
      manager.addInstanceConfigChangeListener(this);
      manager.addClusterfigChangeListener(this);
      LOGGER.info(
          "Setup InstanceConfigChangeListener and ClusterConfigChangeListener in {}.",
          this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new VeniceException("Failed to refresh " + this.getClass().getSimpleName(), e);
    }
  }

  @Override
  public void clear() {
    manager.removeListener(keyBuilder.clusterConfig(), this);
    manager.removeListener(keyBuilder.instanceConfigs(), this);
    LOGGER.info(
        "Removed InstanceConfigChangeListener and ClusterConfigChangeListener in {}.",
        this.getClass().getSimpleName());
  }

  public int getInstanceGroupId(String instanceId) {
    Integer groupId = instanceGroupIdMapping.get(instanceId);
    if (groupId == null) {
      /**
       * Defensive code.
       * This could happen if there is a race condition between ExternalView and InstanceConfig, for example, an Instance
       * is showed up in ExternalView, but Router doesn't know this instance because of missing ZK notification or
       * other bugs.
       * If this issue is happening a lot, we need to consider to do force refresh periodically.
       */
      if (unknownInstanceCall.incrementAndGet() % 100 == 1) {
        LOGGER.warn(
            "Couldn't find group id for instance: {}, and we need to look into this issue to understand how it"
                + " happens and come up with the right mitigation/solution since it will affect the scatter-gathering perf.",
            instanceId);
      }
      return DEFAULT_INSTANCE_GROUP_ID;
    }
    return groupId;
  }

  public Map<String, Integer> getInstanceGroupIdMapping() {
    return instanceGroupIdMapping;
  }

  public int getGroupCount() {
    return groupCount;
  }

  private void updateInstanceGroupIdMappings() {
    if (instanceConfigs.isEmpty()) {
      LOGGER.warn("Received empty instance configs, so will skip it");
      return;
    }
    // Sort it to make the group id generation deterministic.
    Collections.sort(instanceConfigs, (instanceConfig1, instanceConfig2) -> {
      // Sort by instance id to ensure the order is consistent for the same set of instance configs.
      // This will help to generate deterministic group ids.
      String id1 = instanceConfig1.getId();
      String id2 = instanceConfig2.getId();
      return id1.compareTo(id2);
    });
    LOGGER.info("Received instance configs: {} and FAULT_ZONE_TYPE: {}", instanceConfigs, faultZoneType);
    Map<String, Integer> newInstanceGroupIdMapping = new VeniceConcurrentHashMap<>();
    Map<String, Integer> groupIdMapping = new HashMap<>();
    final AtomicInteger groupIdCnt = new AtomicInteger(0);
    for (InstanceConfig instanceConfig: instanceConfigs) {
      // Extract group config
      if (instanceConfig.getInstanceEnabled()) {
        Map<String, String> domainConfigMap = instanceConfig.getDomainAsMap();
        String groupConfig;
        if (faultZoneType == null || !domainConfigMap.containsKey(faultZoneType)) {
          // Set the default group to be empty if not present
          groupConfig = "";
        } else {
          groupConfig = domainConfigMap.get(faultZoneType);
        }
        int currentGroupId = groupIdMapping.computeIfAbsent(groupConfig, gc -> groupIdCnt.getAndIncrement());
        newInstanceGroupIdMapping.put(instanceConfig.getId(), currentGroupId);
      }
    }
    instanceGroupIdMapping = Collections.unmodifiableMap(newInstanceGroupIdMapping);
    groupCount = groupIdCnt.get();
    LOGGER.info("New instance group id mapping: {}.", instanceGroupIdMapping);
    LOGGER.info("The total number of groups: {}.", groupCount);
  }

  /**
   * This function will assign a unique group id per group.
   * If there is no group defined for some instances, this function will use empty string as the default.
   */
  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    synchronized (this) {
      this.instanceConfigs = instanceConfigs;
      updateInstanceGroupIdMappings();
    }
  }

  @Override
  public void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext context) {
    String updatedFaultZoneType = clusterConfig.getFaultZoneType();
    if (Objects.equals(faultZoneType, updatedFaultZoneType)) {
      return;
    }
    synchronized (this) {
      LOGGER.info("Updated FAULT_ZONE_TYPE to: {}", updatedFaultZoneType);
      this.faultZoneType = updatedFaultZoneType;
      updateInstanceGroupIdMappings();
    }
  }
}
