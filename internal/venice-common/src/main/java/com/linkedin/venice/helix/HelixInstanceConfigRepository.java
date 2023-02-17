package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This repository is used to store the instance config per instance.
 * So far it only stores group/zone info, and we could add more if necessary in the future.
 */
public class HelixInstanceConfigRepository implements VeniceResource, InstanceConfigChangeListener {
  private static final Logger LOGGER = LogManager.getLogger(HelixInstanceConfigRepository.class);

  public static final int DEFAULT_INSTANCE_GROUP_ID = 0;
  public static final String GROUP_FIELD_NAME_IN_DOMAIN = "group";
  public static final String ZONE_FIELD_NAME_IN_DOMAIN = "zone";

  private final SafeHelixManager manager;
  private final PropertyKey.Builder keyBuilder;
  private final String virtualGroupFieldName;

  private Map<String, Integer> instanceGroupIdMapping = Collections.emptyMap();
  private int groupCount = 1;
  /**
   * This field is used to record the unknown instance call in {@link #getInstanceGroupId}.
   * Since this function will be invoked very frequently, we don't want to log every unknown instance to bloat the log file
   * in case this issue is happening very frequently.
   * The current implementation is to do 1% logging.
   */
  private final AtomicLong unknownInstanceCall = new AtomicLong();

  public HelixInstanceConfigRepository(SafeHelixManager manager, boolean useGroupFieldInDomain) {
    this.manager = manager;
    this.keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    this.virtualGroupFieldName = useGroupFieldInDomain ? GROUP_FIELD_NAME_IN_DOMAIN : ZONE_FIELD_NAME_IN_DOMAIN;
    LOGGER.info("Will use '{}' as the virtual group field in Helix domain.", this.virtualGroupFieldName);
  }

  @Override
  public void refresh() {
    try {
      /**
       * Here only sets up the callback listener, but didn't initialize the instance configs since Helix lib will guarantee
       * an initialized callback will be triggered even there is no instance config changed.
       */
      manager.addInstanceConfigChangeListener(this);
      LOGGER.info("Setup InstanceConfigChangeListener in {}.", this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new VeniceException("Failed to refresh " + this.getClass().getSimpleName(), e);
    }
  }

  @Override
  public void clear() {
    manager.removeListener(keyBuilder.instanceConfigs(), this);
    LOGGER.info("Removed InstanceConfigChangeListener in {}.", this.getClass().getSimpleName());
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

  /**
   * This function will assign a unique group id per group.
   * If there is no group defined for some instances, this function will use empty string as the default.
   */
  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    if (instanceConfigs.isEmpty()) {
      LOGGER.warn("Received empty instance configs, so will skip it");
      return;
    }
    LOGGER.info("Received instance configs: {}.", instanceConfigs);
    Map<String, Integer> newInstanceGroupIdMapping = new VeniceConcurrentHashMap<>();
    Map<String, Integer> groupIdMapping = new HashMap<>();
    final AtomicInteger groupIdCnt = new AtomicInteger(0);
    for (InstanceConfig instanceConfig: instanceConfigs) {
      // Extract group config
      if (instanceConfig.getInstanceEnabled()) {
        Map<String, String> domainConfigMap = instanceConfig.getDomainAsMap();
        String groupConfig = domainConfigMap.get(virtualGroupFieldName);
        if (groupConfig == null) {
          // Set the default group to be empty if not present
          groupConfig = "";
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
}
